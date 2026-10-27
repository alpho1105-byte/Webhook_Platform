// Package queue 是投遞工作的佇列，語意對應 Amazon SQS：取出的訊息只是暫時看不見（visibility timeout），
// consumer 處理完明確 Ack 才會刪除；consumer 當掉或逾時沒有 Ack，訊息會重新出現。
// 另外支援延遲出現（用於重試退避）與一段時間內的重複訊息去重（對應 SQS FIFO 的 deduplication id）。
//
// 佇列依 endpoint_id 分區：同一個端點的投遞都在同一個分區，由同一個 worker 處理，
// 對每個端點的併發上限與 circuit breaker 都只需要本機狀態。
package queue

import (
	"container/heap"
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"webhook-platform/internal/domain"
)

type Config struct {
	Partitions int
	// Visibility 是取出後多久沒有 Ack 就重新出現。要大於一次投遞的最長時間（HTTP 逾時加上處理時間）。
	Visibility time.Duration
	// DedupeWindow 內同一個 delivery id 只會被加入一次。
	DedupeWindow time.Duration
}

type Message struct {
	Partition int
	Receipt   uint64
	Task      domain.Delivery
	// Receives 是這則訊息被取出的次數，大於 1 代表前一個 consumer 沒有 Ack（例如當掉）。
	Receives int
}

type Stats struct {
	Enqueued    int64
	Deduped     int64
	Redelivered int64
}

type Queue struct {
	cfg   Config
	parts []*partition

	enqueued, deduped, redelivered atomic.Int64
}

func New(cfg Config) *Queue {
	q := &Queue{cfg: cfg, parts: make([]*partition, cfg.Partitions)}
	for i := range q.parts {
		q.parts[i] = &partition{inflight: make(map[uint64]*item), seen: make(map[string]time.Time), wake: make(chan struct{})}
	}
	return q
}

func (q *Queue) Partitions() int { return len(q.parts) }

func (q *Queue) PartitionOf(endpointID string) int {
	h := fnv.New32a()
	h.Write([]byte(endpointID))
	return int(h.Sum32() % uint32(len(q.parts)))
}

func (q *Queue) Stats() Stats {
	return Stats{Enqueued: q.enqueued.Load(), Deduped: q.deduped.Load(), Redelivered: q.redelivered.Load()}
}

// Enqueue 加入新的投遞。DedupeWindow 內已經加入過同一個 id 就忽略並回傳 false：
// ingestion 當掉後重新處理同一批事件，不會產生重複的投遞。
func (q *Queue) Enqueue(t domain.Delivery) bool {
	p := q.parts[q.PartitionOf(t.EndpointID)]
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if exp, ok := p.seen[t.ID]; ok && now.Before(exp) {
		q.deduped.Add(1)
		return false
	}
	p.seen[t.ID] = now.Add(q.cfg.DedupeWindow)
	p.prune(now)
	p.pushLocked(t, now, 0)
	q.enqueued.Add(1)
	return true
}

// Requeue 把投遞重新放回佇列（長期重試到期、dead letter 重送），不經過去重。
func (q *Queue) Requeue(t domain.Delivery, delay time.Duration) {
	p := q.parts[q.PartitionOf(t.EndpointID)]
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushLocked(t, time.Now().Add(delay), 0)
}

// Receive 從分區取出最多 limit 則可見的訊息；沒有就等到有或 ctx 結束。
func (q *Queue) Receive(ctx context.Context, part, limit int) ([]Message, error) {
	p := q.parts[part]
	for {
		p.mu.Lock()
		now := time.Now()
		q.redelivered.Add(p.reclaim(now))
		var out []Message
		for len(out) < limit && p.ready.Len() > 0 && !p.ready[0].visibleAt.After(now) {
			it := heap.Pop(&p.ready).(*item)
			it.receives++
			p.nextReceipt++
			it.receipt = p.nextReceipt
			it.visibleAt = now.Add(q.cfg.Visibility)
			p.inflight[it.receipt] = it
			heap.Push(&p.leases, it)
			out = append(out, Message{Partition: part, Receipt: it.receipt, Task: it.task, Receives: it.receives})
		}
		if len(out) > 0 {
			p.mu.Unlock()
			return out, nil
		}
		wait := time.Hour
		if p.ready.Len() > 0 {
			wait = min(wait, p.ready[0].visibleAt.Sub(now))
		}
		if p.leases.Len() > 0 {
			wait = min(wait, p.leases[0].visibleAt.Sub(now))
		}
		wake := p.wake
		p.mu.Unlock()

		t := time.NewTimer(max(wait, time.Millisecond))
		select {
		case <-wake:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		}
		t.Stop()
	}
}

// Ack 刪除處理完的訊息。lease 已經過期（訊息已經交給別人）時回傳 false。
func (q *Queue) Ack(m Message) bool {
	p := q.parts[m.Partition]
	p.mu.Lock()
	defer p.mu.Unlock()
	it, ok := p.inflight[m.Receipt]
	if !ok {
		return false
	}
	delete(p.inflight, m.Receipt)
	heap.Remove(&p.leases, it.leaseIndex)
	return true
}

// Retry 讓訊息在 delay 之後重新出現，並換成新的內容（例如嘗試次數加一）。對應 SQS 的 ChangeMessageVisibility。
func (q *Queue) Retry(m Message, t domain.Delivery, delay time.Duration) bool {
	p := q.parts[m.Partition]
	p.mu.Lock()
	defer p.mu.Unlock()
	it, ok := p.inflight[m.Receipt]
	if !ok {
		return false
	}
	delete(p.inflight, m.Receipt)
	heap.Remove(&p.leases, it.leaseIndex)
	p.pushLocked(t, time.Now().Add(delay), it.receives)
	return true
}

// Len 回傳尚未處理完的訊息數：等待中（含延遲中）與處理中。
func (q *Queue) Len() (waiting, inflight int) {
	for _, p := range q.parts {
		p.mu.Lock()
		waiting += p.ready.Len()
		inflight += len(p.inflight)
		p.mu.Unlock()
	}
	return waiting, inflight
}

type item struct {
	task       domain.Delivery
	visibleAt  time.Time
	receives   int
	receipt    uint64
	readyIndex int
	leaseIndex int
}

type partition struct {
	mu          sync.Mutex
	ready       readyHeap
	leases      leaseHeap
	inflight    map[uint64]*item
	seen        map[string]time.Time
	nextReceipt uint64
	wake        chan struct{}
	ops         int
}

func (p *partition) pushLocked(t domain.Delivery, visibleAt time.Time, receives int) {
	heap.Push(&p.ready, &item{task: t, visibleAt: visibleAt, receives: receives})
	close(p.wake)
	p.wake = make(chan struct{})
}

// reclaim 把 lease 過期的訊息放回可取出的狀態。
func (p *partition) reclaim(now time.Time) int64 {
	var n int64
	for p.leases.Len() > 0 && !p.leases[0].visibleAt.After(now) {
		it := heap.Pop(&p.leases).(*item)
		delete(p.inflight, it.receipt)
		heap.Push(&p.ready, it)
		n++
	}
	return n
}

func (p *partition) prune(now time.Time) {
	p.ops++
	if p.ops%4096 != 0 {
		return
	}
	for id, exp := range p.seen {
		if now.After(exp) {
			delete(p.seen, id)
		}
	}
}

type readyHeap []*item

func (h readyHeap) Len() int           { return len(h) }
func (h readyHeap) Less(i, j int) bool { return h[i].visibleAt.Before(h[j].visibleAt) }
func (h readyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].readyIndex, h[j].readyIndex = i, j
}
func (h *readyHeap) Push(x any) {
	it := x.(*item)
	it.readyIndex = len(*h)
	*h = append(*h, it)
}
func (h *readyHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

type leaseHeap []*item

func (h leaseHeap) Len() int           { return len(h) }
func (h leaseHeap) Less(i, j int) bool { return h[i].visibleAt.Before(h[j].visibleAt) }
func (h leaseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].leaseIndex, h[j].leaseIndex = i, j
}
func (h *leaseHeap) Push(x any) {
	it := x.(*item)
	it.leaseIndex = len(*h)
	*h = append(*h, it)
}
func (h *leaseHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}
