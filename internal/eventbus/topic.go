package eventbus

import (
	"context"
	"hash/fnv"
	"sync"
)

// Topic 是 Kafka 風格的分區日誌：每個分區是 append-only 的序列，consumer 自己記錄讀到的 offset。
// 同一個 key（例如 event id）一定落在同一個分區，分區內的順序就是寫入順序。
type Topic[T any] struct {
	parts []*partition[T]
}

type partition[T any] struct {
	mu      sync.Mutex
	records []T
	wake    chan struct{}
}

func New[T any](partitions int) *Topic[T] {
	t := &Topic[T]{parts: make([]*partition[T], partitions)}
	for i := range t.parts {
		t.parts[i] = &partition[T]{wake: make(chan struct{})}
	}
	return t
}

func (t *Topic[T]) Partitions() int { return len(t.parts) }

func (t *Topic[T]) PartitionFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.parts)))
}

// Append 附加到分區尾端，回傳附加後的結尾 offset。
func (t *Topic[T]) Append(part int, values ...T) uint64 {
	p := t.parts[part]
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, values...)
	if len(values) > 0 {
		close(p.wake)
		p.wake = make(chan struct{})
	}
	return uint64(len(p.records))
}

// Read 從 offset 開始讀最多 max 筆；沒有新資料時阻塞到有資料或 ctx 結束。
func (t *Topic[T]) Read(ctx context.Context, part int, offset uint64, max int) ([]T, error) {
	p := t.parts[part]
	for {
		p.mu.Lock()
		if offset < uint64(len(p.records)) {
			end := min(offset+uint64(max), uint64(len(p.records)))
			out := append([]T(nil), p.records[offset:end]...)
			p.mu.Unlock()
			return out, nil
		}
		wake := p.wake
		p.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
