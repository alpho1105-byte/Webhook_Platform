// Package ingest 把內部服務發布的事件轉成投遞工作：查出訂閱的端點，每個端點一份。
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"webhook-platform/internal/directory"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/eventbus"
	"webhook-platform/internal/queue"
)

// Offsets 保存每個分區處理到哪裡（對應 Kafka consumer group 的 committed offset）。
type Offsets struct {
	mu sync.Mutex
	m  map[int]uint64
}

func NewOffsets() *Offsets {
	return &Offsets{m: make(map[int]uint64)}
}

func (o *Offsets) Load(part int) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.m[part]
}

func (o *Offsets) Commit(part int, offset uint64) {
	o.mu.Lock()
	o.m[part] = offset
	o.mu.Unlock()
}

type Config struct {
	ReadBatch int
	// CommitEvery 控制多久 commit 一次 offset。當掉時，上次 commit 之後的事件會被重新處理，
	// 產生的投遞由佇列依 delivery id 去重。
	CommitEvery time.Duration
	// Deadline 是投遞期限，從事件發生起算。
	Deadline time.Duration
}

type Ingestor struct {
	events  *eventbus.Topic[domain.Event]
	offsets *Offsets
	dir     *directory.Directory
	q       *queue.Queue
	cfg     Config
	log     *slog.Logger
}

func New(events *eventbus.Topic[domain.Event], offsets *Offsets, dir *directory.Directory, q *queue.Queue, cfg Config, log *slog.Logger) *Ingestor {
	return &Ingestor{events: events, offsets: offsets, dir: dir, q: q, cfg: cfg, log: log}
}

// Run 處理所有分區直到 ctx 結束。結束時不做最後一次 commit：這就是當掉的語意。
func (in *Ingestor) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for part := range in.events.Partitions() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.consume(ctx, part)
		}()
	}
	wg.Wait()
}

func (in *Ingestor) consume(ctx context.Context, part int) {
	offset := in.offsets.Load(part)
	lastCommit := time.Now()
	for {
		readCtx, cancel := context.WithTimeout(ctx, in.cfg.CommitEvery)
		batch, err := in.events.Read(readCtx, part, offset, in.cfg.ReadBatch)
		cancel()
		if ctx.Err() != nil {
			return
		}
		for _, e := range batch {
			// 查不到設定時不能跳過事件，否則就違反 at-least-once：停在這裡重試，不前進 offset。
			for err := in.fanOut(ctx, e); err != nil; err = in.fanOut(ctx, e) {
				if ctx.Err() != nil {
					return
				}
				in.log.Error("fan out event", "event", e.ID, "err", err)
				time.Sleep(100 * time.Millisecond)
			}
			offset++
		}
		if err == nil && time.Since(lastCommit) < in.cfg.CommitEvery {
			continue
		}
		in.offsets.Commit(part, offset)
		lastCommit = time.Now()
	}
}

// fanOut 依 (user_id, event_type) 查出端點，每個端點產生一份投遞。payload 在這裡就固定下來。
func (in *Ingestor) fanOut(ctx context.Context, e domain.Event) error {
	eps, err := in.dir.Subscribers(ctx, e.UserID, e.Type)
	if err != nil || len(eps) == 0 {
		return err
	}
	payload, err := json.Marshal(struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		CreatedAt time.Time       `json:"created_at"`
		Data      json.RawMessage `json:"data"`
	}{e.ID, e.Type, e.CreatedAt, e.Data})
	if err != nil {
		return err
	}
	for _, ep := range eps {
		in.q.Enqueue(domain.Delivery{ID: domain.DeliveryID(e.ID, ep.ID), EventID: e.ID, EventType: e.Type, EndpointID: ep.ID,
			UserID: e.UserID, Payload: payload, CreatedAt: e.CreatedAt, Deadline: e.CreatedAt.Add(in.cfg.Deadline)})
	}
	return nil
}
