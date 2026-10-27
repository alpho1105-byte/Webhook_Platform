// Package retry 定義重試的節奏，以及把長期重試放回佇列的 scheduler。
//
// 重試分兩層：前幾次失敗在佇列裡延遲重送（毫秒到秒級，處理短暫的網路問題）；
// 之後改存進資料庫，由 scheduler 定期取出到期的放回佇列（分鐘到小時級，處理端點長時間停機）。
// 佇列不適合放幾小時之後才要處理的訊息（SQS 的延遲上限是 15 分鐘），也不該讓大量等待中的訊息塞住它。
package retry

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"webhook-platform/internal/domain"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/store"
)

type Policy struct {
	// QuickAttempts 次以內的失敗在佇列裡快速重試。
	QuickAttempts       int
	QuickBase, QuickCap time.Duration
	SlowBase, SlowCap   time.Duration
	// Deadline 是從事件發生起算、持續嘗試投遞的期限（SLA），過了就進 dead letter queue。
	Deadline time.Duration
}

func DefaultPolicy() Policy {
	return Policy{QuickAttempts: 5, QuickBase: 100 * time.Millisecond, QuickCap: 2 * time.Second,
		SlowBase: time.Minute, SlowCap: 6 * time.Hour, Deadline: 72 * time.Hour}
}

// Backoff 回傳第 attempt 次失敗（從 1 起算）之後要等多久。
// 上限隨失敗次數指數成長，讓系統在對方出問題時快速退讓；實際等待時間在 0 到上限之間隨機（full jitter），
// 讓同一刻一起失敗的大量投遞分散開來，不會在同一刻一起重試（thundering herd）。
func (p Policy) Backoff(attempt int) time.Duration {
	base, limit, n := p.QuickBase, p.QuickCap, attempt-1
	if attempt > p.QuickAttempts {
		base, limit, n = p.SlowBase, p.SlowCap, attempt-p.QuickAttempts-1
	}
	d := limit
	if n < 30 && base<<n < limit {
		d = base << n
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// Quick 回傳第 attempt 次失敗之後是否仍在佇列內重試。
func (p Policy) Quick(attempt int) bool {
	return attempt < p.QuickAttempts
}

// Suspender 停用持續失敗的端點並通知使用者。
type Suspender interface {
	Suspend(ctx context.Context, endpointID, reason string) error
}

// GiveUp 把超過期限的投遞放進 dead letter queue，並停用對應的端點。
func GiveUp(ctx context.Context, st *store.Store, susp Suspender, tasks []domain.Delivery, reason string) error {
	if len(tasks) == 0 {
		return nil
	}
	items := make([]store.DeadLetter, len(tasks))
	endpoints := make(map[string]bool)
	for i, t := range tasks {
		items[i] = store.DeadLetter{Task: t, Reason: reason}
		endpoints[t.EndpointID] = true
	}
	if err := st.AddDeadLetters(ctx, items); err != nil {
		return err
	}
	for id := range endpoints {
		if err := susp.Suspend(ctx, id, reason); err != nil {
			return err
		}
	}
	return nil
}

// Scheduler 定期取出到期的長期重試放回佇列；已經超過期限的直接放棄。
type Scheduler struct {
	store  *store.Store
	q      *queue.Queue
	susp   Suspender
	policy Policy
	tick   time.Duration
	log    *slog.Logger

	promoted, expired atomic.Int64
}

func NewScheduler(st *store.Store, q *queue.Queue, susp Suspender, policy Policy, tick time.Duration, log *slog.Logger) *Scheduler {
	return &Scheduler{store: st, q: q, susp: susp, policy: policy, tick: tick, log: log}
}

func (s *Scheduler) Stats() (promoted, expired int64) {
	return s.promoted.Load(), s.expired.Load()
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for {
			n, err := s.runOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					s.log.Error("retry scheduler", "err", err)
				}
				break
			}
			if n < 500 {
				break
			}
		}
	}
}

func (s *Scheduler) runOnce(ctx context.Context) (int, error) {
	now := time.Now()
	due, err := s.store.DueRetries(ctx, now, 500)
	if err != nil || len(due) == 0 {
		return 0, err
	}
	var expired, promoted []domain.Delivery
	for _, t := range due {
		if now.After(t.Deadline) {
			expired = append(expired, t)
			continue
		}
		// 先放回佇列再刪除：中間當掉頂多重送一次，反過來則可能永遠遺失。
		s.q.Requeue(t, 0)
		promoted = append(promoted, t)
	}
	if err := s.store.DeleteRetries(ctx, promoted); err != nil {
		return 0, err
	}
	if err := GiveUp(ctx, s.store, s.susp, expired, s.policy.ExpiredReason()); err != nil {
		return 0, err
	}
	s.promoted.Add(int64(len(promoted)))
	s.expired.Add(int64(len(expired)))
	return len(due), nil
}

// ExpiredReason 是超過期限時記錄在 dead letter 與通知裡的原因。
func (p Policy) ExpiredReason() string {
	return "deliveries kept failing for " + p.Deadline.String() + "; endpoint suspended"
}
