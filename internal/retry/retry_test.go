package retry

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"webhook-platform/internal/domain"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/store"
)

func TestBackoffCeilingGrowsExponentiallyAndIsCapped(t *testing.T) {
	p := Policy{QuickAttempts: 5, QuickBase: 100 * time.Millisecond, QuickCap: time.Second,
		SlowBase: time.Minute, SlowCap: 6 * time.Hour, Deadline: 72 * time.Hour}
	for attempt, full := range map[int]time.Duration{
		1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 4: 800 * time.Millisecond, 5: time.Second, // 快速重試，上限 1s
		6: time.Minute, 7: 2 * time.Minute, 20: 6 * time.Hour, 500: 6 * time.Hour, // 長期重試，上限 6h
	} {
		var longest time.Duration
		for range 200 {
			d := p.Backoff(attempt)
			if d < 0 || d > full {
				t.Fatalf("attempt %d: %s not in [0, %s]", attempt, d, full)
			}
			longest = max(longest, d)
		}
		if longest < full/2 {
			t.Fatalf("attempt %d: 200 samples never exceeded %s; the ceiling should be %s", attempt, longest, full)
		}
	}
	if !p.Quick(4) || p.Quick(5) {
		t.Fatal("the fifth failure should move the delivery to the retry store")
	}
}

type suspender struct{ ids []string }

func (s *suspender) Suspend(_ context.Context, id, _ string) error {
	s.ids = append(s.ids, id)
	return nil
}

func TestSchedulerPromotesDueRetriesAndGivesUpAfterTheDeadline(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	q := queue.New(queue.Config{Partitions: 1, Visibility: time.Second, DedupeWindow: time.Minute})
	susp := &suspender{}
	s := NewScheduler(st, q, susp, DefaultPolicy(), time.Hour, slog.New(slog.DiscardHandler))

	now := time.Now()
	due := domain.Delivery{ID: "due", EndpointID: "ep_ok", Attempt: 5, Deadline: now.Add(time.Hour)}
	later := domain.Delivery{ID: "later", EndpointID: "ep_ok", Attempt: 5, Deadline: now.Add(time.Hour)}
	expired := domain.Delivery{ID: "expired", EndpointID: "ep_dead", Attempt: 9, Deadline: now.Add(-time.Second)}
	st.ScheduleRetries(ctx, []store.Retry{{Task: due, NextAt: now.Add(-time.Millisecond)}, {Task: later, NextAt: now.Add(time.Hour)},
		{Task: expired, NextAt: now.Add(-time.Millisecond)}})

	if _, err := s.runOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if waiting, _ := q.Len(); waiting != 1 {
		t.Fatalf("queue has %d messages, want the one due retry", waiting)
	}
	if pending, _ := st.PendingRetries(ctx); pending != 1 {
		t.Fatalf("retry store has %d rows, want only the later one", pending)
	}
	dlq, _ := st.DeadLetterReasons(ctx)
	if dlq["expired"] == "" || len(susp.ids) != 1 || susp.ids[0] != "ep_dead" {
		t.Fatalf("dlq=%v suspended=%v", dlq, susp.ids)
	}
}

// 回歸測試：scheduler 把重試放回佇列之後、刪除那一列之前，worker 可能已經又失敗一次並寫入下一次重試。
// 刪除只能刪掉自己放回去的那一次，否則那個投遞就從此消失了。
func TestDeletingAPromotedRetryKeepsTheNextAttempt(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	d := domain.Delivery{ID: "d1", EndpointID: "ep_1", Attempt: 5}
	st.ScheduleRetries(ctx, []store.Retry{{Task: d, NextAt: time.Now()}})
	promoted := d
	d.Attempt = 6
	st.ScheduleRetries(ctx, []store.Retry{{Task: d, NextAt: time.Now().Add(time.Second)}})
	st.DeleteRetries(ctx, []domain.Delivery{promoted})
	if n, _ := st.PendingRetries(ctx); n != 1 {
		t.Fatalf("pending = %d: the sixth attempt was deleted", n)
	}
}
