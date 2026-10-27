package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"webhook-platform/internal/domain"
	"webhook-platform/internal/metrics"
)

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// ms 格式化延遲。Windows 上 time.Now 約每 0.5ms 才前進一格，比這更短的間隔量起來是 0。
func ms(d time.Duration) string {
	if d < 500*time.Microsecond {
		return "<0.5ms"
	}
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func describeRoles(slots []*slot) string {
	count := make(map[string]int)
	for _, sl := range slots {
		name := string(sl.role)
		switch {
		case sl.deactivateAt > 0:
			name = "deactivated mid-run"
		case sl.changeAt > 0:
			name = "URL changed mid-run"
		}
		count[name]++
	}
	var parts []string
	for _, name := range []string{"healthy", "flaky", "slow", "outage", "rate-limited", "dead", "gone", "deactivated mid-run", "URL changed mid-run"} {
		parts = append(parts, fmt.Sprintf("%d %s", count[name], name))
	}
	return strings.Join(parts, ", ")
}

func (s *sim) report(ctx context.Context) {
	st, qs := s.app.Stats, s.app.Queue.Stats()
	promoted, expired := s.app.Scheduler.Stats()
	hits, misses := s.app.CacheStats()
	fmt.Printf("ingest    %d deliveries created; %d duplicates from the ingestion replay dropped by the queue's dedupe window\n",
		qs.Enqueued, qs.Deduped)
	fmt.Printf("          endpoint lookups: %.1f%% served from cache (%d misses)\n", 100*float64(hits)/float64(max(1, hits+misses)), misses)
	fmt.Printf("deliver   %d HTTP attempts, %d delivered; %d messages reappeared after a crashed worker's lease expired\n",
		st.Attempts.Load(), st.Delivered.Load(), qs.Redelivered)
	fmt.Printf("retry     %d quick retries in the queue, %d moved to the retry store, %d brought back by the scheduler, %d expired\n",
		st.Retried.Load(), st.Escalated.Load(), promoted, expired)
	fmt.Printf("          %d attempts skipped because the endpoint's circuit was open, %d deferred by the per-endpoint limit\n",
		st.SkippedOpen.Load(), st.Deferred.Load())
	fmt.Printf("          %d dead-lettered, %d canceled because the customer deactivated the endpoint\n\n",
		st.DeadLettered.Load(), st.Canceled.Load())
}

func verify(ctx context.Context, s *sim) error {
	fmt.Println("verify")
	byUser := make(map[string][]*slot)
	for _, sl := range s.slots {
		byUser[sl.user] = append(byUser[sl.user], sl)
	}
	dlq, err := s.app.Store.DeadLetterReasons(ctx)
	if err != nil {
		return err
	}

	var expected, missing, goneExpected, goneWrong, changedExpected, changedMissing int
	var healthyLat metrics.Latency
	outageMissing, outageExpected := 0, 0
	deadMissing, deadExpected := 0, 0
	for _, e := range s.events {
		for _, sl := range byUser[e.user] {
			if !slices.Contains(sl.types, e.typ) || sl.deactivateAt > 0 {
				continue
			}
			t := s.rec.get(sl.key)
			switch {
			case sl.role == gone:
				goneExpected++
				if !strings.HasPrefix(dlq[domain.DeliveryID(e.id, sl.endpointID)], "HTTP 410") || accepted(t, e.id) {
					goneWrong++
				}
			case sl.changeAt > 0:
				changedExpected++
				newT := s.rec.get(sl.newKey)
				ok := accepted(newT, e.id) || (e.at.Before(sl.cutoff) && accepted(t, e.id))
				if !ok {
					changedMissing++
				}
			default:
				expected++
				ok := accepted(t, e.id)
				if !ok {
					missing++
				}
				switch sl.role {
				case outage:
					outageExpected++
					if !ok {
						outageMissing++
					}
				case dead:
					deadExpected++
					if !ok {
						deadMissing++
					}
				case healthy:
					if ok {
						t.mu.Lock()
						healthyLat.Observe(t.accepted[e.id].Sub(e.at))
						t.mu.Unlock()
					}
				}
			}
		}
	}

	dups := 0
	for _, sl := range s.slots {
		for _, key := range []string{sl.key, sl.newKey} {
			if t := s.rec.get(key); t != nil {
				t.mu.Lock()
				dups += t.dups
				t.mu.Unlock()
			}
		}
	}
	check(fmt.Sprintf("every event reached every active subscribed endpoint at least once (%d deliveries, incl. %d to flaky/slow/rate-limited/outage endpoints)",
		expected, expected-countRole(s, healthy)), missing == 0)
	if missing > 0 {
		fmt.Printf("          missing %d (outage %d of %d, dead %d of %d)\n", missing, outageMissing, outageExpected, deadMissing, deadExpected)
	}
	fmt.Printf("          receivers saw %d duplicate deliveries (at-least-once) and processed each event once by webhook-id\n", dups)
	check(fmt.Sprintf("endpoints that were down for 2-5s got every event once they recovered (%d deliveries)", outageExpected), outageMissing == 0 && outageExpected > 0)
	check(fmt.Sprintf("endpoints answering 410 Gone were not retried: all %d deliveries went straight to the dead letter queue", goneExpected), goneWrong == 0)

	suspendedOnce := 0
	deadCount := 0
	for _, sl := range s.slots {
		if sl.role != dead {
			continue
		}
		deadCount++
		notes, _ := s.app.Store.Notifications(ctx, sl.user)
		n := 0
		for _, note := range notes {
			if note.EndpointID == sl.endpointID {
				n++
			}
		}
		if n == 1 {
			suspendedOnce++
		}
	}
	check(fmt.Sprintf("each always-failing endpoint was suspended after the deadline and its customer notified exactly once (%d endpoints)", deadCount),
		suspendedOnce == deadCount && deadCount > 0)
	check(fmt.Sprintf("after the customers fixed and reactivated them, redrive delivered all %d of their events", deadExpected), deadMissing == 0 && deadExpected > 0)

	late, changed := 0, 0
	for _, sl := range s.slots {
		if sl.deactivateAt == 0 && sl.changeAt == 0 {
			continue
		}
		changed++
		if last := s.rec.get(sl.key).lastArrival.Load(); last > sl.cutoff.Add(changeGrace).UnixNano() {
			late++
		}
	}
	check(fmt.Sprintf("no request reached a deactivated endpoint or an old URL after the API call returned (%d endpoints, %s grace)", changed, changeGrace), late == 0)
	check(fmt.Sprintf("endpoints that changed URL got every event at the old or new URL, and later events at the new one (%d deliveries)", changedExpected), changedMissing == 0)

	check(fmt.Sprintf("every request from the platform had a valid signature; %d forged requests rejected, %d replays ignored by webhook-id",
		s.rec.forgedRejected.Load(), s.rec.replaysIgnored.Load()),
		s.rec.invalidSig.Load() == 0 && s.rec.forgedRejected.Load() > 0 && s.rec.forgedRejected.Load() == s.rec.replaysIgnored.Load())
	check(fmt.Sprintf("registrations without proof of ownership (%d) and to internal addresses (%d) were refused", s.wrongOwnerRejected, s.ssrfRejected),
		s.wrongOwnerRejected == 20 && s.ssrfRejected == 3)

	l := healthyLat.Summary()
	fmt.Printf("  latency   event published -> received by a healthy endpoint: p50=%s p95=%s p99=%s max=%s (%d deliveries)\n",
		ms(l.P50), ms(l.P95), ms(l.P99), ms(l.Max), l.Count)
	if failures > 0 {
		return fmt.Errorf("%d checks failed", failures)
	}
	return nil
}

func accepted(t *target, id string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.accepted[id]
	return ok
}

func countRole(s *sim, r role) int {
	byUser := make(map[string][]*slot)
	for _, sl := range s.slots {
		byUser[sl.user] = append(byUser[sl.user], sl)
	}
	n := 0
	for _, e := range s.events {
		for _, sl := range byUser[e.user] {
			if sl.role == r && sl.deactivateAt == 0 && sl.changeAt == 0 && slices.Contains(sl.types, e.typ) {
				n++
			}
		}
	}
	return n
}
