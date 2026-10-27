package queue

import (
	"context"
	"testing"
	"time"

	"webhook-platform/internal/domain"
)

func task(id string) domain.Delivery {
	return domain.Delivery{ID: id, EndpointID: "ep_1"}
}

func receive(t *testing.T, q *Queue, n int) []Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs, err := q.Receive(ctx, q.PartitionOf("ep_1"), n)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return msgs
}

func TestUnackedMessageReappearsAfterVisibilityTimeout(t *testing.T) {
	q := New(Config{Partitions: 4, Visibility: 50 * time.Millisecond, DedupeWindow: time.Minute})
	q.Enqueue(task("d1"))
	first := receive(t, q, 10)
	if len(first) != 1 || first[0].Receives != 1 {
		t.Fatalf("first = %+v", first)
	}
	// consumer 當掉沒有 Ack：lease 到期後訊息重新出現，而原本的 receipt 不能再 Ack。
	again := receive(t, q, 10)
	if len(again) != 1 || again[0].Receives != 2 || q.Stats().Redelivered != 1 {
		t.Fatalf("again = %+v stats=%+v", again, q.Stats())
	}
	if q.Ack(first[0]) {
		t.Fatal("an expired receipt must not delete the message")
	}
	if !q.Ack(again[0]) {
		t.Fatal("ack failed")
	}
	if waiting, inflight := q.Len(); waiting+inflight != 0 {
		t.Fatalf("left %d waiting, %d in flight", waiting, inflight)
	}
}

func TestDedupeWindowDropsRepeatedEnqueues(t *testing.T) {
	q := New(Config{Partitions: 1, Visibility: time.Second, DedupeWindow: time.Minute})
	if !q.Enqueue(task("d1")) || q.Enqueue(task("d1")) {
		t.Fatal("the second enqueue of the same delivery should be dropped")
	}
	// 重試與重送不經過去重。
	q.Requeue(task("d1"), 0)
	if msgs := receive(t, q, 10); len(msgs) != 2 {
		t.Fatalf("got %d messages", len(msgs))
	}
}

func TestRetryDelaysTheMessageAndUpdatesIt(t *testing.T) {
	q := New(Config{Partitions: 1, Visibility: time.Second, DedupeWindow: time.Minute})
	q.Enqueue(task("d1"))
	m := receive(t, q, 1)[0]
	next := m.Task
	next.Attempt = 1
	start := time.Now()
	q.Retry(m, next, 80*time.Millisecond)
	got := receive(t, q, 1)[0]
	if waited := time.Since(start); waited < 70*time.Millisecond {
		t.Fatalf("retried message came back after %s", waited)
	}
	if got.Task.Attempt != 1 {
		t.Fatalf("attempt = %d", got.Task.Attempt)
	}
}
