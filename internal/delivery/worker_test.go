package delivery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"webhook-platform/internal/directory"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/endpoints"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/retry"
	"webhook-platform/internal/signing"
	"webhook-platform/internal/store"
	"webhook-platform/internal/urlcheck"
)

// receiver 依 path 扮演不同的客戶端點，並記錄每個 path 收到的、簽章正確的請求。
type receiver struct {
	mu       sync.Mutex
	secrets  map[string]string
	received map[string]int
	calls    map[string]int
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	var msg struct{ Type, Challenge string }
	json.Unmarshal(body, &msg)
	if msg.Type == "webhook.challenge" {
		json.NewEncoder(w).Encode(map[string]string{"challenge": msg.Challenge})
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[req.URL.Path]++
	switch {
	case req.URL.Path == "/gone":
		w.WriteHeader(http.StatusGone)
	case req.URL.Path == "/down", req.URL.Path == "/flaky" && r.calls["/flaky"] <= 2:
		w.WriteHeader(http.StatusServiceUnavailable)
	case signing.Verify(req.Header, r.secrets[req.URL.Path], body, time.Now(), time.Minute) != nil:
		w.WriteHeader(http.StatusBadRequest)
	default:
		r.received[req.URL.Path]++
	}
}

type env struct {
	t     *testing.T
	ctx   context.Context
	store *store.Store
	q     *queue.Queue
	svc   *endpoints.Service
	stats *Stats
	rec   *receiver
	srv   *httptest.Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rec := &receiver{secrets: map[string]string{}, received: map[string]int{}, calls: map[string]int{}}
	srv := httptest.NewServer(rec)
	t.Cleanup(func() {
		cancel()
		srv.Close()
		time.Sleep(20 * time.Millisecond)
		st.Close()
	})

	urls := urlcheck.Policy{AllowLoopbackHTTP: true}
	policy := retry.Policy{QuickAttempts: 3, QuickBase: 10 * time.Millisecond, QuickCap: 40 * time.Millisecond,
		SlowBase: 50 * time.Millisecond, SlowCap: 100 * time.Millisecond, Deadline: 5 * time.Second}
	client := NewClient(urls, 500*time.Millisecond)
	q := queue.New(queue.Config{Partitions: 2, Visibility: 2 * time.Second, DedupeWindow: time.Minute})
	dir := directory.New(st)
	log := slog.New(slog.DiscardHandler)
	svc := endpoints.New(st, urls, client, []*directory.Directory{dir}, q, policy, log)
	stats := &Stats{}
	cfg := Config{RequestTimeout: 500 * time.Millisecond, MaxInFlight: 16, MaxPerEndpoint: 4, ReceiveBatch: 8,
		BreakerThreshold: 5, BreakerCooldown: 100 * time.Millisecond, DeferDelay: 5 * time.Millisecond}
	w := NewWorker([]int{0, 1}, cfg, Deps{Queue: q, Directory: dir, Store: st, Policy: policy, Suspender: svc, URLs: urls, Stats: stats, Log: log}, client)
	go w.Run(ctx)
	go retry.NewScheduler(st, q, svc, policy, 20*time.Millisecond, log).Run(ctx)
	return &env{t: t, ctx: ctx, store: st, q: q, svc: svc, stats: stats, rec: rec, srv: srv}
}

func (e *env) register(path string) domain.Endpoint {
	e.t.Helper()
	ep, err := e.svc.Register(e.ctx, "u1", e.srv.URL+path, []string{"invoice.paid"})
	if err != nil {
		e.t.Fatalf("register %s: %v", path, err)
	}
	e.rec.mu.Lock()
	e.rec.secrets[path] = ep.Secret
	e.rec.mu.Unlock()
	return ep
}

func (e *env) deliver(ep domain.Endpoint, eventID string, attempt int, deadline time.Time) {
	e.q.Enqueue(domain.Delivery{ID: domain.DeliveryID(eventID, ep.ID), EventID: eventID, EventType: "invoice.paid", EndpointID: ep.ID,
		UserID: "u1", Payload: []byte(`{"id":"` + eventID + `"}`), CreatedAt: time.Now(), Deadline: deadline, Attempt: attempt})
}

func (e *env) drain() {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		waiting, inflight := e.q.Len()
		pending, _ := e.store.PendingRetries(e.ctx)
		if waiting+inflight+pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("not drained: %d waiting, %d in flight, %d pending retries", waiting, inflight, pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDeliversSignedRequestsAndHandlesFailures(t *testing.T) {
	e := newEnv(t)
	ok, gone, flaky := e.register("/ok"), e.register("/gone"), e.register("/flaky")
	later := time.Now().Add(time.Minute)
	e.deliver(ok, "evt_1", 0, later)
	e.deliver(gone, "evt_2", 0, later)
	e.deliver(flaky, "evt_3", 0, later)
	e.drain()

	e.rec.mu.Lock()
	defer e.rec.mu.Unlock()
	if e.rec.received["/ok"] != 1 || e.rec.received["/flaky"] != 1 {
		t.Fatalf("received = %v", e.rec.received)
	}
	if e.rec.calls["/gone"] != 1 {
		t.Fatalf("410 Gone was retried %d times", e.rec.calls["/gone"]-1)
	}
	if e.stats.Retried.Load() != 2 {
		t.Fatalf("retried = %d, want the two 503s", e.stats.Retried.Load())
	}
	dlq, _ := e.store.DeadLetterReasons(e.ctx)
	if dlq[domain.DeliveryID("evt_2", gone.ID)] != "HTTP 410" {
		t.Fatalf("dlq = %v", dlq)
	}
}

func TestDeactivatedEndpointsAreSkippedAndFailingOnesSuspended(t *testing.T) {
	e := newEnv(t)
	ok, down := e.register("/ok"), e.register("/down")
	if _, err := e.svc.SetActive(e.ctx, "u1", ok.ID, false); err != nil {
		t.Fatal(err)
	}
	e.deliver(ok, "evt_1", 0, time.Now().Add(time.Minute))
	// 已經失敗過很多次、期限也到了的投遞：放進 dead letter，端點停用並通知客戶。
	e.deliver(down, "evt_2", 8, time.Now().Add(-time.Second))
	e.drain()

	e.rec.mu.Lock()
	okCalls := e.rec.calls["/ok"]
	e.rec.mu.Unlock()
	if e.stats.Canceled.Load() != 1 || okCalls != 0 {
		t.Fatalf("canceled=%d calls to the deactivated endpoint=%d", e.stats.Canceled.Load(), okCalls)
	}
	got, _ := e.svc.Get(e.ctx, "u1", down.ID)
	notes, _ := e.store.Notifications(e.ctx, "u1")
	if got.Status != domain.StatusSuspended || len(notes) != 1 || !strings.Contains(notes[0].Message, "suspended") {
		t.Fatalf("status=%s notes=%+v", got.Status, notes)
	}

	// 客戶重新啟用並重送：dead letter 被取出放回佇列。
	if _, err := e.svc.SetActive(e.ctx, "u1", down.ID, true); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.Redrive(e.ctx, "u1", down.ID); err != nil || n != 1 {
		t.Fatalf("redrive: n=%d err=%v", n, err)
	}
}
