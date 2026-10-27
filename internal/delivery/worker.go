// Package delivery 把投遞工作實際送到客戶的端點。
package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"webhook-platform/internal/directory"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/retry"
	"webhook-platform/internal/signing"
	"webhook-platform/internal/store"
	"webhook-platform/internal/urlcheck"
)

type Config struct {
	RequestTimeout time.Duration
	// MaxInFlight 是一個 worker 同時進行中的請求數。
	MaxInFlight int
	// MaxPerEndpoint 限制對單一端點的同時請求數：慢的端點最多佔住這麼多名額，不會吃光整個 worker。
	MaxPerEndpoint   int
	ReceiveBatch     int
	BreakerThreshold int
	BreakerCooldown  time.Duration
	// DeferDelay 是端點的名額已滿時，訊息延後多久再處理。
	DeferDelay time.Duration
}

func DefaultConfig() Config {
	return Config{RequestTimeout: 10 * time.Second, MaxInFlight: 256, MaxPerEndpoint: 8, ReceiveBatch: 32,
		BreakerThreshold: 5, BreakerCooldown: 30 * time.Second, DeferDelay: 50 * time.Millisecond}
}

// Stats 是所有 worker 共用的計數器。
type Stats struct {
	Attempts, Delivered, Retried, Escalated, DeadLettered, Canceled, SkippedOpen, Deferred atomic.Int64
}

// Deps 是 worker 需要的外部元件。
type Deps struct {
	Queue     *queue.Queue
	Directory *directory.Directory
	Store     *store.Store
	Policy    retry.Policy
	Suspender retry.Suspender
	URLs      urlcheck.Policy
	Stats     *Stats
	Log       *slog.Logger
}

// NewClient 建立投遞用的 HTTP client：每次建立連線都再檢查一次目的地址。
func NewClient(urls urlcheck.Policy, timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: timeout, Control: urls.DialControl}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{DialContext: dialer.DialContext, MaxIdleConns: 4096, MaxIdleConnsPerHost: 1024,
			IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: timeout},
		// 不跟隨轉址：轉址可以把請求導到註冊時沒檢查過的位址。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Worker 負責一組佇列分區。同一個端點的投遞都在同一個分區，
// 所以每個端點的併發計數與 circuit breaker 只存在這個 worker 的記憶體裡。
type Worker struct {
	parts  []int
	cfg    Config
	d      Deps
	client *http.Client
	sem    chan struct{}

	mu       sync.Mutex
	breakers map[string]*breaker
	inflight map[string]int
}

func NewWorker(parts []int, cfg Config, d Deps, client *http.Client) *Worker {
	return &Worker{parts: parts, cfg: cfg, d: d, client: client, sem: make(chan struct{}, cfg.MaxInFlight),
		breakers: make(map[string]*breaker), inflight: make(map[string]int)}
}

// Run 處理負責的分區直到 ctx 結束。ctx 結束時處理到一半的訊息不 Ack，
// lease 到期後由接手的 worker 重新處理：這正是 worker 當掉時的情況。
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, part := range w.parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.consume(ctx, part)
		}()
	}
	wg.Wait()
}

func (w *Worker) consume(ctx context.Context, part int) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		msgs, err := w.d.Queue.Receive(ctx, part, w.cfg.ReceiveBatch)
		if err != nil {
			return
		}
		for _, m := range msgs {
			select {
			case w.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-w.sem }()
				w.handle(ctx, m)
			}()
		}
	}
}

func (w *Worker) handle(ctx context.Context, m queue.Message) {
	t := m.Task
	ep, version, ok, err := w.d.Directory.Endpoint(ctx, t.UserID, t.EndpointID)
	if err != nil {
		return // 讀不到設定：不 Ack，lease 到期後重試
	}
	switch {
	case !ok || ep.Status == domain.StatusDisabled:
		// 使用者刪除或停用了端點：還沒送出的投遞一併取消。
		w.d.Stats.Canceled.Add(1)
		w.d.Queue.Ack(m)
		return
	case ep.Status == domain.StatusSuspended:
		added, err := w.d.Store.DeadLetterIfSuspended(ctx, t)
		switch {
		case err != nil:
		case added:
			w.d.Stats.DeadLettered.Add(1)
			w.d.Queue.Ack(m)
		default:
			w.d.Queue.Retry(m, t, 0) // 剛好被重新啟用：用新的設定再處理一次
		}
		return
	case time.Now().After(t.Deadline) && t.Attempt == 0:
		// 一次都還沒送就過期：端點沒有失敗，只是處理得比事件產生得慢，排隊排到期限。
		// 放進 dead letter 讓客戶之後重送，但不能因此把端點當成故障停用。
		w.deadLetter(ctx, m, "not delivered before the deadline: the endpoint could not keep up")
		return
	case time.Now().After(t.Deadline):
		w.giveUp(ctx, m)
		return
	}

	// 讀到設定之後若剛好有失效通知（例如使用者停用了端點），重新處理一次，不用舊設定送出。
	if !w.d.Directory.Current(t.UserID, version) {
		w.d.Queue.Retry(m, t, 0)
		return
	}
	if !w.acquire(ep.ID) {
		w.d.Stats.Deferred.Add(1)
		w.d.Queue.Retry(m, t, w.cfg.DeferDelay)
		return
	}
	defer w.release(ep.ID)

	if !w.allow(ep.ID) {
		// 斷路器打開：不送出，但仍算一次失敗的嘗試，退避與期限照常推進。
		w.d.Stats.SkippedOpen.Add(1)
		w.failed(ctx, m, 0)
		return
	}
	res := w.send(ctx, ep, t)
	if ctx.Err() != nil {
		return
	}
	// 回了 4xx 的端點是活著的，只是不收這個請求：不能讓它把斷路器打開，害其他投遞也被擋下。
	if w.record(ep.ID, res.ok || res.permanent) {
		// 端點剛恢復：排在後面的長期重試不必等各自的退避時間，全部提前。
		go w.d.Store.Expedite(context.Background(), ep.ID, time.Now())
	}
	switch {
	case res.ok:
		w.d.Stats.Delivered.Add(1)
		w.d.Queue.Ack(m)
	case res.permanent:
		w.deadLetter(ctx, m, res.reason)
	default:
		w.failed(ctx, m, res.retryAfter)
	}
}

type result struct {
	ok         bool
	permanent  bool
	reason     string
	retryAfter time.Duration
}

func (w *Worker) send(ctx context.Context, ep domain.Endpoint, t domain.Delivery) result {
	w.d.Stats.Attempts.Add(1)
	ctx, cancel := context.WithTimeout(ctx, w.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(t.Payload))
	if err != nil {
		return result{permanent: true, reason: "invalid endpoint URL"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-platform/1.0")
	if err := signing.Sign(req.Header, ep.Secret, t.EventID, time.Now(), t.Payload); err != nil {
		return result{permanent: true, reason: "invalid signing secret"}
	}
	resp, err := w.client.Do(req)
	if err != nil {
		if errors.Is(err, urlcheck.ErrBlocked) {
			return result{permanent: true, reason: "destination address is not allowed"}
		}
		return result{reason: "request failed"}
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()

	switch c := resp.StatusCode; {
	case c >= 200 && c < 300:
		return result{ok: true}
	case c == http.StatusRequestTimeout || c == http.StatusTooManyRequests || c >= 500:
		return result{reason: fmt.Sprintf("HTTP %d", c), retryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	default:
		// 其他 4xx（例如 400、401、404、410）重試也不會變好，直接進 dead letter queue。
		return result{permanent: true, reason: fmt.Sprintf("HTTP %d", c)}
	}
}

func retryAfter(v string) time.Duration {
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

// failed 處理一次可重試的失敗：前幾次在佇列裡延遲重送，之後交給長期重試。
func (w *Worker) failed(ctx context.Context, m queue.Message, minDelay time.Duration) {
	t := m.Task
	t.Attempt++
	delay := max(w.d.Policy.Backoff(t.Attempt), minDelay)
	if w.d.Policy.Quick(t.Attempt) {
		w.d.Stats.Retried.Add(1)
		w.d.Queue.Retry(m, t, delay)
		return
	}
	if err := w.d.Store.ScheduleRetries(ctx, []store.Retry{{Task: t, NextAt: time.Now().Add(delay)}}); err != nil {
		w.d.Log.Error("schedule retry", "delivery", t.ID, "err", err)
		return
	}
	w.d.Stats.Escalated.Add(1)
	w.d.Queue.Ack(m)
}

func (w *Worker) deadLetter(ctx context.Context, m queue.Message, reason string) {
	if err := w.d.Store.AddDeadLetters(ctx, []store.DeadLetter{{Task: m.Task, Reason: reason}}); err != nil {
		w.d.Log.Error("dead letter", "delivery", m.Task.ID, "err", err)
		return
	}
	w.d.Stats.DeadLettered.Add(1)
	w.d.Queue.Ack(m)
}

func (w *Worker) giveUp(ctx context.Context, m queue.Message) {
	if err := retry.GiveUp(ctx, w.d.Store, w.d.Suspender, []domain.Delivery{m.Task}, w.d.Policy.ExpiredReason()); err != nil {
		w.d.Log.Error("give up", "delivery", m.Task.ID, "err", err)
		return
	}
	w.d.Stats.DeadLettered.Add(1)
	w.d.Queue.Ack(m)
}

func (w *Worker) acquire(endpointID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inflight[endpointID] >= w.cfg.MaxPerEndpoint {
		return false
	}
	w.inflight[endpointID]++
	return true
}

func (w *Worker) release(endpointID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inflight[endpointID]--; w.inflight[endpointID] <= 0 {
		delete(w.inflight, endpointID)
	}
}

func (w *Worker) breaker(endpointID string) *breaker {
	b, ok := w.breakers[endpointID]
	if !ok {
		b = &breaker{threshold: w.cfg.BreakerThreshold, cooldown: w.cfg.BreakerCooldown}
		w.breakers[endpointID] = b
	}
	return b
}

func (w *Worker) allow(endpointID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.breaker(endpointID).allow(time.Now())
}

func (w *Worker) record(endpointID string, ok bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.breaker(endpointID).record(ok, time.Now())
}
