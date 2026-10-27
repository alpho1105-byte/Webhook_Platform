package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"webhook-platform/internal/delivery"
	"webhook-platform/internal/directory"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/endpoints"
	"webhook-platform/internal/eventbus"
	"webhook-platform/internal/ingest"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/retry"
	"webhook-platform/internal/store"
	"webhook-platform/internal/urlcheck"
)

type Options struct {
	DBPath          string
	EventPartitions int
	Queue           queue.Config
	Workers         int
	Delivery        delivery.Config
	Policy          retry.Policy
	SchedulerTick   time.Duration
	IngestCommit    time.Duration
	URLs            urlcheck.Policy
	Logger          *slog.Logger
}

func DefaultOptions(dbPath string) Options {
	d := delivery.DefaultConfig()
	return Options{DBPath: dbPath, EventPartitions: 8, Workers: 4, Delivery: d, Policy: retry.DefaultPolicy(),
		Queue:         queue.Config{Partitions: 32, Visibility: d.RequestTimeout + 5*time.Second, DedupeWindow: 5 * time.Minute},
		SchedulerTick: time.Second, IngestCommit: 200 * time.Millisecond, Logger: slog.Default()}
}

// App 組裝所有元件。正式環境裡 ingestion、delivery worker、scheduler 與管理 API 是各自擴展的服務，
// 這裡放在同一個 process，彼此只透過 event bus、佇列與資料庫溝通。
type App struct {
	opts      Options
	Store     *store.Store
	Events    *eventbus.Topic[domain.Event]
	Offsets   *ingest.Offsets
	Queue     *queue.Queue
	Endpoints *endpoints.Service
	Scheduler *retry.Scheduler
	Stats     *delivery.Stats

	ingestDir, deliveryDir *directory.Directory
	client                 *http.Client

	ctx     context.Context
	mu      sync.Mutex
	ingest  *instance
	workers []*instance
}

type instance struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func New(opts Options) (*App, error) {
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	a := &App{opts: opts, Store: st, Events: eventbus.New[domain.Event](opts.EventPartitions),
		Offsets: ingest.NewOffsets(), Queue: queue.New(opts.Queue), Stats: &delivery.Stats{},
		ingestDir: directory.New(st), deliveryDir: directory.New(st),
		client: delivery.NewClient(opts.URLs, opts.Delivery.RequestTimeout)}
	a.Endpoints = endpoints.New(st, opts.URLs, a.client, []*directory.Directory{a.ingestDir, a.deliveryDir}, a.Queue, opts.Policy,
		opts.Logger.With("component", "endpoints"))
	a.Scheduler = retry.NewScheduler(st, a.Queue, a.Endpoints, opts.Policy, opts.SchedulerTick, opts.Logger.With("component", "scheduler"))
	return a, nil
}

func (a *App) Start(ctx context.Context) {
	a.ctx = ctx
	a.mu.Lock()
	a.ingest = a.startIngest()
	for i := range a.opts.Workers {
		a.workers = append(a.workers, a.startWorker(i))
	}
	a.mu.Unlock()
	go a.Scheduler.Run(ctx)
}

func (a *App) start(run func(context.Context)) *instance {
	ctx, cancel := context.WithCancel(a.ctx)
	inst := &instance{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(inst.done)
		run(ctx)
	}()
	return inst
}

func (a *App) startIngest() *instance {
	in := ingest.New(a.Events, a.Offsets, a.ingestDir, a.Queue,
		ingest.Config{ReadBatch: 500, CommitEvery: a.opts.IngestCommit, Deadline: a.opts.Policy.Deadline}, a.opts.Logger.With("component", "ingest"))
	return a.start(in.Run)
}

// startWorker 啟動第 i 個 worker，負責 i, i+Workers, i+2*Workers... 這些分區。
func (a *App) startWorker(i int) *instance {
	var parts []int
	for p := i; p < a.Queue.Partitions(); p += a.opts.Workers {
		parts = append(parts, p)
	}
	w := delivery.NewWorker(parts, a.opts.Delivery, delivery.Deps{Queue: a.Queue, Directory: a.deliveryDir, Store: a.Store,
		Policy: a.opts.Policy, Suspender: a.Endpoints, URLs: a.opts.URLs, Stats: a.Stats, Log: a.opts.Logger.With("component", "delivery")}, a.client)
	return a.start(w.Run)
}

// Publish 是內部服務發布事件的入口：指派 id 後依 id 分區寫進 event bus。
func (a *App) Publish(userID, eventType string, data json.RawMessage) domain.Event {
	b := make([]byte, 12)
	rand.Read(b)
	e := domain.Event{ID: "evt_" + hex.EncodeToString(b), UserID: userID, Type: eventType, CreatedAt: time.Now(), Data: data}
	a.Events.Append(a.Events.PartitionFor(e.ID), e)
	return e
}

// CrashWorker 讓第 i 個 worker 當掉並立刻換上新的：處理到一半的投遞沒有 Ack，lease 到期後重新出現。
func (a *App) CrashWorker(i int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workers[i].cancel()
	<-a.workers[i].done
	a.workers[i] = a.startWorker(i)
}

// CrashIngest 讓 ingestion 當掉並重新啟動，從上次 commit 的 offset 重新處理。
func (a *App) CrashIngest() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ingest.cancel()
	<-a.ingest.done
	a.ingest = a.startIngest()
}

// CacheStats 回傳 ingestion 端快取的命中與未命中次數。
func (a *App) CacheStats() (hits, misses int64) {
	return a.ingestDir.Stats()
}

func (a *App) Close() error {
	return a.Store.Close()
}
