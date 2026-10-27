// simulate 讓 400 個客戶註冊上千個 webhook 端點，內部服務在 20 秒內發布 8 萬個事件。
// 端點有的健康、有的偶爾出錯、有的很慢、有的中途停機、有的一直掛著、有的已經下線（410）；
// 途中客戶停用或改了網址，平台的 worker 與 ingestion 也各自當掉。時間是壓縮過的：
// 「三天內持續重試」在模擬裡是 8 秒。最後驗證每個該送達的事件都送達、不該送的沒有送。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"webhook-platform/internal/app"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/retry"
	"webhook-platform/internal/urlcheck"
)

const (
	userCount       = 400
	busyUsers       = 20
	trafficFor      = 20 * time.Second
	eventsPerSecond = 4_000
	changeGrace     = 50 * time.Millisecond
)

var eventTypes = func() []string {
	var out []string
	for _, r := range []string{"invoice", "payment", "customer", "subscription", "issue"} {
		for _, a := range []string{"created", "updated", "completed", "deleted"} {
			out = append(out, r+"."+a)
		}
	}
	return out
}()

func main() {
	seed := flag.Uint64("seed", 42, "random seed")
	verbose := flag.Bool("v", false, "print component logs")
	flag.Parse()

	level := slog.LevelError + 4
	if *verbose {
		level = slog.LevelInfo
	}
	if err := run(*seed, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))); err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(1)
	}
}

// slot 是一個客戶端點。
type slot struct {
	key        string
	role       role
	user       string
	types      []string
	endpointID string

	// 中途停用或更換網址：cutoff 是 API 回傳的時間，之後舊網址不該再收到任何請求。
	deactivateAt, changeAt time.Duration
	cutoff                 time.Time
	newKey                 string
}

type published struct {
	id   string
	user string
	typ  string
	at   time.Time
}

type sim struct {
	app   *app.App
	rec   *receivers
	slots []*slot
	start time.Time

	mu     sync.Mutex
	events []published

	wrongOwnerRejected, ssrfRejected int
}

func simOptions(dbPath string, logger *slog.Logger) app.Options {
	opts := app.DefaultOptions(dbPath)
	opts.Logger = logger
	opts.URLs = urlcheck.Policy{AllowLoopbackHTTP: true}
	opts.Delivery.RequestTimeout = time.Second
	opts.Delivery.BreakerCooldown = 500 * time.Millisecond
	opts.Delivery.DeferDelay = 20 * time.Millisecond
	opts.Queue.Visibility = 2 * time.Second
	opts.Policy = retry.Policy{QuickAttempts: 5, QuickBase: 20 * time.Millisecond, QuickCap: 320 * time.Millisecond,
		SlowBase: 250 * time.Millisecond, SlowCap: 2 * time.Second, Deadline: 8 * time.Second}
	opts.SchedulerTick = 100 * time.Millisecond
	return opts
}

func run(seed uint64, logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "webhook-sim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	rec, err := newReceivers()
	if err != nil {
		return err
	}
	defer rec.srv.Close()

	opts := simOptions(filepath.Join(dir, "webhooks.db"), logger)
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Start(ctx)

	s := &sim{app: a, rec: rec}
	rng := rand.New(rand.NewPCG(seed, 1))
	setup := time.Now()
	if err := s.register(ctx, rng); err != nil {
		return err
	}
	fmt.Printf("setup     %d customers registered %d endpoints in %s (every URL answered a verification challenge)\n",
		userCount, len(s.slots), time.Since(setup).Round(time.Millisecond))
	fmt.Printf("          %s\n", describeRoles(s.slots))
	fmt.Printf("          rejected: %d registrations whose URL did not echo the challenge, %d internal addresses (SSRF)\n",
		s.wrongOwnerRejected, s.ssrfRejected)
	fmt.Printf("time      compressed: quick retries %s-%s, long retries %s-%s, give up after %s (stands for 3 days)\n\n",
		opts.Policy.QuickBase, opts.Policy.QuickCap, opts.Policy.SlowBase, opts.Policy.SlowCap, opts.Policy.Deadline)

	s.traffic(ctx, rng)
	s.recover(ctx)
	for _, sl := range s.slots {
		if sl.role == healthy && sl.deactivateAt == 0 && sl.changeAt == 0 && rng.Float64() < 0.05 {
			rec.attack(sl.key)
		}
	}

	drainStart := time.Now()
	err = waitFor(60*time.Second, func() bool {
		waiting, inflight := a.Queue.Len()
		pending, _ := a.Store.PendingRetries(ctx)
		return waiting == 0 && inflight == 0 && pending == 0
	})
	fmt.Printf("drain     queue and retry store empty %s after the last event\n\n", time.Since(drainStart).Round(time.Millisecond))
	if err != nil {
		fmt.Println("  warning: did not drain:", err)
	}
	s.report(ctx)
	return verify(ctx, s)
}

// register 替每個客戶註冊 1 到 3 個端點，並試著註冊幾個不該通過的網址。
func (s *sim) register(ctx context.Context, rng *rand.Rand) error {
	pickRole := func() role {
		switch r := rng.Float64(); {
		case r < 0.78:
			return healthy
		case r < 0.86:
			return flaky
		case r < 0.90:
			return slow
		case r < 0.94:
			return outage
		case r < 0.96:
			return rateLimited
		case r < 0.98:
			return dead
		default:
			return gone
		}
	}
	for u := range userCount {
		for n := range 1 + rng.IntN(3) {
			sl := &slot{key: fmt.Sprintf("u%03d-e%d", u, n), role: pickRole(), user: fmt.Sprintf("user-%03d", u)}
			perm := rng.Perm(len(eventTypes))[:3+rng.IntN(6)]
			for _, i := range perm {
				sl.types = append(sl.types, eventTypes[i])
			}
			t := &target{role: sl.role}
			switch sl.role {
			case outage:
				t.downFrom = time.Duration(2+rng.IntN(14)) * time.Second
				t.downUntil = t.downFrom + time.Duration(2000+rng.IntN(3000))*time.Millisecond
				t.hang = rng.IntN(2) == 0
			case dead:
				t.downUntil = trafficFor + time.Second
			case healthy:
				switch r := rng.Float64(); {
				case r < 0.025:
					sl.deactivateAt = time.Duration(3+rng.IntN(14)) * time.Second
				case r < 0.05:
					sl.changeAt = time.Duration(3+rng.IntN(14)) * time.Second
				}
			}
			url := s.rec.add(sl.key, t)
			ep, err := s.app.Endpoints.Register(ctx, sl.user, url, sl.types)
			if err != nil {
				return fmt.Errorf("register %s: %w", sl.key, err)
			}
			t.secret.Store(ep.Secret)
			sl.endpointID = ep.ID
			s.slots = append(s.slots, sl)
		}
	}

	for i := range 20 {
		url := s.rec.add(fmt.Sprintf("stranger-%d", i), &target{role: healthy, wrongOwner: true})
		if _, err := s.app.Endpoints.Register(ctx, "user-000", url, []string{"invoice.created"}); errors.Is(err, domain.ErrOwnership) {
			s.wrongOwnerRejected++
		}
	}
	for _, url := range []string{"http://169.254.169.254/latest/meta-data", "https://10.0.0.8/hooks", "http://192.168.1.20/hook"} {
		if _, err := s.app.Endpoints.Register(ctx, "user-000", url, []string{"invoice.created"}); errors.Is(err, domain.ErrInvalidURL) {
			s.ssrfRejected++
		}
	}
	return nil
}

// traffic 以固定速率發布事件 20 秒，並依時間表注入故障與設定變更。
func (s *sim) traffic(ctx context.Context, rng *rand.Rand) {
	s.start = time.Now()
	s.rec.start = s.start
	end := s.start.Add(trafficFor)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		prng := rand.New(rand.NewPCG(rng.Uint64(), 2))
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for now := range ticker.C {
			if now.After(end) {
				return
			}
			for range eventsPerSecond / 100 {
				u := prng.IntN(userCount)
				if prng.Float64() < 0.3 {
					u = prng.IntN(busyUsers)
				}
				user, typ := fmt.Sprintf("user-%03d", u), eventTypes[prng.IntN(len(eventTypes))]
				data, _ := json.Marshal(map[string]any{"amount": prng.IntN(100_000), "currency": "TWD"})
				e := s.app.Publish(user, typ, data)
				s.mu.Lock()
				s.events = append(s.events, published{id: e.ID, user: user, typ: typ, at: e.CreatedAt})
				s.mu.Unlock()
			}
		}
	}()

	for _, sl := range s.slots {
		switch {
		case sl.deactivateAt > 0:
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Until(s.start.Add(sl.deactivateAt)))
				if _, err := s.app.Endpoints.SetActive(ctx, sl.user, sl.endpointID, false); err == nil {
					sl.cutoff = time.Now()
				}
			}()
		case sl.changeAt > 0:
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Until(s.start.Add(sl.changeAt)))
				sl.newKey = sl.key + "-v2"
				t := &target{role: healthy}
				t.secret.Store(s.rec.get(sl.key).secret.Load())
				url := s.rec.add(sl.newKey, t)
				if _, err := s.app.Endpoints.Update(ctx, sl.user, sl.endpointID, &url, nil); err == nil {
					sl.cutoff = time.Now()
				}
			}()
		}
	}

	for _, f := range []struct {
		at   time.Duration
		what string
		do   func()
	}{
		{6 * time.Second, "delivery worker 1 crashed mid-flight and was replaced", func() { s.app.CrashWorker(1) }},
		{10 * time.Second, "ingestion crashed and restarted from its last committed offsets", s.app.CrashIngest},
		{14 * time.Second, "delivery worker 2 crashed mid-flight and was replaced", func() { s.app.CrashWorker(2) }},
	} {
		time.Sleep(time.Until(s.start.Add(f.at)))
		f.do()
		fmt.Printf("fault     %4.1fs  %s\n", time.Since(s.start).Seconds(), f.what)
	}
	wg.Wait()
	fmt.Printf("traffic   %d events published in %.1fs\n", len(s.events), time.Since(s.start).Seconds())
}

// recover 讓一直掛著的端點在流量結束後修好：客戶收到暫停通知，重新啟用端點並重送 dead letter。
func (s *sim) recover(ctx context.Context) {
	time.Sleep(time.Until(s.start.Add(trafficFor + 1500*time.Millisecond)))
	requeued, endpoints := 0, 0
	for _, sl := range s.slots {
		if sl.role != dead {
			continue
		}
		if _, err := s.app.Endpoints.SetActive(ctx, sl.user, sl.endpointID, true); err != nil {
			continue
		}
		n, err := s.app.Endpoints.Redrive(ctx, sl.user, sl.endpointID)
		if err == nil {
			requeued += n
			endpoints++
		}
	}
	fmt.Printf("recover   %.1fs  %d dead endpoints fixed; customers reactivated them and redrove %d dead letters\n",
		time.Since(s.start).Seconds(), endpoints, requeued)
}
