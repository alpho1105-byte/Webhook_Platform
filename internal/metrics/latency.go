package metrics

import (
	"slices"
	"sync"
	"time"
)

// Latency 收集延遲樣本。nil 的 *Latency 可以安全呼叫 Observe，方便不需要指標的元件。
type Latency struct {
	mu      sync.Mutex
	samples []time.Duration
}

type Summary struct {
	Count int
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

func (l *Latency) Observe(d time.Duration) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

func (l *Latency) Reset() {
	l.mu.Lock()
	l.samples = nil
	l.mu.Unlock()
}

func (l *Latency) Summary() Summary {
	l.mu.Lock()
	sorted := slices.Clone(l.samples)
	l.mu.Unlock()

	if len(sorted) == 0 {
		return Summary{}
	}
	slices.Sort(sorted)
	at := func(p float64) time.Duration {
		return sorted[min(len(sorted)-1, int(p*float64(len(sorted))))]
	}
	return Summary{Count: len(sorted), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: sorted[len(sorted)-1]}
}
