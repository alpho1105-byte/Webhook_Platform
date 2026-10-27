package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"webhook-platform/internal/signing"
)

// role 是模擬中客戶端點的行為。
type role string

const (
	healthy     role = "healthy"      // 1-10ms 回 200
	flaky       role = "flaky"        // 25% 回 500
	slow        role = "slow"         // 每次都要 100-300ms
	outage      role = "outage"       // 中途停機 2-5 秒（一半回 503、一半卡住直到逾時），之後恢復
	rateLimited role = "rate-limited" // 20% 回 429 + Retry-After
	dead        role = "dead"         // 一直回 503，超過期限後被暫停；模擬結束前修好，由使用者重新啟用並重送
	gone        role = "gone"         // 回 410：重試也沒用
)

// target 是接收端上的一個 URL path，代表一個客戶端點。
type target struct {
	role        role
	secret      atomic.Value // string
	wrongOwner  bool
	downFrom    time.Duration
	downUntil   time.Duration
	hang        bool
	lastArrival atomic.Int64 // unix nano

	mu       sync.Mutex
	accepted map[string]time.Time // webhook-id → 第一次成功處理的時間
	dups     int
	captured *http.Request // 攻擊者截取到的一個合法請求
	body     []byte
}

// receivers 是一台 HTTP server，用不同的 path 扮演上千個客戶端點。
type receivers struct {
	start   time.Time
	srv     *http.Server
	baseURL string

	mu      sync.RWMutex
	targets map[string]*target

	invalidSig, forgedRejected, replaysIgnored atomic.Int64
}

func newReceivers() (*receivers, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &receivers{targets: make(map[string]*target), baseURL: "http://" + ln.Addr().String()}
	r.srv = &http.Server{Handler: r}
	go r.srv.Serve(ln)
	return r, nil
}

func (r *receivers) add(key string, t *target) string {
	t.accepted = make(map[string]time.Time)
	r.mu.Lock()
	r.targets[key] = t
	r.mu.Unlock()
	return r.baseURL + "/r/" + key
}

func (r *receivers) get(key string) *target {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targets[key]
}

func (r *receivers) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	t := r.get(strings.TrimPrefix(req.URL.Path, "/r/"))
	if t == nil {
		http.NotFound(w, req)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	now := time.Now()

	var msg struct {
		ID        string    `json:"id"`
		Type      string    `json:"type"`
		Challenge string    `json:"challenge"`
		CreatedAt time.Time `json:"created_at"`
	}
	json.Unmarshal(body, &msg)
	if msg.Type == "webhook.challenge" {
		if t.wrongOwner {
			http.NotFound(w, req) // 這個網址的主人根本沒有註冊這個 webhook
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"challenge": msg.Challenge})
		return
	}
	attacker := req.Header.Get("X-Sim-Attacker") != ""
	if !attacker {
		t.lastArrival.Store(now.UnixNano())
	}

	elapsed := now.Sub(r.start)
	switch {
	case attacker:
	case t.role == healthy:
		time.Sleep(time.Duration(1+rand.IntN(10)) * time.Millisecond)
	case t.role == flaky:
		if rand.Float64() < 0.25 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	case t.role == slow:
		time.Sleep(time.Duration(100+rand.IntN(200)) * time.Millisecond)
	case t.role == outage:
		if elapsed >= t.downFrom && elapsed < t.downUntil {
			if t.hang {
				time.Sleep(1500 * time.Millisecond) // 比平台的逾時還久
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	case t.role == rateLimited:
		if rand.Float64() < 0.2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
	case t.role == dead:
		if elapsed < t.downUntil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	case t.role == gone:
		w.WriteHeader(http.StatusGone)
		return
	}

	// 客戶端的處理方式：先驗簽章，再依 webhook-id 去重，同一個事件只處理一次。
	secret, _ := t.secret.Load().(string)
	if err := signing.Verify(req.Header, secret, body, now, 5*time.Minute); err != nil {
		if attacker {
			r.forgedRejected.Add(1)
		} else {
			r.invalidSig.Add(1)
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	id := req.Header.Get(signing.HeaderID)
	t.mu.Lock()
	_, seen := t.accepted[id]
	switch {
	case seen && attacker:
		r.replaysIgnored.Add(1)
	case seen:
		t.dups++
	default:
		t.accepted[id] = now
	}
	if t.captured == nil && !attacker {
		t.captured, t.body = req.Clone(req.Context()), body
	}
	t.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// attack 扮演攻擊者，對端點送出兩種請求：用錯誤密鑰簽的偽造請求，簽章驗證要擋下；
// 原封不動重放截取到的合法請求，簽章雖然正確，但 webhook-id 已經處理過，接收端不會再處理一次。
func (r *receivers) attack(key string) {
	t := r.get(key)
	t.mu.Lock()
	captured, body := t.captured, t.body
	t.mu.Unlock()
	if captured == nil {
		return
	}
	url := r.baseURL + "/r/" + key

	forged, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	signing.Sign(forged.Header, signing.NewSecret(), "evt_forged", time.Now(), body)
	forged.Header.Set("X-Sim-Attacker", "1")
	if resp, err := http.DefaultClient.Do(forged); err == nil {
		resp.Body.Close()
	}

	replay, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	for _, h := range []string{signing.HeaderID, signing.HeaderTimestamp, signing.HeaderSignature, "Content-Type"} {
		replay.Header.Set(h, captured.Header.Get(h))
	}
	replay.Header.Set("X-Sim-Attacker", "1")
	if resp, err := http.DefaultClient.Do(replay); err == nil {
		resp.Body.Close()
	}
}
