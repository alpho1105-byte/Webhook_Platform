// Package endpoints 是客戶管理 webhook 端點的服務：註冊、修改、停用、刪除與重送失敗的投遞。
package endpoints

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"time"

	"webhook-platform/internal/directory"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/queue"
	"webhook-platform/internal/retry"
	"webhook-platform/internal/signing"
	"webhook-platform/internal/store"
	"webhook-platform/internal/urlcheck"
)

var eventType = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$`)

type Service struct {
	store  *store.Store
	urls   urlcheck.Policy
	client *http.Client
	caches []*directory.Directory
	q      *queue.Queue
	policy retry.Policy
	log    *slog.Logger
}

// New 建立服務。caches 是所有需要失效通知的快取；正式環境裡它們在不同的 process，通知經由 pub/sub 廣播。
func New(st *store.Store, urls urlcheck.Policy, client *http.Client, caches []*directory.Directory, q *queue.Queue, policy retry.Policy, log *slog.Logger) *Service {
	return &Service{store: st, urls: urls, client: client, caches: caches, q: q, policy: policy, log: log}
}

// Register 註冊新端點。URL 必須通過檢查，而且對方要正確回應驗證挑戰，
// 證明它真的是這個網址的擁有者，而不是隨便填一個別人的網址讓平台去轟炸。
func (s *Service) Register(ctx context.Context, userID, url string, types []string) (domain.Endpoint, error) {
	types, err := normalizeTypes(types)
	if err != nil {
		return domain.Endpoint{}, err
	}
	if err := s.verify(ctx, url); err != nil {
		return domain.Endpoint{}, err
	}
	now := time.Now()
	ep := domain.Endpoint{ID: "ep_" + randomHex(8), UserID: userID, URL: url, Secret: signing.NewSecret(),
		EventTypes: types, Status: domain.StatusActive, CreatedAt: now, UpdatedAt: now}
	version, err := s.store.CreateEndpoint(ctx, ep)
	if err != nil {
		return domain.Endpoint{}, err
	}
	s.invalidate(userID, version)
	return ep, nil
}

// Update 修改 URL 或訂閱的事件類型；改了 URL 就要重新驗證擁有權。
func (s *Service) Update(ctx context.Context, userID, id string, url *string, types []string) (domain.Endpoint, error) {
	if types != nil {
		var err error
		if types, err = normalizeTypes(types); err != nil {
			return domain.Endpoint{}, err
		}
	}
	if url != nil {
		if err := s.verify(ctx, *url); err != nil {
			return domain.Endpoint{}, err
		}
	}
	return s.update(ctx, userID, id, func(ep *domain.Endpoint) error {
		if url != nil {
			ep.URL = *url
		}
		if types != nil {
			ep.EventTypes = types
		}
		return nil
	})
}

// SetActive 啟用或停用端點。停用後還沒送出的投遞會被取消；被平台暫停的端點也用它重新啟用。
func (s *Service) SetActive(ctx context.Context, userID, id string, active bool) (domain.Endpoint, error) {
	return s.update(ctx, userID, id, func(ep *domain.Endpoint) error {
		ep.Status = domain.StatusDisabled
		if active {
			ep.Status = domain.StatusActive
		}
		return nil
	})
}

func (s *Service) Delete(ctx context.Context, userID, id string) error {
	version, err := s.store.DeleteEndpoint(ctx, userID, id)
	if err != nil {
		return err
	}
	s.invalidate(userID, version)
	return nil
}

func (s *Service) Get(ctx context.Context, userID, id string) (domain.Endpoint, error) {
	return s.store.Endpoint(ctx, userID, id)
}

// Redrive 把端點的 dead letter 重新放回佇列，給一個新的投遞期限。端點必須已經重新啟用。
func (s *Service) Redrive(ctx context.Context, userID, id string) (int, error) {
	ep, err := s.store.Endpoint(ctx, userID, id)
	if err != nil {
		return 0, err
	}
	if ep.Status != domain.StatusActive {
		return 0, domain.ErrNotActive
	}
	tasks, err := s.store.TakeDeadLetters(ctx, id)
	if err != nil {
		return 0, err
	}
	for _, t := range tasks {
		t.Attempt, t.Deadline = 0, time.Now().Add(s.policy.Deadline)
		s.q.Requeue(t, 0)
	}
	return len(tasks), nil
}

// Suspend 由投遞端呼叫：端點持續失敗超過期限，停用它並通知使用者。
func (s *Service) Suspend(ctx context.Context, id, reason string) error {
	userID, version, changed, err := s.store.Suspend(ctx, id, reason)
	if err != nil || !changed {
		return err
	}
	s.log.Warn("endpoint suspended", "endpoint", id, "user", userID)
	s.invalidate(userID, version)
	return nil
}

func (s *Service) update(ctx context.Context, userID, id string, fn func(*domain.Endpoint) error) (domain.Endpoint, error) {
	ep, version, err := s.store.UpdateEndpoint(ctx, userID, id, fn)
	if err != nil {
		return domain.Endpoint{}, err
	}
	s.invalidate(userID, version)
	return ep, nil
}

// invalidate 在回應使用者之前通知所有快取：API 回傳之後，新設定就生效。
func (s *Service) invalidate(userID string, version int64) {
	for _, c := range s.caches {
		c.Invalidate(userID, version)
	}
}

// verify 檢查 URL，並對它送出一個隨機挑戰，對方必須原樣回傳。
func (s *Service) verify(ctx context.Context, url string) error {
	if !s.urls.Check(ctx, url) {
		return domain.ErrInvalidURL
	}
	challenge := randomHex(16)
	body, _ := json.Marshal(map[string]string{"type": "webhook.challenge", "challenge": challenge})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return domain.ErrInvalidURL
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return domain.ErrOwnership
	}
	defer resp.Body.Close()
	var answer struct {
		Challenge string `json:"challenge"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&answer) != nil || answer.Challenge != challenge {
		return domain.ErrOwnership
	}
	return nil
}

func normalizeTypes(types []string) ([]string, error) {
	out := slices.Clone(types)
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) == 0 || len(out) > 100 {
		return nil, domain.ErrInvalidTypes
	}
	for _, t := range out {
		if len(t) > 64 || !eventType.MatchString(t) {
			return nil, domain.ErrInvalidTypes
		}
	}
	return out, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// IsClientError 回傳錯誤是否來自使用者的輸入，API 據此回 4xx 而不是 500。
func IsClientError(err error) bool {
	for _, e := range []error{domain.ErrNotFound, domain.ErrInvalidURL, domain.ErrInvalidTypes, domain.ErrOwnership, domain.ErrNotActive} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
