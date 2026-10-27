package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"webhook-platform/internal/app"
	"webhook-platform/internal/auth"
	"webhook-platform/internal/urlcheck"
)

type server struct {
	t       *testing.T
	handler http.Handler
	signer  *auth.Signer
	hook    string
}

func newServer(t *testing.T) *server {
	t.Helper()
	opts := app.DefaultOptions(filepath.Join(t.TempDir(), "w.db"))
	opts.Logger = slog.New(slog.DiscardHandler)
	opts.URLs = urlcheck.Policy{AllowLoopbackHTTP: true}
	a, err := app.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	// 客戶的接收端：正確回應驗證挑戰，只有 /stranger 不回應。
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg struct{ Challenge string }
		json.NewDecoder(r.Body).Decode(&msg)
		if r.URL.Path == "/stranger" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"challenge": msg.Challenge})
	}))
	t.Cleanup(func() {
		receiver.Close()
		cancel()
		time.Sleep(20 * time.Millisecond)
		a.Close()
	})
	signer := auth.NewSigner([]byte("secret"))
	return &server{t: t, signer: signer, hook: receiver.URL,
		handler: NewRouter(Deps{Signer: signer, Endpoints: a.Endpoints, Store: a.Store, Publisher: a, InternalToken: "internal", Log: opts.Logger})}
}

func (s *server) do(method, path, body, user string, header ...string) (int, map[string]any) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != "" {
		tok, _ := s.signer.Issue(user, time.Hour)
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	var out map[string]any
	b, _ := io.ReadAll(rec.Body)
	json.Unmarshal(b, &out)
	return rec.Code, out
}

func TestEndpointLifecycle(t *testing.T) {
	s := newServer(t)
	body := `{"endpoint":"` + s.hook + `/hooks","event_types":["invoice.paid","invoice.created"]}`
	if code, _ := s.do("POST", "/v1/webhook", body, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	code, created := s.do("POST", "/v1/webhook", body, "alice")
	if code != http.StatusCreated || !strings.HasPrefix(created["secret"].(string), "whsec_") {
		t.Fatalf("register: %d %v", code, created)
	}
	path := "/v1/webhook/" + created["id"].(string)

	if code, got := s.do("GET", path, "", "alice"); code != http.StatusOK || got["secret"] != nil || got["status"] != "active" {
		t.Fatalf("get: %d %v", code, got)
	}
	if code, _ := s.do("GET", path, "", "mallory"); code != http.StatusNotFound {
		t.Fatalf("someone else's endpoint: %d", code)
	}
	if code, got := s.do("PUT", path, `{"event_types":["issue.opened"]}`, "alice"); code != http.StatusOK || len(got["event_types"].([]any)) != 1 {
		t.Fatalf("update: %d %v", code, got)
	}
	if code, got := s.do("POST", path+"/deactivate", "", "alice"); code != http.StatusOK || got["status"] != "disabled" {
		t.Fatalf("deactivate: %d %v", code, got)
	}
	if code, _ := s.do("POST", path+"/redrive", "", "alice"); code != http.StatusUnprocessableEntity {
		t.Fatalf("redrive of a disabled endpoint: %d", code)
	}
	if code, _ := s.do("DELETE", path, "", "alice"); code != http.StatusNoContent {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := s.do("GET", path, "", "alice"); code != http.StatusNotFound {
		t.Fatalf("get after delete: %d", code)
	}
}

func TestRegistrationIsRefusedWithoutProofOfOwnershipOrForBadInput(t *testing.T) {
	s := newServer(t)
	for name, body := range map[string]string{
		"no challenge answer": `{"endpoint":"` + s.hook + `/stranger","event_types":["invoice.paid"]}`,
		"internal address":    `{"endpoint":"http://169.254.169.254/latest","event_types":["invoice.paid"]}`,
		"bad event type":      `{"endpoint":"` + s.hook + `/hooks","event_types":["Invoice Paid"]}`,
		"no event types":      `{"endpoint":"` + s.hook + `/hooks","event_types":[]}`,
	} {
		if code, out := s.do("POST", "/v1/webhook", body, "alice"); code != http.StatusUnprocessableEntity {
			t.Errorf("%s: %d %v", name, code, out)
		}
	}
}

func TestInternalEventsRequireTheInternalToken(t *testing.T) {
	s := newServer(t)
	body := `{"user_id":"alice","type":"invoice.paid","data":{"amount":100}}`
	if code, _ := s.do("POST", "/internal/events", body, ""); code != http.StatusUnauthorized {
		t.Fatalf("without token: %d", code)
	}
	code, out := s.do("POST", "/internal/events", body, "", "X-Internal-Token", "internal")
	if code != http.StatusAccepted || !strings.HasPrefix(out["id"].(string), "evt_") {
		t.Fatalf("publish: %d %v", code, out)
	}
}
