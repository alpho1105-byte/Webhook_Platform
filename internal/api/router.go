package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"webhook-platform/internal/auth"
	"webhook-platform/internal/domain"
	"webhook-platform/internal/endpoints"
	"webhook-platform/internal/store"
)

// Publisher 是內部服務發布事件的入口。
type Publisher interface {
	Publish(userID, eventType string, data json.RawMessage) domain.Event
}

type Deps struct {
	Signer    *auth.Signer
	Endpoints *endpoints.Service
	Store     *store.Store
	Publisher Publisher
	// InternalToken 保護 /internal/events；正式環境這個入口只在內網開放，或改由服務直接寫進 event bus。
	InternalToken string
	Log           *slog.Logger
	Dev           bool
}

func NewRouter(d Deps) http.Handler {
	h := &handler{d: d}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/webhook", h.user(h.register))
	mux.HandleFunc("GET /v1/webhook/{id}", h.user(h.get))
	mux.HandleFunc("PUT /v1/webhook/{id}", h.user(h.update))
	mux.HandleFunc("DELETE /v1/webhook/{id}", h.user(h.delete))
	mux.HandleFunc("POST /v1/webhook/{id}/activate", h.user(h.setActive(true)))
	mux.HandleFunc("POST /v1/webhook/{id}/deactivate", h.user(h.setActive(false)))
	mux.HandleFunc("POST /v1/webhook/{id}/redrive", h.user(h.redrive))
	mux.HandleFunc("GET /v1/notifications", h.user(h.notifications))
	mux.HandleFunc("POST /internal/events", h.publish)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if d.Dev {
		mux.HandleFunc("POST /dev/token", h.token)
	}
	return mux
}

type handler struct {
	d Deps
}

type userHandler func(w http.ResponseWriter, r *http.Request, userID string)

func (h *handler) user(next userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		userID, err := h.d.Signer.Verify(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next(w, r, userID)
	}
}

type endpointView struct {
	ID         string    `json:"id"`
	Endpoint   string    `json:"endpoint"`
	EventTypes []string  `json:"event_types"`
	Status     string    `json:"status"`
	Secret     string    `json:"secret,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func view(ep domain.Endpoint, withSecret bool) endpointView {
	v := endpointView{ID: ep.ID, Endpoint: ep.URL, EventTypes: ep.EventTypes, Status: string(ep.Status), CreatedAt: ep.CreatedAt, UpdatedAt: ep.UpdatedAt}
	if withSecret {
		v.Secret = ep.Secret
	}
	return v
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

// register 建立端點。簽章用的 secret 只在這個回應裡出現一次。
func (h *handler) register(w http.ResponseWriter, r *http.Request, userID string) {
	var req struct {
		Endpoint   string   `json:"endpoint"`
		EventTypes []string `json:"event_types"`
	}
	if !decode(w, r, &req) {
		return
	}
	ep, err := h.d.Endpoints.Register(r.Context(), userID, req.Endpoint, req.EventTypes)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, view(ep, true))
}

func (h *handler) get(w http.ResponseWriter, r *http.Request, userID string) {
	ep, err := h.d.Endpoints.Get(r.Context(), userID, r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view(ep, false))
}

func (h *handler) update(w http.ResponseWriter, r *http.Request, userID string) {
	var req struct {
		Endpoint   *string  `json:"endpoint"`
		EventTypes []string `json:"event_types"`
	}
	if !decode(w, r, &req) {
		return
	}
	ep, err := h.d.Endpoints.Update(r.Context(), userID, r.PathValue("id"), req.Endpoint, req.EventTypes)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view(ep, false))
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request, userID string) {
	if err := h.d.Endpoints.Delete(r.Context(), userID, r.PathValue("id")); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) setActive(active bool) userHandler {
	return func(w http.ResponseWriter, r *http.Request, userID string) {
		ep, err := h.d.Endpoints.SetActive(r.Context(), userID, r.PathValue("id"), active)
		if err != nil {
			h.fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view(ep, false))
	}
}

func (h *handler) redrive(w http.ResponseWriter, r *http.Request, userID string) {
	n, err := h.d.Endpoints.Redrive(r.Context(), userID, r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"requeued": n})
}

func (h *handler) notifications(w http.ResponseWriter, r *http.Request, userID string) {
	list, err := h.d.Store.Notifications(r.Context(), userID)
	if err != nil {
		h.fail(w, err)
		return
	}
	if list == nil {
		list = []store.Notification{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"notifications": list})
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Token")), []byte(h.d.InternalToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "internal token required")
		return
	}
	var req struct {
		UserID string          `json:"user_id"`
		Type   string          `json:"type"`
		Data   json.RawMessage `json:"data"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.UserID == "" || req.Type == "" || len(req.Data) == 0 {
		writeError(w, http.StatusBadRequest, "user_id, type and data are required")
		return
	}
	e := h.d.Publisher.Publish(req.UserID, req.Type, req.Data)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": e.ID})
}

func (h *handler) token(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.UserID == "" || len(req.UserID) > 64 {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}
	token, _ := h.d.Signer.Issue(req.UserID, 24*time.Hour)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (h *handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case endpoints.IsClientError(err):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		h.d.Log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
