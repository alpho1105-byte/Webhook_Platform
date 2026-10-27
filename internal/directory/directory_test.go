package directory

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"webhook-platform/internal/domain"
	"webhook-platform/internal/store"
)

func TestStaleFillIsNotCachedAfterAnInvalidation(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now()
	ep := domain.Endpoint{ID: "ep_1", UserID: "u1", URL: "https://example.com/h", Secret: "whsec_", EventTypes: []string{"invoice.paid"},
		Status: domain.StatusActive, CreatedAt: now, UpdatedAt: now}
	version, err := st.CreateEndpoint(ctx, ep)
	if err != nil {
		t.Fatal(err)
	}

	d := New(st)
	if eps, _ := d.Subscribers(ctx, "u1", "invoice.paid"); len(eps) != 1 {
		t.Fatalf("subscribers = %v", eps)
	}
	if _, misses := d.Stats(); misses != 1 {
		t.Fatal("first lookup should miss")
	}
	d.Subscribers(ctx, "u1", "invoice.paid")
	if hits, _ := d.Stats(); hits != 1 {
		t.Fatal("second lookup should hit the cache")
	}

	// 模擬失效通知比資料庫讀取先到：這個使用者的設定已經是 version+1，手上 version 的資料不能放進快取。
	d.Invalidate("u1", version+1)
	if d.Current("u1", version) {
		t.Fatal("an older version must not be current")
	}
	d.Subscribers(ctx, "u1", "invoice.paid")
	d.Subscribers(ctx, "u1", "invoice.paid")
	if _, misses := d.Stats(); misses != 3 {
		t.Fatalf("misses = %d: the stale snapshot was cached", misses)
	}
}
