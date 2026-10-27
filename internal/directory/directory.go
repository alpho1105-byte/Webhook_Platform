// Package directory 快取端點設定。每個事件都要查「這位使用者的這個事件類型要送到哪些端點」，
// 讀取遠多於寫入，所以放在記憶體；設定改變時由管理服務發出失效通知。
package directory

import (
	"context"
	"sync"
	"sync/atomic"

	"webhook-platform/internal/domain"
	"webhook-platform/internal/store"
)

// Directory 以使用者為單位快取完整的端點設定（store.Snapshot）。
//
// 只在寫入後刪除快取並不夠：另一個請求可能在寫入「之前」讀到舊設定，卻在刪除「之後」才放回快取，
// 舊設定就此留在快取裡。所以每份設定帶著版本號，失效通知也帶著版本號，
// 比最新失效版本舊的設定一律不放進快取。
type Directory struct {
	store *store.Store

	mu      sync.RWMutex
	entries map[string]store.Snapshot
	latest  map[string]int64

	hits, misses atomic.Int64
}

func New(st *store.Store) *Directory {
	return &Directory{store: st, entries: make(map[string]store.Snapshot), latest: make(map[string]int64)}
}

func (d *Directory) snapshot(ctx context.Context, userID string) (store.Snapshot, error) {
	d.mu.RLock()
	snap, ok := d.entries[userID]
	d.mu.RUnlock()
	if ok {
		d.hits.Add(1)
		return snap, nil
	}
	d.misses.Add(1)
	snap, err := d.store.Snapshot(ctx, userID)
	if err != nil {
		return snap, err
	}
	d.mu.Lock()
	if snap.Version >= d.latest[userID] {
		d.entries[userID] = snap
	}
	d.mu.Unlock()
	return snap, nil
}

// Subscribers 回傳使用者訂閱了 eventType 的端點。被平台暫停的端點也算：
// 暫停期間的事件照樣產生投遞並直接進 dead letter，使用者修好端點後可以一次重送。
func (d *Directory) Subscribers(ctx context.Context, userID, eventType string) ([]domain.Endpoint, error) {
	snap, err := d.snapshot(ctx, userID)
	if err != nil {
		return nil, err
	}
	var out []domain.Endpoint
	for _, ep := range snap.Endpoints {
		if ep.Status == domain.StatusDisabled {
			continue
		}
		for _, t := range ep.EventTypes {
			if t == eventType {
				out = append(out, ep)
				break
			}
		}
	}
	return out, nil
}

// Endpoint 回傳端點目前的設定與它所屬設定的版本。
func (d *Directory) Endpoint(ctx context.Context, userID, id string) (domain.Endpoint, int64, bool, error) {
	snap, err := d.snapshot(ctx, userID)
	if err != nil {
		return domain.Endpoint{}, 0, false, err
	}
	ep, ok := snap.Endpoints[id]
	return ep, snap.Version, ok, nil
}

// Invalidate 處理失效通知：使用者的設定已經更新到 version。
func (d *Directory) Invalidate(userID string, version int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if version > d.latest[userID] {
		d.latest[userID] = version
	}
	if snap, ok := d.entries[userID]; ok && snap.Version < version {
		delete(d.entries, userID)
	}
}

// Current 回傳 version 是否仍是最新：送出前再確認一次，期間若有失效通知就重新查。
func (d *Directory) Current(userID string, version int64) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return version >= d.latest[userID]
}

func (d *Directory) Stats() (hits, misses int64) {
	return d.hits.Load(), d.misses.Load()
}
