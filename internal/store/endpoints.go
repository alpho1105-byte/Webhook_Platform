package store

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"

	"webhook-platform/internal/domain"
)

// Snapshot 是某位使用者在某個版本時的完整端點設定，快取以它為單位。
type Snapshot struct {
	Version   int64
	Endpoints map[string]domain.Endpoint
}

// bumpVersion 在同一個交易裡把使用者的設定版本加一。
func bumpVersion(ctx context.Context, tx *sql.Tx, userID string) (int64, error) {
	var v int64
	err := tx.QueryRowContext(ctx, `INSERT INTO users (user_id, version) VALUES (?, 1)
		ON CONFLICT (user_id) DO UPDATE SET version = version + 1 RETURNING version`, userID).Scan(&v)
	return v, err
}

func setTypes(ctx context.Context, tx *sql.Tx, ep domain.Endpoint) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM subscriptions WHERE endpoint_id = ?`, ep.ID); err != nil {
		return err
	}
	for _, t := range ep.EventTypes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO subscriptions (user_id, event_type, endpoint_id) VALUES (?, ?, ?)`,
			ep.UserID, t, ep.ID); err != nil {
			return err
		}
	}
	return nil
}

// CreateEndpoint 寫入端點與它訂閱的事件類型，回傳使用者新的設定版本。
func (s *Store) CreateEndpoint(ctx context.Context, ep domain.Endpoint) (version int64, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO endpoints (endpoint_id, user_id, url, secret, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, ep.ID, ep.UserID, ep.URL, ep.Secret, ep.Status, ep.CreatedAt.UnixMilli(), ep.UpdatedAt.UnixMilli()); err != nil {
			return err
		}
		if err := setTypes(ctx, tx, ep); err != nil {
			return err
		}
		version, err = bumpVersion(ctx, tx, ep.UserID)
		return err
	})
	return version, err
}

// UpdateEndpoint 以 fn 修改端點後寫回。fn 回傳錯誤時不做任何修改。
func (s *Store) UpdateEndpoint(ctx context.Context, userID, id string, fn func(*domain.Endpoint) error) (ep domain.Endpoint, version int64, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		if ep, err = loadEndpoint(ctx, tx, userID, id); err != nil {
			return err
		}
		if err := fn(&ep); err != nil {
			return err
		}
		ep.UpdatedAt = time.Now()
		if _, err := tx.ExecContext(ctx, `UPDATE endpoints SET url = ?, status = ?, updated_at = ? WHERE endpoint_id = ?`,
			ep.URL, ep.Status, ep.UpdatedAt.UnixMilli(), ep.ID); err != nil {
			return err
		}
		if err := setTypes(ctx, tx, ep); err != nil {
			return err
		}
		version, err = bumpVersion(ctx, tx, userID)
		return err
	})
	return ep, version, err
}

func (s *Store) DeleteEndpoint(ctx context.Context, userID, id string) (version int64, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM endpoints WHERE endpoint_id = ? AND user_id = ?`, id, userID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return domain.ErrNotFound
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM subscriptions WHERE endpoint_id = ?`, id); err != nil {
			return err
		}
		version, err = bumpVersion(ctx, tx, userID)
		return err
	})
	return version, err
}

// Suspend 停用持續投遞失敗的端點，並在同一個交易裡留下給使用者的通知。
// 已經不是 active 的端點不會重複停用、也不會重複通知。
func (s *Store) Suspend(ctx context.Context, id, message string) (userID string, version int64, changed bool, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE endpoints SET status = ?, updated_at = ? WHERE endpoint_id = ? AND status = ?`,
			domain.StatusSuspended, time.Now().UnixMilli(), id, domain.StatusActive)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		if err := tx.QueryRowContext(ctx, `SELECT user_id FROM endpoints WHERE endpoint_id = ?`, id).Scan(&userID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO notifications (user_id, endpoint_id, message, created_at) VALUES (?, ?, ?, ?)`,
			userID, id, message, time.Now().UnixMilli()); err != nil {
			return err
		}
		changed = true
		version, err = bumpVersion(ctx, tx, userID)
		return err
	})
	return userID, version, changed, err
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func loadEndpoint(ctx context.Context, q querier, userID, id string) (domain.Endpoint, error) {
	var ep domain.Endpoint
	var created, updated int64
	err := q.QueryRowContext(ctx, `SELECT endpoint_id, user_id, url, secret, status, created_at, updated_at
		FROM endpoints WHERE endpoint_id = ? AND user_id = ?`, id, userID).
		Scan(&ep.ID, &ep.UserID, &ep.URL, &ep.Secret, &ep.Status, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return ep, domain.ErrNotFound
	}
	if err != nil {
		return ep, err
	}
	ep.CreatedAt, ep.UpdatedAt = time.UnixMilli(created), time.UnixMilli(updated)
	rows, err := q.QueryContext(ctx, `SELECT event_type FROM subscriptions WHERE endpoint_id = ? ORDER BY event_type`, id)
	if err != nil {
		return ep, err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return ep, err
		}
		ep.EventTypes = append(ep.EventTypes, t)
	}
	return ep, rows.Err()
}

func (s *Store) Endpoint(ctx context.Context, userID, id string) (domain.Endpoint, error) {
	return loadEndpoint(ctx, s.r, userID, id)
}

// Snapshot 在同一個讀取交易裡讀出版本與所有端點，兩者一定一致。
func (s *Store) Snapshot(ctx context.Context, userID string) (Snapshot, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()
	snap := Snapshot{Endpoints: make(map[string]domain.Endpoint)}
	if err := tx.QueryRowContext(ctx, `SELECT version FROM users WHERE user_id = ?`, userID).Scan(&snap.Version); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT endpoint_id, url, secret, status FROM endpoints WHERE user_id = ?`, userID)
	if err != nil {
		return Snapshot{}, err
	}
	for rows.Next() {
		ep := domain.Endpoint{UserID: userID}
		if err := rows.Scan(&ep.ID, &ep.URL, &ep.Secret, &ep.Status); err != nil {
			rows.Close()
			return Snapshot{}, err
		}
		snap.Endpoints[ep.ID] = ep
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT event_type, endpoint_id FROM subscriptions WHERE user_id = ?`, userID)
	if err != nil {
		return Snapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var t, id string
		if err := rows.Scan(&t, &id); err != nil {
			return Snapshot{}, err
		}
		ep := snap.Endpoints[id]
		ep.EventTypes = append(ep.EventTypes, t)
		snap.Endpoints[id] = ep
	}
	for id, ep := range snap.Endpoints {
		slices.Sort(ep.EventTypes)
		snap.Endpoints[id] = ep
	}
	return snap, rows.Err()
}
