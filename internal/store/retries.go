package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"webhook-platform/internal/domain"
)

type Retry struct {
	Task   domain.Delivery
	NextAt time.Time
}

// ScheduleRetries 把快速重試用完的投遞存進長期重試表。同一個投遞只會有一列。
func (s *Store) ScheduleRetries(ctx context.Context, items []Retry) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		for _, it := range items {
			blob, err := json.Marshal(it.Task)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO retries (delivery_id, endpoint_id, attempt, next_at, task) VALUES (?, ?, ?, ?, ?)
				ON CONFLICT (delivery_id) DO UPDATE SET attempt = excluded.attempt, next_at = excluded.next_at, task = excluded.task`,
				it.Task.ID, it.Task.EndpointID, it.Task.Attempt, it.NextAt.UnixMilli(), blob); err != nil {
				return err
			}
		}
		return nil
	})
}

// DueRetries 回傳到期的長期重試，最早到期的在前。
func (s *Store) DueRetries(ctx context.Context, now time.Time, limit int) ([]domain.Delivery, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT task FROM retries WHERE next_at <= ? ORDER BY next_at LIMIT ?`, now.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	return scanTasks(rows)
}

// DeleteRetries 刪除已經放回佇列的重試。只刪除指定的那一次嘗試：放回佇列之後，
// worker 可能已經又失敗一次、寫入了下一次的重試，那一列不能被刪掉。
func (s *Store) DeleteRetries(ctx context.Context, tasks []domain.Delivery) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		for _, t := range tasks {
			if _, err := tx.ExecContext(ctx, `DELETE FROM retries WHERE delivery_id = ? AND attempt = ?`, t.ID, t.Attempt); err != nil {
				return err
			}
		}
		return nil
	})
}

// Expedite 把某個端點所有排定的重試提前到現在：端點剛恢復，不必等各自的退避時間到了才送。
func (s *Store) Expedite(ctx context.Context, endpointID string, now time.Time) (int64, error) {
	var n int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE retries SET next_at = ? WHERE endpoint_id = ? AND next_at > ?`,
			now.UnixMilli(), endpointID, now.UnixMilli())
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n, err
}

type DeadLetter struct {
	Task   domain.Delivery
	Reason string
}

// AddDeadLetters 把放棄的投遞存進 dead letter queue，同時移除它可能還在的長期重試。
func (s *Store) AddDeadLetters(ctx context.Context, items []DeadLetter) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		for _, it := range items {
			blob, err := json.Marshal(it.Task)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO dead_letters (delivery_id, endpoint_id, reason, failed_at, task) VALUES (?, ?, ?, ?, ?)
				ON CONFLICT (delivery_id) DO UPDATE SET reason = excluded.reason, failed_at = excluded.failed_at`,
				it.Task.ID, it.Task.EndpointID, it.Reason, time.Now().UnixMilli(), blob); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM retries WHERE delivery_id = ?`, it.Task.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeadLetterIfSuspended 只在端點仍是暫停狀態時才把投遞放進 dead letter，回傳是否放入。
// 和「重新啟用」在資料庫裡排序：使用者剛重新啟用並重送，晚一步的 worker 不會再把投遞丟進已經清空的 dead letter。
func (s *Store) DeadLetterIfSuspended(ctx context.Context, t domain.Delivery) (bool, error) {
	blob, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	var added bool
	err = s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO dead_letters (delivery_id, endpoint_id, reason, failed_at, task)
			SELECT ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM endpoints WHERE endpoint_id = ? AND status = ?)
			ON CONFLICT (delivery_id) DO NOTHING`,
			t.ID, t.EndpointID, "endpoint suspended", time.Now().UnixMilli(), blob, t.EndpointID, domain.StatusSuspended)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		added = n > 0
		return err
	})
	return added, err
}

// TakeDeadLetters 取出並刪除某個端點的所有 dead letter，用於重送。
func (s *Store) TakeDeadLetters(ctx context.Context, endpointID string) ([]domain.Delivery, error) {
	var out []domain.Delivery
	err := s.write(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT task FROM dead_letters WHERE endpoint_id = ? ORDER BY failed_at`, endpointID)
		if err != nil {
			return err
		}
		if out, err = scanTasks(rows); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM dead_letters WHERE endpoint_id = ?`, endpointID)
		return err
	})
	return out, err
}

// DeadLetterReasons 回傳每個 dead letter 的原因，給驗證與除錯使用。
func (s *Store) DeadLetterReasons(ctx context.Context) (map[string]string, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT delivery_id, reason FROM dead_letters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			return nil, err
		}
		out[id] = reason
	}
	return out, rows.Err()
}

func (s *Store) PendingRetries(ctx context.Context) (int, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM retries`).Scan(&n)
	return n, err
}

type Notification struct {
	EndpointID string    `json:"endpoint_id"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s *Store) Notifications(ctx context.Context, userID string) ([]Notification, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT endpoint_id, message, created_at FROM notifications WHERE user_id = ? ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		var created int64
		if err := rows.Scan(&n.EndpointID, &n.Message, &created); err != nil {
			return nil, err
		}
		n.CreatedAt = time.UnixMilli(created)
		out = append(out, n)
	}
	return out, rows.Err()
}

func scanTasks(rows *sql.Rows) ([]domain.Delivery, error) {
	defer rows.Close()
	var out []domain.Delivery
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		var d domain.Delivery
		if err := json.Unmarshal(blob, &d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
