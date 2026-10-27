package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
-- version 在使用者的任何端點設定改變時加一，快取用它判斷手上的資料是不是過期了。
CREATE TABLE IF NOT EXISTS users (
	user_id TEXT PRIMARY KEY,
	version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
	endpoint_id TEXT PRIMARY KEY,
	user_id     TEXT NOT NULL,
	url         TEXT NOT NULL,
	secret      TEXT NOT NULL,
	status      TEXT NOT NULL,
	created_at  INTEGER NOT NULL,
	updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS endpoints_by_user ON endpoints (user_id);

-- 依 (user_id, event_type) 查出要送到哪些端點：ingestion 每個事件都要查一次。
CREATE TABLE IF NOT EXISTS subscriptions (
	user_id     TEXT NOT NULL,
	event_type  TEXT NOT NULL,
	endpoint_id TEXT NOT NULL,
	PRIMARY KEY (user_id, event_type, endpoint_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS subscriptions_by_endpoint ON subscriptions (endpoint_id);

-- 快速重試用完之後的長期重試：scheduler 定期取出到期的再放回佇列。
-- attempt 用來辨識同一個投遞的不同次重試：scheduler 只刪除自己放回佇列的那一次。
CREATE TABLE IF NOT EXISTS retries (
	delivery_id TEXT PRIMARY KEY,
	endpoint_id TEXT NOT NULL,
	attempt     INTEGER NOT NULL,
	next_at     INTEGER NOT NULL,
	task        BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS retries_due ON retries (next_at);
CREATE INDEX IF NOT EXISTS retries_by_endpoint ON retries (endpoint_id);

CREATE TABLE IF NOT EXISTS dead_letters (
	delivery_id TEXT PRIMARY KEY,
	endpoint_id TEXT NOT NULL,
	reason      TEXT NOT NULL,
	failed_at   INTEGER NOT NULL,
	task        BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS dead_letters_by_endpoint ON dead_letters (endpoint_id);

CREATE TABLE IF NOT EXISTS notifications (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id     TEXT NOT NULL,
	endpoint_id TEXT NOT NULL,
	message     TEXT NOT NULL,
	created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS notifications_by_user ON notifications (user_id);
`

// Store 保存端點設定、長期重試、dead letter 與通知。
// 寫入走單一連線（SQLite 同時只允許一個 writer），讀取另開連線池，WAL 模式下讀不會擋寫。
type Store struct {
	w *sql.DB
	r *sql.DB
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	base := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)"
	w, err := open(base, 1)
	if err != nil {
		return nil, err
	}
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	r, err := open(base+"&_pragma=query_only(1)", 8)
	if err != nil {
		w.Close()
		return nil, err
	}
	return &Store{w: w, r: r}, nil
}

func open(dsn string, conns int) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, nil
}

func (s *Store) Close() error {
	s.r.Close()
	return s.w.Close()
}

func (s *Store) write(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
