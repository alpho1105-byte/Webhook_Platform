package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("endpoint not found")
	ErrInvalidURL   = errors.New("endpoint URL is not allowed")
	ErrInvalidTypes = errors.New("event_types must contain 1 to 100 valid event types")
	ErrOwnership    = errors.New("endpoint did not answer the verification challenge")
	ErrNotActive    = errors.New("endpoint is not active")
)

// Event 是內部服務產生的 domain event，例如 invoice.paid。
type Event struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

type Status string

const (
	StatusActive   Status = "active"
	StatusDisabled Status = "disabled"
	// StatusSuspended 是平台在投遞持續失敗超過 SLA 後停用的端點，使用者修好後可以重新啟用並重送。
	StatusSuspended Status = "suspended"
)

type Endpoint struct {
	ID         string
	UserID     string
	URL        string
	Secret     string
	EventTypes []string
	Status     Status
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Delivery 是「把一個事件送到一個端點」的工作。ID 由事件與端點決定，
// 同一個事件被重複處理時產生的 Delivery 一模一樣，佇列可以據此去重。
type Delivery struct {
	ID         string `json:"id"`
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	EndpointID string `json:"endpoint_id"`
	UserID     string `json:"user_id"`
	// Payload 在 ingestion 時就固定下來，每次重試送出的內容完全相同。
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	// Deadline 之前都會持續重試，過了就進 dead letter queue。
	Deadline time.Time `json:"deadline"`
	Attempt  int       `json:"attempt"`
}

func DeliveryID(eventID, endpointID string) string {
	return eventID + ":" + endpointID
}
