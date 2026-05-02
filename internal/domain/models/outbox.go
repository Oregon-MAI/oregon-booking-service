package models

import (
	"encoding/json"
	"time"
)

type OutboxMessage struct {
	OutboxID    string          `json:"outbox_id"`
	BookingID   string          `json:"booking_id"`
	Topic       string          `json:"topic"`
	Key         string          `json:"key"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	SentAt      *time.Time      `json:"sent_at"`
	Attempts    int             `json:"attempts"`
	LastError   string          `json:"last_error"`
}
