package models

import "time"

type Booking struct {
	BookingID        string        `json:"booking_id"`
	ResourceID       string        `json:"resource_id"`
	UserID           string        `json:"user_id"`
	ResourceName     string        `json:"resource_name"`
	ResourceType     string        `json:"resource_type"`
	ResourceLocation string        `json:"resource_location"`
	StartsAt         time.Time     `json:"start_time"`
	EndsAt           time.Time     `json:"end_time"`
	Status           BookingStatus `json:"status"`
	CancelReason string    `json:"cancel_reason"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type BookingStatus string

const (
	BookingStatusConfirmed  BookingStatus = "CONFIRMED"
	BookingStatusCanceled   BookingStatus = "CANCELED"
)