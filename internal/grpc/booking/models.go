package booking

import (
	"errors"
	"time"
)

var (
	ErrUnauthenticated  = errors.New("unauthenticated")
	ErrPermissionDenied = errors.New("permission denied")
)

type CreateBookingRequest struct {
	ResourceID string
	UserID     string
	StartsAt   time.Time
	EndsAt     time.Time
}

type ListBookingsByResourceRequest struct {
	ResourceID string
	From       time.Time
	To         time.Time
}
