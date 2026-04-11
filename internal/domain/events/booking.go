package events

import "time"

type UserBooking struct {
	ToUser    string    `json:"to_user"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Location  string    `json:"location"`
	Type      string    `json:"type"`
	Name      string    `json:"name"`
}

type AdminCancel struct {
	ToUser    string    `json:"to_user"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Location  string    `json:"location"`
	Type      string    `json:"type"`
	Name      string    `json:"name"`
}

type UserCancel struct {
	ToUser    string    `json:"to_user"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Location  string    `json:"location"`
	Type      string    `json:"type"`
	Name      string    `json:"name"`
}
