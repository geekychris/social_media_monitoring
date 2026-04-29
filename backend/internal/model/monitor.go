package model

import "time"

type Monitor struct {
	ID        string    `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Query     string    `json:"query" db:"query"`
	Platforms []string  `json:"platforms" db:"platforms"`
	Active    bool      `json:"active" db:"active"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

type Alert struct {
	ID         string    `json:"id" db:"id"`
	MonitorID  string    `json:"monitor_id" db:"monitor_id"`
	Type       string    `json:"type" db:"type"` // threshold, spike, narrative, influencer
	Severity   string    `json:"severity" db:"severity"` // low, medium, high, critical
	Title      string    `json:"title" db:"title"`
	Message    string    `json:"message" db:"message"`
	Fired      bool      `json:"fired" db:"fired"`
	FiredAt    time.Time `json:"fired_at,omitempty" db:"fired_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty" db:"resolved_at"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}
