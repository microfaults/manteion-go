package model

import (
	"errors"
	"time"
)

// SDKInstance represents a registered atropos-go SDK instance.
// Status is computed at read time from last_poll_at vs poll_interval_ms.
type SDKInstance struct {
	ID             string    `json:"id"`
	Service        string    `json:"service"`
	Version        string    `json:"version"`
	Address        string    `json:"address"`
	PollIntervalMs int64     `json:"poll_interval_ms"` // SDK's configured poll cadence
	RegisteredAt   time.Time `json:"registered_at"`
	LastPollAt     time.Time `json:"last_poll_at"`
	Status         string    `json:"status"` // "alive","stale","dead" — computed from poll staleness
}

func (i *SDKInstance) Validate() error {
	if i.ID == "" {
		return errors.New("sdk instance: id required")
	}
	if i.Service == "" {
		return errors.New("sdk instance: service required")
	}
	return nil
}
