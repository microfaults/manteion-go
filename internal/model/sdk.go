package model

import (
	"errors"
	"time"
)

// SDKInstance represents a registered atropos-go SDK instance.
// Status is computed by the reaper goroutine, not persisted to the database.
type SDKInstance struct {
	ID           string    `json:"id"`
	Service      string    `json:"service"`
	Version      string    `json:"version"`
	Address      string    `json:"address"`
	RegisteredAt time.Time `json:"registered_at"`
	LastPollAt   time.Time `json:"last_poll_at"`
	Status       string    `json:"status,omitempty"` // "alive","stale","dead" — computed, not persisted
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
