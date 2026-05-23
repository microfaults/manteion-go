package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SDKRoute is one HTTP route published by a service at registration time.
// The workflow-builder catalog aggregates routes across all live SDK
// instances so the picker reflects what's actually deployed on the cluster.
//
// Path is the literal template the service registers (e.g. "/product/{id}");
// substitution happens at load-generation time. DependsOn names prerequisite
// routes as "METHOD /path" (same-service) or "service METHOD /path"
// (fully-qualified). Unresolvable deps are dropped at catalog-build time.
type SDKRoute struct {
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Description string   `json:"description,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
}

func (r *SDKRoute) Validate() error {
	if strings.TrimSpace(r.Method) == "" {
		return errors.New("sdk route: method required")
	}
	if strings.TrimSpace(r.Path) == "" {
		return errors.New("sdk route: path required")
	}
	return nil
}

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
	// Routes is the HTTP route inventory the service published at
	// registration. Optional — services may register without routes
	// (e.g. gRPC services). Aggregated across instances by the
	// workflow-builder catalog handler.
	Routes []SDKRoute `json:"routes,omitempty"`
}

func (i *SDKInstance) Validate() error {
	if i.ID == "" {
		return errors.New("sdk instance: id required")
	}
	if i.Service == "" {
		return errors.New("sdk instance: service required")
	}
	for idx, r := range i.Routes {
		if err := (&r).Validate(); err != nil {
			return fmt.Errorf("sdk instance route[%d]: %w", idx, err)
		}
	}
	return nil
}
