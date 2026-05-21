package model

import (
	"encoding/json"
	"errors"
	"time"
)

// Workflow is a manteion-owned DSL v2 workflow definition.
//
// Storage split: manteion holds the *definition* (this struct).
// Zeus holds *execution* state (runs, attacks, validation results) —
// the UI fans out two parallel queries to compose the detail view.
// When a workflow is started, manteion inlines this struct into the
// proxied call so zeus never has to call back for the spec.
//
// Steps/thresholds are opaque JSON to manteion: zeus parses and
// validates at run start. Manteion is responsible for storing and
// returning whatever bytes the UI/API hands it.
type Workflow struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description,omitempty"`
	Targets           []string        `json:"targets"`
	EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
	Steps             json.RawMessage `json:"steps"`
	Thresholds        json.RawMessage `json:"thresholds,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

func (w *Workflow) Validate() error {
	if w.ID == "" {
		return errors.New("workflow: id required")
	}
	if w.Name == "" {
		return errors.New("workflow: name required")
	}
	if len(w.Targets) == 0 {
		return errors.New("workflow: at least one target required")
	}
	if len(w.Steps) == 0 || string(w.Steps) == "null" {
		return errors.New("workflow: steps required")
	}
	return nil
}
