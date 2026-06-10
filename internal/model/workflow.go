package model

import (
	"encoding/json"
	"errors"
	"time"
)

// Workflow is a manteion-owned DSL v2 workflow definition.
//
// Ownership split: manteion is the durable system of record for the
// DEFINITION; zeus is the validation + execution runtime. Create/update
// validate the document against zeus's stateless validate endpoint, and
// starting a run MATERIALIZES the definition into zeus (register with
// overwrite) before the run is triggered — zeus's in-memory store is just
// a cache of this table.
//
// DSL is the complete zeus DSL v2 document (name, version, targets,
// estimated_rps_per_vu, root, thresholds, data_schema, default_delay,
// base_url, ...). It is stored whole and opaque: the epoch-1 schema split
// fields into columns and silently dropped the ones it didn't model
// (base_url/data_schema/default_delay). Name/version/description are
// lifted into columns for uniqueness and listing only; the document is
// the contract.
type Workflow struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Version     string          `json:"version"`
	Description string          `json:"description,omitempty"`
	DSL         json.RawMessage `json:"dsl"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (w *Workflow) Validate() error {
	if w.ID == "" {
		return errors.New("workflow: id required")
	}
	if w.Name == "" {
		return errors.New("workflow: name required")
	}
	if len(w.DSL) == 0 || string(w.DSL) == "null" {
		return errors.New("workflow: dsl document required")
	}
	return nil
}

// WorkflowDSLMeta is the presentation view extracted from the DSL document
// for list rows and cards — manteion does not interpret these fields beyond
// surfacing them (zeus owns DSL semantics).
type WorkflowDSLMeta struct {
	Targets           []string        `json:"targets"`
	EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
	Root              json.RawMessage `json:"root"`
}

// DSLMeta best-effort parses the presentation fields out of the document.
// Unparseable documents yield the zero meta (zeus validation prevents them
// from being stored in the first place).
func (w *Workflow) DSLMeta() WorkflowDSLMeta {
	var m WorkflowDSLMeta
	_ = json.Unmarshal(w.DSL, &m)
	if m.Targets == nil {
		m.Targets = []string{}
	}
	return m
}
