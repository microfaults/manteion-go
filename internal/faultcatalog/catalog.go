// Package faultcatalog is manteion's single source of truth for which faults
// the platform supports and how they are configured. The per-(category,
// fault_type) parameter schemas come from atropos-go/faultparams — the same
// structs the SDK decoders consume — so the control plane validates exactly
// the contract the data plane executes.
//
// The catalog replaces the old scattered validFaultTypes maps: domain models
// (FaultSpec, FaultConfig) delegate vocabulary, params, and envelope checks
// here, and the UI renders fault forms from GET /api/v1/faults/catalog.
package faultcatalog

import (
	"encoding/json"
	"fmt"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
)

// Envelope is the catalog's view of a fault's placement: the host knob plus
// the network envelope fields. It is deliberately not the model type to keep
// this package free of model imports (model imports the catalog).
type Envelope struct {
	Host       string  // "" | "proxy" | "inline" | "process"
	HasNetwork bool    // whether a network object was supplied at all
	Target     string  // network only: logical upstream name
	Direction  string  // network only: "" | "upstream" | "downstream"
	Scope      float64 // network only: fraction of connections, 0 = all
}

// Supported reports whether (category, faultType) names a fault the platform
// can execute.
func Supported(category, faultType string) bool {
	_, ok := faultparams.Lookup(category, faultType)
	return ok
}

// ValidateFault checks the full fault definition surface shared by fault
// specs and long-running fault configs: vocabulary, typed params (strict —
// unknown fields rejected), host/category coupling, and the network
// envelope rules.
func ValidateFault(category, faultType string, params json.RawMessage, env Envelope) error {
	if !Supported(category, faultType) {
		return fmt.Errorf("unsupported fault %s/%s", category, faultType)
	}
	if err := faultparams.ValidateRaw(category, faultType, params); err != nil {
		return err
	}

	// Host vocabulary + category coupling.
	switch env.Host {
	case "":
		// optional; atropos defaults network faults to proxy
	case "proxy", "inline":
		if category != "network" {
			return fmt.Errorf("host=%q only valid for network category, got %q", env.Host, category)
		}
	case "process":
		if category == "network" {
			return fmt.Errorf("host=process invalid for network category")
		}
	default:
		return fmt.Errorf("invalid host %q", env.Host)
	}

	// Network envelope: required for network faults (proxy host needs a
	// resolvable target), forbidden elsewhere.
	if category == "network" {
		if !env.HasNetwork {
			return fmt.Errorf("network fault %s requires a network envelope", faultType)
		}
		if (env.Host == "" || env.Host == "proxy") && env.Target == "" {
			return fmt.Errorf("network fault %s with host=proxy requires network.target", faultType)
		}
		switch env.Direction {
		case "", "upstream", "downstream":
		default:
			return fmt.Errorf("invalid network direction %q", env.Direction)
		}
		if env.Scope < 0 || env.Scope > 1 {
			return fmt.Errorf("network scope %.2f outside [0,1]", env.Scope)
		}
	} else if env.HasNetwork {
		return fmt.Errorf("network envelope only valid for network category, got %q", category)
	}

	return nil
}

// FieldSpec describes one params field for UI form rendering. Types are
// "duration" (Go duration string), "int", "float", "bool", "string", "enum".
type FieldSpec struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Required    bool     `json:"required"`
	Default     any      `json:"default,omitempty"`
	Enum        []string `json:"enum,omitempty"`
	Description string   `json:"description,omitempty"`
}

// Entry is one catalogue row served to the UI.
type Entry struct {
	Category        string      `json:"category"`
	FaultType       string      `json:"fault_type"`
	Description     string      `json:"description"`
	NetworkRequired bool        `json:"network_required"`
	Params          []FieldSpec `json:"params"`
}

// fieldSpecs is the hand-authored UI metadata per fault type. The fields and
// json names mirror faultparams (the catalog_test asserts every entry decodes
// against its faultparams struct); defaults document SDK-side behaviour.
var fieldSpecs = map[string][]FieldSpec{
	"inline/latency": {
		{Name: "delay", Type: "duration", Required: true, Description: "base delay per request"},
		{Name: "jitter", Type: "duration", Description: "uniform jitter added to delay"},
	},
	"inline/error": {
		{Name: "status_code", Type: "int", Default: 500, Description: "HTTP status (100–599)"},
		{Name: "message", Type: "string", Default: "injected fault", Description: "response body"},
	},
	"inline/hang": {
		{Name: "duration", Type: "duration", Required: true, Description: "how long to block the request"},
	},
	"network/latency": {
		{Name: "delay", Type: "duration", Required: true, Description: "base delay on piped bytes"},
		{Name: "jitter", Type: "duration", Description: "uniform jitter added to delay"},
	},
	"network/retransmit_delay": {
		{Name: "rate", Type: "float", Required: true, Description: "fraction of reads stalled (0–1)"},
		{Name: "delay", Type: "duration", Default: "200ms", Description: "per-stall delay"},
		{Name: "reset_threshold", Type: "int", Default: 0, Description: "stalls before RST; 0 = never"},
	},
	"network/blackhole": {},
	"network/drip": {
		{Name: "chunk_size", Type: "int", Default: 1, Description: "bytes per chunk"},
		{Name: "interval", Type: "duration", Description: "pause between chunks"},
	},
	"network/rst": {
		{Name: "after_bytes", Type: "int", Default: 0, Description: "forwarded bytes before RST"},
		{Name: "after_duration", Type: "duration", Description: "elapsed time before RST"},
	},
	"network/throttle": {
		{Name: "bytes_per_sec", Type: "int", Required: true, Description: "bandwidth cap"},
	},
	"resource/cpu": {
		{Name: "target_load", Type: "float", Required: true, Description: "incremental CPU fraction (0–1]"},
		{Name: "window", Type: "duration", Default: "100ms", Description: "duty-cycle period"},
	},
	"resource/memory": {
		{Name: "target_load", Type: "float", Required: true, Description: "fraction of available memory (0–1]"},
		{Name: "chunk_size", Type: "int", Default: 1 << 20, Description: "bytes per allocated chunk"},
		{Name: "thrashing", Type: "bool", Default: false, Description: "page-thrash mode"},
		{Name: "thrash_workers", Type: "int", Default: 2, Description: "thrash goroutines"},
	},
	"resource/disk": {
		{Name: "write_rate", Type: "int", Default: 10 << 20, Description: "bytes/sec sustained writes"},
		{Name: "max_disk_usage", Type: "int", Default: 512 << 20, Description: "byte cap on scratch usage"},
		{Name: "chunk_size", Type: "int", Default: 1 << 20, Description: "bytes per write"},
		{Name: "path", Type: "string", Description: "scratch dir; empty = OS temp"},
	},
	"resource/io": {
		{Name: "read_rate", Type: "int", Default: 100 << 10, Description: "bytes/sec"},
		{Name: "file_size", Type: "int", Default: 4096, Description: "bytes per file"},
		{Name: "file_count", Type: "int", Default: 256, Description: "files in the working set"},
		{Name: "workers", Type: "int", Default: 4, Description: "concurrent I/O goroutines"},
		{Name: "path", Type: "string", Description: "scratch dir; empty = OS temp"},
		{Name: "mode", Type: "enum", Default: "read", Enum: []string{"read", "write", "read_write"}, Description: "I/O direction"},
	},
}

// Entries returns the full catalogue in faultparams order, ready to serve to
// the UI.
func Entries() []Entry {
	specs := faultparams.All()
	out := make([]Entry, 0, len(specs))
	for _, s := range specs {
		out = append(out, Entry{
			Category:        s.Category,
			FaultType:       s.Type,
			Description:     s.Description,
			NetworkRequired: s.Category == "network",
			Params:          fieldSpecs[s.Category+"/"+s.Type],
		})
	}
	return out
}
