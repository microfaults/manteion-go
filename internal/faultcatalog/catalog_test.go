package faultcatalog

import (
	"encoding/json"
	"fmt"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
)

// TestEntriesCoverFaultparams pins the UI metadata to the faultparams
// registry: every supported fault type has a fieldSpecs entry (blackhole's
// is intentionally empty but must exist as a key), and there are no
// orphaned keys for types faultparams doesn't know.
func TestEntriesCoverFaultparams(t *testing.T) {
	known := map[string]bool{}
	for _, s := range faultparams.All() {
		key := s.Category + "/" + s.Type
		known[key] = true
		if _, ok := fieldSpecs[key]; !ok {
			t.Errorf("fieldSpecs missing entry for %s", key)
		}
	}
	for key := range fieldSpecs {
		if !known[key] {
			t.Errorf("fieldSpecs has orphaned key %s (not in faultparams)", key)
		}
	}
	if got, want := len(Entries()), len(faultparams.All()); got != want {
		t.Errorf("Entries() = %d rows, want %d", got, want)
	}
}

// TestFieldSpecsDecodeAgainstFaultparams builds a params object from each
// entry's field names and asserts the corresponding faultparams struct
// accepts it under DisallowUnknownFields — i.e. the UI metadata cannot name
// a field the SDK contract doesn't have.
func TestFieldSpecsDecodeAgainstFaultparams(t *testing.T) {
	sample := func(f FieldSpec) any {
		switch f.Type {
		case "duration":
			return "100ms"
		case "int":
			return 200 // in-range for every int field incl. status_code (100–599)
		case "float":
			return 0.5
		case "bool":
			return true
		case "enum":
			return f.Enum[0]
		default:
			return "x"
		}
	}
	for key, fields := range fieldSpecs {
		obj := map[string]any{}
		for _, f := range fields {
			obj[f.Name] = sample(f)
		}
		raw, _ := json.Marshal(obj)
		var category, faultType string
		fmt.Sscanf(key, "%s", &category) // placeholder; split below
		for i := range key {
			if key[i] == '/' {
				category, faultType = key[:i], key[i+1:]
				break
			}
		}
		if err := faultparams.ValidateRaw(category, faultType, raw); err != nil {
			t.Errorf("fieldSpecs[%s] produced params the contract rejects: %v (params=%s)", key, err, raw)
		}
	}
}

func TestValidateFault_EnvelopeRules(t *testing.T) {
	lat := json.RawMessage(`{"delay":"100ms"}`)

	cases := []struct {
		name      string
		cat, typ  string
		params    json.RawMessage
		env       Envelope
		wantError bool
	}{
		{"inline ok", "inline", "latency", lat, Envelope{}, false},
		{"inline with process host", "inline", "latency", lat, Envelope{Host: "process"}, false},
		{"inline with proxy host", "inline", "latency", lat, Envelope{Host: "proxy"}, true},
		{"inline with network env", "inline", "latency", lat, Envelope{HasNetwork: true, Target: "x"}, true},
		{"network ok", "network", "latency", lat, Envelope{HasNetwork: true, Target: "redis"}, false},
		{"network missing env", "network", "latency", lat, Envelope{}, true},
		{"network proxy missing target", "network", "latency", lat, Envelope{HasNetwork: true}, true},
		{"network bad direction", "network", "latency", lat, Envelope{HasNetwork: true, Target: "redis", Direction: "sideways"}, true},
		{"network scope > 1", "network", "latency", lat, Envelope{HasNetwork: true, Target: "redis", Scope: 1.5}, true},
		{"network process host", "network", "latency", lat, Envelope{Host: "process", HasNetwork: true, Target: "redis"}, true},
		{"unknown type", "inline", "explode", json.RawMessage(`{}`), Envelope{}, true},
		{"bad params", "inline", "latency", json.RawMessage(`{"delay":"nope"}`), Envelope{}, true},
		{"unknown param field", "inline", "latency", json.RawMessage(`{"delay":"100ms","x":1}`), Envelope{}, true},
		{"resource ok", "resource", "cpu", json.RawMessage(`{"target_load":0.5}`), Envelope{Host: "process"}, false},
	}
	for _, c := range cases {
		err := ValidateFault(c.cat, c.typ, c.params, c.env)
		if (err != nil) != c.wantError {
			t.Errorf("%s: ValidateFault = %v, wantError=%v", c.name, err, c.wantError)
		}
	}
}
