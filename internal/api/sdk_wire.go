package api

import (
	"fmt"
	"math"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
)

// The bodies on the SDK-facing endpoints are the SDK's own wire types, not
// local mirrors: the poll's 200 is an atroposdk.RuleSync, the register 201 an
// atroposdk.RegisterResponse (a status plus the same RuleSync) and the status
// report an atroposdk.StatusReport. A field the SDK renames, retypes or drops
// stops this package from compiling instead of failing on the wire; one it
// adds fails sdk_wire_test.go, whose parity tests build fully populated values
// and pin the bytes against the SDK's own encoder and decoder.
//
// One shape stays manteion's: model.ReplaySetRef, the persistence type, whose
// count is the BIGINT column's int64 (replaySetWire converts it at this
// boundary). The register answer for a service whose desired state cannot be
// materialized is the SDK's own empty RegisterResponse too (§2 Register: etag
// absent or empty, which the SDK treats alike).

// ruleSyncFrom puts a materialized document on the wire as the SDK's RuleSync
// (desired-state protocol v1 §2). It fails only when the replay set cannot be
// represented there (replaySetWire); the caller must not send a document then.
func ruleSyncFrom(doc desired.Document) (atroposdk.RuleSync, error) {
	set, err := replaySetWire(doc.ReplaySet)
	if err != nil {
		return atroposdk.RuleSync{}, err
	}
	return atroposdk.RuleSync{Version: doc.Generation, Etag: doc.ETag, Rules: doc.Rules,
		ActiveFaults: doc.ActiveFaults, FreezeCfg: doc.FreezeCfg, ReplaySet: set}, nil
}

// replaySetWire converts the stored replay-set ref to the SDK's, nil to nil.
// Every field is copied by name, so a field the SDK drops or renames breaks the
// build (one it adds is caught by TestReplaySetWire); the entry count is
// narrowed to the SDK's int with a bounds check.
func replaySetWire(ref *model.ReplaySetRef) (*atroposdk.ReplaySetRef, error) {
	if ref == nil {
		return nil, nil
	}
	count, err := narrowCount(ref.Count, math.MinInt, math.MaxInt)
	if err != nil {
		return nil, fmt.Errorf("replay set %s: %w", ref.ID, err)
	}
	return &atroposdk.ReplaySetRef{ID: ref.ID, ExperimentID: ref.ExperimentID, PhaseID: ref.PhaseID,
		KeyStrategy: ref.KeyStrategy, Count: count, SHA256: ref.SHA256, SizeBytes: ref.SizeBytes, URL: ref.URL}, nil
}

// narrowCount converts a stored count to int when it lies in [lo, hi], else
// errors rather than truncating. replaySetWire passes the platform's int range,
// which holds every int64 on a 64-bit build and only 32 bits on a 32-bit one;
// the bounds are parameters so the check is testable on any platform.
func narrowCount(n, lo, hi int64) (int, error) {
	if n < lo || n > hi {
		return 0, fmt.Errorf("count %d does not fit an int (%d to %d)", n, lo, hi)
	}
	return int(n), nil
}

// observedStatusFrom takes the store's view of a decoded StatusReport: the
// report is a full snapshot, so every field but the generation replaces what is
// stored. UnsupportedRule and ReplaySetStatus convert by Go type conversion,
// which compiles only while manteion's shapes and the SDK's have the same
// fields, so a field the SDK adds, drops or retypes breaks the build instead of
// going missing from the stored report. A nil list stays nil.
func observedStatusFrom(rep atroposdk.StatusReport) model.ObservedStatus {
	st := model.ObservedStatus{AppliedGeneration: rep.AppliedGeneration, Etag: rep.Etag, Errors: rep.Errors}
	if rep.Unsupported != nil {
		st.Unsupported = make([]model.UnsupportedRule, len(rep.Unsupported))
		for i, u := range rep.Unsupported {
			st.Unsupported[i] = model.UnsupportedRule(u)
		}
	}
	if rep.ReplaySet != nil {
		set := model.ReplaySetStatus(*rep.ReplaySet)
		st.ReplaySet = &set
	}
	return st
}
