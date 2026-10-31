package api

import (
	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
)

// RuleSyncWire is an instance's desired state on the wire (desired-state
// protocol v1 §2): the body of a 200 poll and, after status, of the register
// response. Field names and tags follow the contract's RuleSync.
//
// TODO(desired-state): swap for atroposdk.RuleSync / RegisterResponse once atropos-go adds Etag and ReplaySet.
type RuleSyncWire struct {
	Version      uint64                   `json:"version"` // the generation, under the historical name
	Etag         string                   `json:"etag"`
	Rules        []atroposdk.CompiledRule `json:"rules"`         // null = "no change" (degraded register only)
	ActiveFaults []atroposdk.FaultRequest `json:"active_faults"` // never null
	FreezeCfg    *atroposdk.DelayRequest  `json:"freeze_cfg,omitempty"`
	ReplaySet    *model.ReplaySetRef      `json:"replay_set,omitempty"`
}

// RegisterResponseWire is the POST /api/v1/sdk/register 201 body: the
// registration status plus the instance's materialized desired state.
type RegisterResponseWire struct {
	Status string `json:"status"`
	RuleSyncWire
}

// ruleSyncFrom puts a materialized document on the wire.
func ruleSyncFrom(doc desired.Document) RuleSyncWire {
	return RuleSyncWire{Version: doc.Generation, Etag: doc.ETag, Rules: doc.Rules,
		ActiveFaults: doc.ActiveFaults, FreezeCfg: doc.FreezeCfg, ReplaySet: doc.ReplaySet}
}
