# Cache-Box Rules, Policy Push Fix & Model Cleanup

**Date:** 2026-05-11
**Status:** Approved

## Problem

1. **Cache-box not expressible as a Rule.** atropos-go's evaluator natively supports cache-box decisions (`Decision.CacheBox`) but manteion's `Rule` model only has `FaultSpecID`/`FaultCompositionID`. Cache-box mode changes travel out-of-band via `atrocontrol.FreezeService` → admin endpoints, bypassing match criteria, priority, and per-workflow label scoping.

2. **Policy push path drops faults.** `policy/engine.go:loadCompiledRules` compiles `Rule` → `CompiledRule` (with faults) → `StaticRule` (faults dropped). `Decision.Fault` is an interface that can't JSON roundtrip. Pushed rules arrive as match-only shells.

3. **Transitional models.** `Flow`/`Persona`/`Workload` in manteion duplicate zeus's `Workflow`/`Run`/`Dataset`. `Attack`/`AttackResult` duplicates zeus's execution state.

## Design

### 1. Rule Model — Discriminated RuleAction

Replace `FaultSpecID`/`FaultCompositionID` with a `RuleAction` struct:

```go
type RuleAction struct {
    Type           string           `json:"type"`                        // "fault_spec" | "fault_composition" | "cachebox"
    FaultSpecID    string           `json:"fault_spec_id,omitempty"`     // when type=fault_spec
    FaultCompID    string           `json:"fault_composition_id,omitempty"` // when type=fault_composition
    CacheBox       *CacheBoxRuleConfig `json:"cachebox,omitempty"`       // when type=cachebox
}

type CacheBoxRuleConfig struct {
    Mode        string `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
    KeyStrategy string `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
}
```

`Rule.Action RuleAction` replaces `Rule.FaultSpecID` and `Rule.FaultCompositionID`.

Validation enforces type matches exactly one non-empty payload.

**DB migration:** Add `action_type TEXT NOT NULL DEFAULT 'fault_spec'`, `cachebox_mode TEXT`, `cachebox_key_strategy TEXT`. Keep existing FK columns. CHECK constraint ties `action_type` to which columns are populated. Backfill `action_type` from existing rows.

### 2. CompiledRule Wire Format

Add `CacheBox *CompiledCacheBox` to both manteion's `ruleconv.CompiledRule` and atropos's `CompiledRule`:

```go
type CompiledCacheBox struct {
    Mode        string `json:"mode"`
    KeyStrategy string `json:"key_strategy"`
}
```

XOR with `Fault`/`Composition` — exactly one must be set.

`ruleconv.compileRule` branches on `Action.Type == "cachebox"` to produce a `CompiledCacheBox`.

`atropos.DecodeCompiledRule` branches on `cr.CacheBox != nil` to set `Decision.CacheBox` and `Decision.CacheBoxKeyStrategy` (new field on Decision, or carried via labels).

### 3. Policy Push Path Fix

**Problem:** `loadCompiledRules` creates `[]StaticRule` with empty decisions. `StaticRule.Decision.Fault` is a Go interface that can't survive JSON marshal/unmarshal.

**Fix:**
- `loadCompiledRules` returns `[]ruleconv.CompiledRule` (wire format, all concrete types)
- `atropos.Client.PostRules` sends `[]ruleconv.CompiledRule` as JSON
- atropos `/admin/rules` accepts `[]CompiledRule`, decodes via `DecodeCompiledRules`, calls `eval.SetRules`

This reuses the same decode path as the SDK poll endpoint.

### 4. Remove Flow/Persona/Workload

Zeus's `Workflow`/`Run`/`Dataset` are canonical. Delete:
- `model.Flow`, `model.Persona`, `model.Workload`
- `store/workload_repo.go`
- Any handlers serving these
- DB migration drops `flows`, `personas`, `workloads` tables

### 5. Slim Down Attack Model

Manteion is source of truth for attack trigger config + results. Zeus handles execution.

- Remove `Status` lifecycle from manteion's `Attack` (zeus owns running/paused/stopped)
- Fold `AttackResult` fields into `Attack` (populated on completion callback from zeus)
- Keep: ID, trigger config (service, target, rate, duration, dedup_bypass), experiment linkage (experiment_run_id, policy_rule_id), result fields (latency percentiles, status codes, success rate, errors), timestamps (created_at, completed_at)
- Remove: `WorkloadID` (transitional), `AutoRuleID` (unused), `Role` (zeus concept)

Zeus communicates: running → completed/failed with result payload.

### 6. Note: cachebox_mode_change Policy Action

`PolicyAction.cachebox_mode_change` is validated but unimplemented in `policy/engine.go:209`. Now that cache-box is expressible as a Rule, this action type could be implemented as "push a cache-box rule" — same as `push_rules` but constructing a cache-box rule on the fly. **Deferred for now** — operators can create cache-box rules manually and use `push_rules` to activate them via policy.
