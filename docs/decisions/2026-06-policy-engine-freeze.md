# Decision: policy engine WIP-freeze (2026-06)

**Status:** accepted (deprecated-until-rebuild).

The policy engine (`internal/policy`) — a 10s promql evaluation loop with
in-memory cooldowns that fires `push_rules`/`clear_rules` actions via
atrocontrol — is **disabled by default**. `cmd/manteion` starts the
goroutine only when `MANTEION_POLICY_ENGINE=threshold`; since 2026-11, `=on`
(like `=rego`) starts the OPA harness instead (`docs/opa-harness.md`).

Why frozen rather than deleted:

- The `attack` and `cachebox_mode_change` action types were never
  implemented; only rule push/clear works.
- The metric-triggered automation story belongs to the phase-aware
  orchestrator FSM rebuild (see the stub note in
  `internal/orchestrator/orchestrator.go`) — building it twice on two
  models is waste.
- No UI surface depends on the engine; the `policy_rules` table and CRUD
  endpoints stay so stored definitions survive the freeze.

The epoch-2 schema keeps `policy_rules` unchanged (and drops the old
`idx_policy_rules_enabled` partial index, which indexed the wrong column
for the engine's scan). Revisit when the orchestrator FSM port lands.

**2026-10:** `push_rules`/`clear_rules` now route through rule CRUD —
`push_rules` enables each listed rule and `clear_rules` disables every
enabled rule of the service, each through `RuleRepo.Update`, which bumps the
service's desired-state generation in the same transaction; nothing is pushed
to `/admin/rules` any more (`internal/atrocontrol`'s push paths are gone). The
engine stays opt-in (`MANTEION_POLICY_ENGINE=threshold`; `=on` now starts the
OPA harness). Unlike the old push, which
was a transient override the next poll undid, `clear_rules` is persistent: it
disables every enabled rule of the service in the database, phase-attached
rules of a running experiment included, and they stay disabled until someone
enables them again.
