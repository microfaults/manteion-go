# Decision: policy engine WIP-freeze (2026-06)

**Status:** accepted (deprecated-until-rebuild).

The policy engine (`internal/policy`) — a 10s promql evaluation loop with
in-memory cooldowns that fires `push_rules`/`clear_rules` actions via
atrocontrol — is **disabled by default**. `cmd/manteion` starts the
goroutine only when `MANTEION_POLICY_ENGINE=on`.

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
