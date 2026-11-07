# Decision: metric- and log-conditioned automation is the OPA harness (2026-10)

**Status:** accepted (2026-10-30).

Context: ADR-1 of the daemon design review asked where automation that
depends on system state (latency percentiles, error rates, log volumes)
should live: in the SDK's per-request matcher, or centrally. The user
resolved it on 2026-10-30 as "keep it simple". The SDK keeps its label-based
matcher, and conditions on system state are evaluated centrally. The 2026-06
freeze of the PromQL threshold engine
(`docs/decisions/2026-06-policy-engine-freeze.md`) said to revisit it "when
the orchestrator FSM port lands". This decision is that revisit.

Decision:

- Metric- and log-conditioned automation is the OPA harness
  (`internal/opaharness`, `docs/opa-harness.md`). It runs centrally in
  manteion, evaluates Rego policies over Prometheus and Loki instant queries,
  and emits rule mutations (`upsert_rule`, `set_enabled`, `set_params`,
  `delete_rule`). It applies them through `store.RuleRepo`, so they reach
  instances through the desired-state generations like any rule edit. Nothing
  is pushed to an instance.
- The SDK keeps label matching; no condition on system state moves into it.
- `MANTEION_POLICY_ENGINE` selects the loop: `rego` (or `on`) starts the
  harness, `threshold` starts the frozen engine, and `off` (the default)
  starts neither. An unknown value logs a Warn and starts neither. `on`
  previously started the threshold engine and now starts the harness; the
  threshold engine is reachable only as `threshold`.
- The frozen `internal/policy` engine is a removal candidate once the harness
  has run a bed experiment. Its `policy_rules` table and `/api/v1/policies`
  CRUD are untouched by this change.

Consequences:

- The binary grows by about 16 MB stripped (19271026 → 35356802 bytes) and
  21.5 MB unstripped (29182226 → 50717666), measured on darwin/arm64; see
  `docs/opa-harness.md` ("Cost and limits").
- The OPA version is tied to the Go toolchain: v1.19.1 fits the module's
  `go 1.25.5`, and a newer OPA can require raising the `go` directive.
- A second policy language, Rego, sits next to the UI's rule model.
  Policies are stored in `opa_policies` and managed through
  `/api/v1/opa/policies`.
- Every evaluation is recorded in Postgres (`policy_decisions`, migration 15),
  with retention of the newest 1000 decisions per policy.
