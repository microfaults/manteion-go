# Schema Refactors: Workflow Entity + Generic Rule Action

## Context

Three schema gaps flagged in the pre-PR review of `feat/inital-setup` against the Figma `manteion-ui` (node `2005:9`) and `manteion-ui/docs/API-NEEDED.md`:

1. **"Workflow" exists as two different things.** `model.Flow` (with steps DSL, targets, RPS estimate, thresholds) is the entity the UI's Workflows editor edits — but the result tables (`workflow_run_results`, `service_run_results`, `contribution_results`) use a free-text `workflow` column with no FK. Experiments can't reference multiple workflows, and "Flow" is k6-internal jargon the UI never uses.

2. **Rules are fault-only.** `model.Rule` carries `FaultSpecID` XOR `FaultCompositionID` with no cache-box action support. Cache-box mode changes can only reach SDKs via one-shot registration, not the rule polling path. Blocks future SDK-polled cache-box decisions.

3. **Experiment is missing `hypothesis` and `created_by`.** Both are visible in the Figma (Experiments list column + detail header) and have no backing field.

Pre-production — no data migration concerns.

> **Design note (cache-box on Rule):** UI option (b) in `manteion-ui/docs/API-NEEDED.md §C.1` recommends keeping cache-box scoped to `ExperimentRun.frozen_services` only and *not* exposing it on Rules. The backend deliberately supports both: `Rule.Action.cachebox` exists as a future-proofing capability for SDK-polled cache-box mode changes. The UI may continue to omit it from the Rules editor; the backend must not foreclose the path.

---

## Phase 1: Workflow as First-Class Entity (manteion-go only)

**Migration v6.** No SDK changes. Renames the existing `flows` entity to `workflows` (UI-aligned naming), introduces M:N with experiments, and FKs the result tables.

> Numbering assumes the AutoRules rename (`PolicyRule` → `AutoRule`, table `policy_rules` → `auto_rules`) lands first as migration v5. Renumber if not.

### 1.1 Migration SQL (`internal/db/migrations.go`)

Append migration `{6, "rename flows→workflows; M:N experiment_workflows; experiment hypothesis/created_by", ...}`:

```sql
-- Rename Flow → Workflow (UI-aligned).
ALTER TABLE flows RENAME TO workflows;
ALTER TABLE workloads RENAME COLUMN flow_id TO workflow_id;

-- M:N join: an experiment may exercise several workflows.
CREATE TABLE IF NOT EXISTS experiment_workflows (
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    workflow_id   TEXT NOT NULL REFERENCES workflows(id),
    role          TEXT NOT NULL DEFAULT 'primary'
                  CHECK (role IN ('primary','secondary')),
    PRIMARY KEY (experiment_id, workflow_id)
);
CREATE INDEX IF NOT EXISTS idx_experiment_workflows_workflow
    ON experiment_workflows(workflow_id);

-- Result tables: swap free-text workflow → FK workflow_id (pre-prod, no data).
ALTER TABLE workflow_run_results
    DROP COLUMN workflow,
    ADD COLUMN workflow_id TEXT NOT NULL REFERENCES workflows(id);

ALTER TABLE service_run_results
    DROP COLUMN workflow,
    ADD COLUMN workflow_id TEXT REFERENCES workflows(id);

ALTER TABLE contribution_results
    DROP COLUMN workflow,
    ADD COLUMN workflow_id TEXT NOT NULL REFERENCES workflows(id);

-- Surface fields the UI already shows.
ALTER TABLE experiments
    ADD COLUMN hypothesis  TEXT,
    ADD COLUMN created_by  TEXT;
```

### 1.2 Model rename: `Flow` → `Workflow`

- Move the `Flow` struct out of `internal/model/workload.go` into a new `internal/model/workflow.go` and rename it to `Workflow`. Field set unchanged (`ID`, `Name`, `Description`, `Targets`, `EstimatedRPSPerVU`, `Steps`, `Thresholds`, `CreatedAt`).
- Rename `Workload.FlowID` → `Workload.WorkflowID` (json tag `workflow_id`). Update `Validate()`.
- Add the join model to `workflow.go`:

```go
type ExperimentWorkflow struct {
    ExperimentID string `json:"experiment_id"`
    WorkflowID   string `json:"workflow_id"`
    Role         string `json:"role"` // "primary" | "secondary"
}
```

with a `Validate()` enforcing role ∈ {primary, secondary} and non-empty IDs.

### 1.3 Update result models (`internal/model/experiment.go`)

Rename the `Workflow string` field on:
- `WorkflowRunResult` → `WorkflowID` (json `workflow_id`)
- `ServiceRunResult` → `WorkflowID` (json `workflow_id,omitempty`)
- `ContributionResult` → `WorkflowID` (json `workflow_id`)

Update each `Validate()` to check `WorkflowID`.

Also add to `Experiment`:
- `Hypothesis string` (json `hypothesis,omitempty`)
- `CreatedBy   string` (json `created_by,omitempty`)

No new validation required for either — both are free-form metadata.

### 1.4 Extract `WorkflowRepo` from `WorkloadRepo`

`internal/store/workload_repo.go` currently mixes Flow/Persona/Workload/Attack methods. Move workflow methods into `internal/store/workflow_repo.go`:

- `WorkloadRepo.CreateFlow` → `WorkflowRepo.Create`
- `WorkloadRepo.GetFlow`    → `WorkflowRepo.Get`
- `WorkloadRepo.ListFlows`  → `WorkflowRepo.List`

Update SQL: `flows` → `workflows`. Add to `WorkflowRepo`:
- `GetByName(ctx, name)`
- `Delete(ctx, id)`
- `AddToExperiment(ctx, experimentID, workflowID, role)`
- `RemoveFromExperiment(ctx, experimentID, workflowID)`
- `ListForExperiment(ctx, experimentID) ([]model.ExperimentWorkflow, error)`

In `WorkloadRepo`: `flow_id` → `workflow_id` in the workload SQL (CREATE/GET/LIST/UPDATE).

### 1.5 Update `internal/store/experiment_repo.go`

All result-table SQL: `workflow` column → `workflow_id`, parameter `res.Workflow` → `res.WorkflowID`. Affected methods (line numbers from current HEAD, may shift):
- `CreateWorkflowResult` (line 227)
- `CreateServiceResult` (line 248)
- `CreateContribution` (line 271)
- `ListContributions` (line 290)

Also update Create/Get/List for experiments to read/write `hypothesis` + `created_by`.

### 1.6 New handler: `internal/api/workflow_handler.go`

CRUD following `rule_handler.go` pattern. Routes:
- `POST   /api/v1/workflows` — create
- `GET    /api/v1/workflows` — list
- `GET    /api/v1/workflows/{id}` — get
- `DELETE /api/v1/workflows/{id}` — delete
- `POST   /api/v1/experiments/{id}/workflows` — body `{workflow_id, role}`
- `DELETE /api/v1/experiments/{id}/workflows/{workflow_id}`

This also fulfils API-NEEDED §B.5 `GET /api/v1/flows` (under the new path `/workflows`).

### 1.7 Wire into `internal/api/server.go` + `cmd/manteion/main.go`

- Add `workflows *store.WorkflowRepo` field on `Server`.
- Update existing references from `WorkloadRepo.{CreateFlow,GetFlow,ListFlows}` to call `WorkflowRepo` instead.
- Register the new routes.

### 1.8 Update tests

- `internal/model/workload_test.go` → split: keep workload/persona/attack tests, move flow tests into a new `internal/model/workflow_test.go` (renamed to `Workflow`).
- `internal/model/experiment_test.go` — `Workflow` → `WorkflowID` in helpers; add `Hypothesis`/`CreatedBy` cases.
- New `internal/store/workflow_repo_test.go` if a pattern exists for other repos (mirrors `RuleRepo` tests).

---

## Phase 2: Rule as Generic Decision Carrier (manteion-go + atropos-go)

**Migration v7.** Wire format is additive (backward compatible).

> See "Design note (cache-box on Rule)" in Context — `cachebox` and `noop` action types are intentional even though the UI may not expose them in v1.1 of the Rules editor.

### 2.1 Migration SQL (`internal/db/migrations.go`)

Append migration `{7, "rule action generalization", ...}`:

```sql
ALTER TABLE rules
    ADD COLUMN action_type TEXT NOT NULL DEFAULT 'fault_spec'
        CHECK (action_type IN ('fault_spec','fault_composition','cachebox','noop')),
    ADD COLUMN cachebox_action TEXT
        CHECK (cachebox_action IS NULL OR cachebox_action IN ('passthrough','replay','replay_with_delay'));

-- Drop the unnamed CHECK that enforces fault_spec XOR fault_composition.
-- Postgres auto-names it; inspect with \d rules at dev time.
ALTER TABLE rules ADD CONSTRAINT rules_action_check CHECK (
    CASE action_type
        WHEN 'fault_spec'        THEN fault_spec_id IS NOT NULL AND fault_composition_id IS NULL AND cachebox_action IS NULL
        WHEN 'fault_composition' THEN fault_spec_id IS NULL AND fault_composition_id IS NOT NULL AND cachebox_action IS NULL
        WHEN 'cachebox'          THEN fault_spec_id IS NULL AND fault_composition_id IS NULL AND cachebox_action IS NOT NULL
        WHEN 'noop'              THEN fault_spec_id IS NULL AND fault_composition_id IS NULL AND cachebox_action IS NULL
    END
);

-- Mode becomes optional (NULL for cachebox/noop actions).
ALTER TABLE rules ALTER COLUMN mode DROP NOT NULL;
```

**Note:** the old unnamed XOR CHECK must be dropped first. Run `\d rules` at dev time to get the auto-generated name (likely `rules_check` or `rules_fault_spec_id_fault_composition_id_check`), then add the appropriate `DROP CONSTRAINT`.

### 2.2 Model changes: `internal/model/rule.go`

Replace top-level fault fields with `Action`:

```go
type Rule struct {
    ID        string        `json:"id"`
    Name      string        `json:"name"`
    Service   string        `json:"service"`
    Enabled   bool          `json:"enabled"`
    Priority  int           `json:"priority"`
    Match     MatchCriteria `json:"match"`
    Action    RuleAction    `json:"action"`
    CreatedAt time.Time     `json:"created_at"`
    UpdatedAt time.Time     `json:"updated_at"`
}

type RuleAction struct {
    Type               string `json:"type"`                           // "fault_spec"|"fault_composition"|"cachebox"|"noop"
    FaultSpecID        string `json:"fault_spec_id,omitempty"`
    FaultCompositionID string `json:"fault_composition_id,omitempty"`
    CacheBoxAction     string `json:"cachebox_action,omitempty"`      // "passthrough"|"replay"|"replay_with_delay"
    Mode               string `json:"mode,omitempty"`                 // "inline"|"background" — fault actions only
}
```

`RuleAction.Validate()` enforces the same invariants as the DB CHECK:
- `fault_spec`: FaultSpecID required, Mode required, others empty
- `fault_composition`: FaultCompositionID required, Mode required, others empty
- `cachebox`: CacheBoxAction required (valid values), others empty
- `noop`: all empty

### 2.3 Update store: `internal/store/rule_repo.go`

- `Create`/`Update`: write `rule.Action.Type` → `action_type`, `rule.Action.FaultSpecID` → `fault_spec_id`, etc.
- `scanRule`: read `action_type`, `cachebox_action` columns, reconstruct `rule.Action`
- All SELECT lists gain `action_type, cachebox_action`

### 2.4 Update compilation: `internal/ruleconv/ruleconv.go`

**Additive wire format** — keep old top-level fields for backward compat, add `Action`:

```go
type CompiledRule struct {
    Name           string               `json:"name"`
    InjectionPoint string               `json:"injection_point,omitempty"`
    Labels         map[string]string    `json:"labels,omitempty"`
    Mode           string               `json:"mode,omitempty"`            // backward compat
    Priority       int                  `json:"priority"`
    Fault          *CompiledFault       `json:"fault,omitempty"`           // backward compat
    Composition    *CompiledComposition `json:"composition,omitempty"`     // backward compat
    Action         *CompiledAction      `json:"action,omitempty"`          // NEW
}

type CompiledAction struct {
    Type        string               `json:"type"`
    Mode        string               `json:"mode,omitempty"`
    Fault       *CompiledFault       `json:"fault,omitempty"`
    Composition *CompiledComposition `json:"composition,omitempty"`
    CacheBox    string               `json:"cachebox,omitempty"`
}
```

`compileRule` branches on `r.Action.Type`:
- `fault_spec`: resolve spec → set BOTH `cr.Fault` (compat) AND `cr.Action.Fault`
- `fault_composition`: resolve comp → set BOTH `cr.Composition` AND `cr.Action.Composition`
- `cachebox`: set `cr.Action` only (no top-level equivalent)
- `noop`: set `cr.Action` with type "noop" only

### 2.5 Update API handlers: `internal/api/rule_handler.go`

No structural changes needed — handlers work through the model. JSON body shape changes (clients send `action: {type: "fault_spec", ...}` instead of top-level `fault_spec_id`).

### 2.6 Update atropos-go: `compiled_rule.go`

Add `CompiledAction` struct + `Action *CompiledAction` field on `CompiledRule`.

Update `DecodeCompiledRule`:
- If `cr.Action != nil`, use it as source of truth
- `cachebox` → set `sr.Decision.CacheBox` enum, `sr.Decision.Fault = nil`
- `noop` → nil Fault, CacheBoxNone
- `fault_spec`/`fault_composition` → existing decode path via `cr.Action.Fault`
- If `cr.Action == nil` → fall back to top-level `Fault`/`Composition` (backward compat)

### 2.7 Update tests (both repos)

**manteion-go:**
- `internal/model/rule_test.go` — rewrite all cases for `Action` struct
- `internal/ruleconv/ruleconv_test.go` — add cachebox/noop compile tests, verify backward compat

**atropos-go:**
- `compiled_rule_test.go` — add `TestDecodeCompiledRules_CacheBox{Replay,Passthrough,ReplayWithDelay}`, `TestDecodeCompiledRules_Noop`, `TestDecodeCompiledRules_BackwardCompat_NoAction`

---

## Deployment Order

1. **PR 0 (manteion-go, ad-hoc):** AutoRules rename — `PolicyRule` → `AutoRule`, table `policy_rules` → `auto_rules`, column `attacks.policy_rule_id` → `auto_rule_id`. Migration v5. Resolves the `Rule.Action` / `PolicyRule.Action` naming collision before Phase 2 lands.
2. **PR 1 (manteion-go):** Phase 1 — Flow→Workflow rename + experiment_workflows + hypothesis/created_by. Self-contained.
3. **PR 2 (manteion-go):** Phase 2 §§2.1–2.5 — rule action model + store + ruleconv. Emits both old and new wire fields.
4. **PR 3 (atropos-go):** Phase 2 §§2.6–2.7 — wire type + decoder update. Can deploy before or after PR 2 (new fields optional, falls back to old).

---

## Verification

### Phase 1
```bash
cd /Users/pronei/work/faults-lab/manteion-go
go build ./...
go vet ./...
go test ./internal/model/... ./internal/store/... ./internal/api/...
```

Manual: create a workflow via curl, add it to an experiment with role=primary, verify mapping persists; create an experiment with `hypothesis` + `created_by`, GET it back.

### Phase 2
```bash
# manteion-go
go build ./... && go test ./internal/model/... ./internal/ruleconv/... ./internal/store/... ./internal/api/...

# atropos-go
cd /Users/pronei/work/faults-lab/atropos-go
go build ./... && go test ./...
```

Manual: create a cachebox rule via curl, poll `/api/v1/sdk/rules`, verify the compiled output includes `action.cachebox`. Verify an old-style fault_spec rule still emits top-level `fault` for backward compat.

---

## Future / Deferred — DO NOT IMPLEMENT IN THIS PLAN

These came out of the same Figma + API-NEEDED.md review and are explicitly deferred. Each warrants its own plan when picked up. Listed here so the context isn't lost.

- **Match-criteria rewrite (`API-NEEDED §C.2`, blocker for Rules v1.1).** Replace flat `MatchCriteria.Labels map[string]string` with a nested AND/OR/NOT expression tree (`match_ast` JSON for round-tripping + optional `match_expr` Rego for evaluation). UI ships a builder component; backend chooses execution strategy (SDK-side OPA or manteion-side lookup). Keep `match_labels` as a derived projection for SDK polling backward compat.
- **Phase as a first-class entity.** UI Experiment detail uses "Phases" with reorderable rule-state expressions per service, per-phase VUs/duration/observed P99. Current `experiment_runs` is `baseline | isolation | combination` — the underlying mechanics differ from the UI's mental model. Decide whether to add a `phases` table on top of `experiment_runs`, or rename + reshape `experiment_runs` to match the UI verbatim.
- **Tenancy / environment scoping (`API-NEEDED §D.1`).** Sidebar shows `Faults Lab / online-boutique` breadcrumb; product decision pending: one manteion-go per env (kustomize overlay) vs. tenant column on every table.
- **Datasets entity (`API-NEEDED §B.6`).** UI Datasets screen exists ("Gap #6 planned") — pool counts, sample preview, NDJSON upload, TTL. No backend model today.
- **Workflow versioning.** UI shows `v2` label on workflows. Add a `version` column or version table when revisions become editable through the UI.
- **Kill switch / per-service disable (`API-NEEDED §D.2`).** UI design assumes per-service kill switch on the Services screen. No backend hook.
- **SDK instance enrichment (`API-NEEDED §C.8`).** Add `last_error`, `last_rule_version_acked`, `active_rule_ids[]` to `SDKInstance` (computed or persisted) so the planned Services detail panel can render.

---

## Flags (not blockers)

- **Unnamed CHECK constraint:** Migration v7 must drop the auto-generated CHECK on rules. Inspect with `\d rules` at dev time.
- **Baggage propagation:** `atropos.workflow` baggage currently carries a name string. Once workflows are ID-keyed, baggage value needs aligning. Out of scope.
- **`handlePollRules` fallback:** On compilation failure (`sdk_handler.go:131`), raw `model.Rule` is returned. After refactor, this shape changes. Consider returning an error instead of the raw model.
