# Desired-State Protocol v1 — manteion plan 1: materializer, register, poll, status

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** manteion materializes `DesiredState(service, instance)` from the database with a per-service generation and a content ETag, serves it on register and on a long-polling `GET /api/v1/sdk/rules`, filtered by the instance's capabilities (this is M3), and records what each instance observed (`POST /api/v1/sdk/status`).

**Architecture:** A new `internal/desired` package owns three things: the SSE/long-poll `Broker` (moved from `internal/api`), `Generations` (per-service counter in `service_generations`, bumped in the same transaction as every input change, with waiters woken after commit) and `Materializer` (a pure function of the DB: phase-aware rules, active fault configs, the synthesized cache-box rule, freeze config and replay-set ref, filtered by capabilities, hashed into `"<generation>-<sha256[:16]>"`). The handlers become thin: register persists then materializes; poll records observed state, honours `If-None-Match`/`wait`, answers 200 or 304 with `ETag` + `X-Atropos-Generation`; status stores the instance's report. The in-memory intent tracker is no longer read by the API (plan 2 deletes it); freeze config and replay-set are left as hooks that plan 2 fills from the frozen phase.

**Tech Stack:** Go 1.25 (`go.mod`), database/sql + pgx, swaggo v2 (`make openapi`), DB-free handler tests with fakes behind narrow interfaces, DB-gated tests via `testutil.TestDB(t)` (`MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1`, Postgres on localhost).

**Spec:** `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0/docs/plans/2026-10-30-desired-state-protocol.md` (authoritative contract), with `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0/docs/plans/2026-09-27-daemon-design-review-2.md` §3 as rationale. Plan 2 (`2026-10-30-desired-state-manteion-2-orchestrator.md`) builds on this one; the sibling atropos-go plan implements the SDK side of the same contract.

Repo: `/Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon` (branch `feat/atropos-daemon`, base develop a85ee9e). Every path below is relative to it unless absolute. Line numbers are as of a85ee9e.

## Global Constraints

- Wire types come from atropos-go through `go.work` (`atroposdk "git.ucsc.edu/microfaults/atropos-go"`): `RegisterRequest{..., Host, Capabilities}`, `RuleSync{Version, Rules, ActiveFaults, FreezeCfg}`, `DelayRequest`, `CompiledRule`, `FaultRequest`. The NEW wire fields (`RuleSync.Etag`, `RuleSync.ReplaySet`, the status body) are defined in atropos-go by the sibling plan; here they are manteion-local structs (`internal/api/sdk_wire.go`, `internal/model/desired.go`) carrying the comment `TODO(desired-state): swap for the atropos-go alias when it lands`.
- Capability vocabulary (contract §1, atropos-go `internal/instrument/capabilities.go:7-12`): `inline`, `network`, `resource`, `cachebox`. Absent capabilities = all. Unknown advertised strings (the sidecar may advertise `netem`) are ignored: they never reject a registration and match no category. A fault's category `""` means `inline` (`wire.FaultRequest.EffectiveCategory`). Compositions ship as today (a composition is kept iff every leaf category is supported).
- `RuleSync.replay_set` carries `id, experiment_id, phase_id, key_strategy, count, sha256, size_bytes, url` (contract §2 amendment of 2026-10-30): the pair names the context the set is installed under; a set without a pair is unusable. The materializer fills the pair from the frozen phase (plan 2) and the artifact record stores it.
- ETag = `"<generation>-<sha256 hex[:16] of the canonical JSON>"` (contract §1). `version` on the wire keeps its name and now carries the generation (contract §5).
- `GET /api/v1/sdk/rules`: `version` = the instance's observed generation, recorded with `last_poll_at` before anything else; `wait` seconds, default 0, max 60; `If-None-Match`; 200 `RuleSync` + `ETag` + `X-Atropos-Generation`, or 304 with the same headers (contract §2).
- Register persists `host` (column `host_kind TEXT`) and `capabilities` (JSONB); the response is the materialized `RuleSync` plus `status`; different capabilities than stored bump the generation (contract §2).
- `service_generations(service TEXT PRIMARY KEY, generation BIGINT NOT NULL DEFAULT 1, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; absent row = generation 1.
- Migrations are embedded Go, append-only, contiguous numbering (`internal/db/migrations.go:103-115`); this plan adds migration 12 using `ADD COLUMN IF NOT EXISTS` and JSONB for lists (no `TEXT[]`).
- SSE event `rules_changed` with data `{"generation":G}` on every bump, orchestrator bumps included (contract §2).
- Conventional commit messages, each ending with the trailer line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- After every task: `gofmt -l .` prints nothing, `go vet ./...` and `go test ./...` pass (run from the repo root; use `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon ...` from elsewhere).
- Do not touch `.claude/` or `vendor/`.

## Review Focus

Failure modes the contract implies that no happy-path test covers; each has its pin added to the owning task.

1. **The HTTP server's 30 s `WriteTimeout` (`cmd/manteion/main.go:133`) kills a 60 s long-poll hold.** A held poll must extend its write deadline (`http.NewResponseController`), exactly as `handleSSEEvents` does, or every long-poll longer than 30 s is a dropped connection. Pinned in Task 6 (`TestHandlePollRules_HoldSurvivesServerWriteTimeout`).
2. **A deregistered or unknown `instance_id` keeps polling (pod replaced, row purged).** The poll must still answer with the unfiltered document and only warn on the failed observed-state write, never 404/500. Pinned in Task 6 (`unknown instance_id`).
3. **`If-None-Match` arrives weak (`W/"..."`) or with a foreign/garbled ETag.** Weak must match; garbage must be a mismatch → 200, never a hang. Pinned in Task 6.
4. **`capabilities: []` or re-ordered capabilities on re-register.** An empty list means absent (all); the same set in another order is not a change and must not bump. Pinned in Task 5.
5. **A materialize error (one broken rule) on register.** Register must not crash-loop the pod: answer 201 with `rules: null` (the SDK's "no change") and log; the poll keeps answering 500 as today. Pinned in Task 5 (`materialize failure → 201 degraded`).

---

## File structure

Created:
- `internal/model/desired.go` — `UnsupportedRule`, `ReplaySetStatus`, `ObservedStatus`, `ReplaySetRef`, `ValidReplaySetState`.
- `internal/store/generation_repo.go` — `GenerationRepo` (SQL for `service_generations`).
- `internal/store/sdk_repo_test.go` — DB-gated round-trip of the new columns.
- `internal/db/migration12_test.go` — DB-gated schema assertion (external test package).
- `internal/desired/doc.go`, `broker.go` (moved from `internal/api/sse_broker.go`), `generations.go`, `memstore.go`, `capabilities.go`, `materializer.go` + tests.
- `internal/api/sdk_wire.go` — `RuleSyncWire`, `RegisterResponseWire`, `ruleSyncFrom`.
- `internal/api/sdk_status_handler.go` — `POST /api/v1/sdk/status`.
- `internal/api/sdk_desired_handler_test.go` — DB-free register/poll/status tests.

Modified:
- `internal/db/migrations.go` — migration 12.
- `internal/model/sdk.go` — new `SDKInstance` fields.
- `internal/store/sdk_repo.go` — columns, `Register`, `RecordPoll` (replaces `TouchPoll`), `RecordStatus`.
- `internal/store/rule_repo.go` — in-tx generation bumps; `NewRuleRepo(db, gens)`; `BumpVersion` deleted (Task 8).
- `internal/testutil/db.go` — truncate `service_generations`.
- `internal/api/server.go`, `sdk_handler.go`, `rule_handler.go`, `fault_config_handler.go`, `fault_config_reaper.go`, `sdk_instance_handler.go`, `sse_broker.go`, `sdk_instance_handler_test.go`, `sdk_recording_test.go`.
- `internal/atrocontrol/intent.go`, `controller.go` — `FreezeConfigForService` adapter (temporary; plan 2 deletes the tracker).
- `internal/orchestrator/orchestrator.go`, `runner.go`, `drain.go`, `orchestrator_test.go`, `runner_test.go` — generation bumps replace `rule_version` bumps.
- `cmd/manteion/main.go` — wiring.
- `docs/swagger.yaml`, `docs/swagger.json` (regenerated), `README.md`, `CLAUDE.md`.

---

### Task 1: Migration 12, model fields, SDK store

**Files:**
- Modify: `internal/db/migrations.go:103-115` (list) and append `const migration12` after `migration11` (end of file)
- Modify: `internal/model/sdk.go:35-66`
- Create: `internal/model/desired.go`
- Modify: `internal/store/sdk_repo.go:34-39, 41-82, 93-113, 197-206`
- Modify: `internal/testutil/db.go:43-70`
- Create: `internal/store/sdk_repo_test.go`, `internal/db/migration12_test.go`

**Interfaces:**
- Consumes: `jsonbMarshal`/`jsonbScan`/`nullTimeToPtr`/`affectedOrNotFound` (`internal/store/helpers.go`), `testutil.TestDB(t)`.
- Produces: `model.SDKInstance{Host, Capabilities, ObservedGeneration, ObservedEtag, ObservedUnsupported, ObservedErrors, ReplaySetID, ReplaySetState, ObservedAt}`; `model.ObservedStatus`, `model.UnsupportedRule`, `model.ReplaySetStatus`, `model.ReplaySetRef`, `model.ValidReplaySetState(string) bool`; `(*store.SDKRepo).RecordPoll(ctx, id string, observedGeneration uint64) error`; `(*store.SDKRepo).RecordStatus(ctx, id string, st model.ObservedStatus) error`; `Register` persisting host/capabilities and resetting observed columns on re-register.

- [ ] **Step 1: Write the failing DB-gated migration test**

`internal/db/migration12_test.go` (package `db_test` — `testutil` imports `db`, so an internal test package would cycle):

```go
package db_test

import (
	"testing"

	"manteion-go/internal/testutil"
)

// TestMigration12_DesiredStateColumns pins migration 12: the observed-state
// columns on sdk_instances and the per-service generation table.
func TestMigration12_DesiredStateColumns(t *testing.T) {
	conn := testutil.TestDB(t)
	for _, col := range []string{"host_kind", "capabilities", "observed_generation", "observed_etag",
		"observed_unsupported", "observed_errors", "replay_set_id", "replay_set_state", "observed_at"} {
		var n int
		if err := conn.QueryRow(`SELECT count(*) FROM information_schema.columns
			WHERE table_name = 'sdk_instances' AND column_name = $1`, col).Scan(&n); err != nil || n != 1 {
			t.Errorf("sdk_instances.%s: present=%d err=%v, want 1", col, n, err)
		}
	}
	var gen uint64
	if err := conn.QueryRow(`INSERT INTO service_generations (service) VALUES ('m12-probe') RETURNING generation`).Scan(&gen); err != nil {
		t.Fatalf("service_generations insert: %v", err)
	}
	if gen != 1 {
		t.Errorf("default generation = %d, want 1", gen)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/db/ -run TestMigration12 -count=1 -v`
Expected: FAIL (`sdk_instances.host_kind: present=0`, then `relation "service_generations" does not exist`). Without the env vars it is SKIPped — that is the DB-free baseline.

- [ ] **Step 3: Add migration 12**

Append after `migration11` in `internal/db/migrations.go`:

```go
// migration12 is the desired-state protocol v1 (docs/plans/2026-10-30-desired-state-*):
// what each SDK instance advertised at registration (host, capabilities) and
// what it has observed since (generation, etag, unsupported rules, errors,
// replay-set state), plus the per-service generation the materializer stamps
// on every document. Absent service row = generation 1. Lists are JSONB
// (the schema has no TEXT[] columns); IF NOT EXISTS keeps the step re-runnable.
const migration12 = `
ALTER TABLE sdk_instances
    ADD COLUMN IF NOT EXISTS host_kind            TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS capabilities         JSONB,
    ADD COLUMN IF NOT EXISTS observed_generation  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_etag        TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS observed_unsupported JSONB,
    ADD COLUMN IF NOT EXISTS observed_errors      JSONB,
    ADD COLUMN IF NOT EXISTS replay_set_id        TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS replay_set_state     TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS observed_at          TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS service_generations (
    service    TEXT PRIMARY KEY,
    generation BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`
```

and the list entry after line 114 (`{11, ...}`):

```go
	{12, "sdk_instances: host/capabilities + observed state; service_generations (desired-state v1)", migration12},
```

Add `"service_generations"` to `truncateAll` in `internal/testutil/db.go` (after `"sdk_instances"`).

- [ ] **Step 4: Run the migration test to verify it passes**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/db/ -run TestMigration12 -count=1 -v`
Expected: PASS

- [ ] **Step 5: Write the failing store test**

`internal/store/sdk_repo_test.go` (package `store`; the package `TestMain` in `rule_repo_test.go:37-66` already gates on `MANTEION_TEST_DB`, and `testutil.TestDB` adds the `MANTEION_INTEGRATION` gate + truncation):

```go
package store

import (
	"context"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/testutil"
)

// TestSDKRepo_ObservedStateRoundTrip pins migration 12 end to end: Register
// persists host/capabilities and resets observed state, RecordPoll records the
// held generation with the poll timestamp, RecordStatus stores the report, and
// every column scans back (the scan-arity bug class).
func TestSDKRepo_ObservedStateRoundTrip(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewSDKRepo(conn)

	inst := &model.SDKInstance{ID: "inst-rt", Service: "cartservice", Address: "10.0.0.1:9090",
		Host: "sidecar", Capabilities: []string{"inline", "network", "cachebox"}}
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, err := repo.Get(ctx, inst.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Host != "sidecar" || len(got.Capabilities) != 3 || got.Capabilities[2] != "cachebox" {
		t.Errorf("host/capabilities = %q/%v, want sidecar/[inline network cachebox]", got.Host, got.Capabilities)
	}
	if got.ObservedGeneration != 0 || got.ObservedAt != nil || got.ReplaySetState != "" {
		t.Errorf("fresh instance observed state = %+v, want zero", got)
	}

	if err := repo.RecordPoll(ctx, inst.ID, 7); err != nil {
		t.Fatalf("record poll: %v", err)
	}
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{
		AppliedGeneration: 7, Etag: "7-abcdef0123456789",
		Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}},
		Errors:      []string{"rule x: decode: boom"},
		ReplaySet:   &model.ReplaySetStatus{ID: "sha256:aa", State: "fetching"},
	}); err != nil {
		t.Fatalf("record status: %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.ObservedGeneration != 7 || got.ObservedEtag != "7-abcdef0123456789" || got.ObservedAt == nil ||
		len(got.ObservedUnsupported) != 1 || got.ObservedUnsupported[0].Category != "resource" ||
		len(got.ObservedErrors) != 1 || got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "fetching" {
		t.Errorf("after status: %+v", got)
	}
	if time.Since(got.LastPollAt) > time.Minute {
		t.Errorf("last_poll_at not touched by RecordPoll: %v", got.LastPollAt)
	}

	// A report without replay_set keeps the stored replay fields; empty lists clear.
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{AppliedGeneration: 8}); err != nil {
		t.Fatalf("record status 2: %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "fetching" || len(got.ObservedUnsupported) != 0 || len(got.ObservedErrors) != 0 {
		t.Errorf("after second status: %+v", got)
	}

	// Re-register resets observed state and replaces capabilities; nil capabilities → absent.
	inst.Capabilities = nil
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.Capabilities != nil || got.ObservedGeneration != 0 || got.ObservedEtag != "" || got.ReplaySetID != "" || got.ObservedAt != nil {
		t.Errorf("after re-register: %+v", got)
	}
	// ForService and List share the scanner.
	if insts, err := repo.ForService(ctx, "cartservice"); err != nil || len(insts) != 1 {
		t.Errorf("ForService = %d err=%v, want 1", len(insts), err)
	}
	if _, err := repo.RecordPoll(ctx, "inst-missing", 1), repo.RecordStatus(ctx, "inst-missing", model.ObservedStatus{}); err == nil {
		t.Error("RecordStatus on an unknown instance must return ErrNotFound")
	}
}
```

(The last assertion is two calls; keep the `RecordPoll` unknown-instance check as its own `if err := ...; !errors.Is(err, ErrNotFound)` if you prefer explicit sentinels.)

- [ ] **Step 6: Run it to verify it fails**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -run TestSDKRepo_ObservedStateRoundTrip -count=1`
Expected: compile error (`inst.Host undefined`, `repo.RecordPoll undefined`).

- [ ] **Step 7: Add the model types**

`internal/model/sdk.go`, extend `SDKInstance` (after `Routes`):

```go
	// Host names the instrument host that registered ("in-process" | "sidecar");
	// stored as host_kind — "host" already means fault placement elsewhere.
	Host string `json:"host,omitempty"`
	// Capabilities lists the fault categories the host executes
	// (inline|network|resource|cachebox). Absent = all (the wire contract).
	Capabilities []string `json:"capabilities,omitempty"`
	// Observed state (desired-state protocol v1): what the instance last told
	// us it holds. ObservedGeneration comes from the poll's version param and
	// the status report; the rest only from POST /api/v1/sdk/status.
	ObservedGeneration  uint64            `json:"observed_generation"`
	ObservedEtag        string            `json:"observed_etag,omitempty"`
	ObservedUnsupported []UnsupportedRule `json:"observed_unsupported,omitempty"`
	ObservedErrors      []string          `json:"observed_errors,omitempty"`
	ReplaySetID         string            `json:"replay_set_id,omitempty"`
	ReplaySetState      string            `json:"replay_set_state,omitempty"` // fetching|committed|failed
	ObservedAt          *time.Time        `json:"observed_at,omitempty"`
```

`internal/model/desired.go`:

```go
package model

// Desired-state protocol v1 (docs/plans/2026-10-30-desired-state-protocol.md).
// TODO(desired-state): swap ReplaySetRef and the status shapes for the
// atropos-go wire aliases once the sibling SDK plan lands them.

// UnsupportedRule is one rule an instance skipped because its host cannot
// execute the fault category.
type UnsupportedRule struct {
	Rule     string `json:"rule"`
	Category string `json:"category"`
}

// ReplaySetStatus is the instance's replay-set progress for the artifact it
// was told to hold.
type ReplaySetStatus struct {
	ID    string `json:"id"`
	State string `json:"state"` // fetching | committed | failed
}

// ValidReplaySetState reports whether s is a state the status endpoint accepts.
func ValidReplaySetState(s string) bool {
	switch s {
	case "fetching", "committed", "failed":
		return true
	}
	return false
}

// ObservedStatus is what POST /api/v1/sdk/status persists. Unsupported and
// Errors replace the previous report (empty clears); a nil ReplaySet keeps the
// stored replay fields (the report carries it only when the state changed).
type ObservedStatus struct {
	AppliedGeneration uint64
	Etag              string
	Unsupported       []UnsupportedRule
	Errors            []string
	ReplaySet         *ReplaySetStatus
}

// ReplaySetRef is the replay-set artifact a frozen service's instances must
// hold (RuleSync.replay_set). ID is "sha256:<hex>" of the artifact file bytes,
// ExperimentID/PhaseID the pair the set is installed under (the cache-box
// serves it only to the rule whose context matches), SHA256 the W5 digest
// over its entries, URL the artifact endpoint path.
type ReplaySetRef struct {
	ID           string `json:"id"`
	ExperimentID string `json:"experiment_id"`
	PhaseID      string `json:"phase_id"`
	KeyStrategy  string `json:"key_strategy"`
	Count       int64  `json:"count"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	URL         string `json:"url"`
}
```

- [ ] **Step 8: Update the SDK store**

`internal/store/sdk_repo.go`:

```go
var selectColumns = fmt.Sprintf(
	`id, service, version, address, poll_interval_ms, routes, registered_at, last_poll_at,
	 host_kind, capabilities, observed_generation, observed_etag, observed_unsupported, observed_errors,
	 replay_set_id, replay_set_state, observed_at, %s AS status`,
	statusExpr,
)
```

`Register`: marshal `inst.Capabilities` the way routes are (`len == 0` → SQL NULL = absent = all), add `host_kind, capabilities` to the INSERT column list/values (`$7, $8`), and extend the `ON CONFLICT` SET with `host_kind = EXCLUDED.host_kind, capabilities = EXCLUDED.capabilities, observed_generation = 0, observed_etag = '', observed_unsupported = NULL, observed_errors = NULL, replay_set_id = '', replay_set_state = '', observed_at = NULL` — a re-registering instance starts from nothing; its register response hands it the current document.

`scanInstance`: scan `host_kind` into `inst.Host`, `capabilities`/`observed_unsupported`/`observed_errors` as `[]byte` through `jsonbScan`, `observed_generation`, `observed_etag`, `replay_set_id`, `replay_set_state`, `observed_at` as `sql.NullTime` → `nullTimeToPtr`, in the `selectColumns` order (status last).

Replace `TouchPoll` (lines 197-206) with:

```go
// RecordPoll is the per-poll write (desired-state v1): the poll timestamp that
// drives liveness and the generation the instance says it holds. Runs BEFORE
// the handler decides 200/304 — most polls are no-change, and this is how the
// orchestrator observes convergence without another round trip.
func (r *SDKRepo) RecordPoll(ctx context.Context, id string, observedGeneration uint64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE sdk_instances SET last_poll_at = now(), observed_generation = $2 WHERE id = $1`, id, observedGeneration)
	if err != nil {
		return fmt.Errorf("record poll: %w", err)
	}
	return affectedOrNotFound(res)
}

// RecordStatus stores an instance's POST /api/v1/sdk/status report.
func (r *SDKRepo) RecordStatus(ctx context.Context, id string, st model.ObservedStatus) error {
	unsupported, errs := jsonbOrNull(st.Unsupported), jsonbOrNull(st.Errors)
	replayID, replayState, setReplay := "", "", false
	if st.ReplaySet != nil {
		replayID, replayState, setReplay = st.ReplaySet.ID, st.ReplaySet.State, true
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE sdk_instances SET
			observed_generation = $2, observed_etag = $3,
			observed_unsupported = $4, observed_errors = $5,
			replay_set_id    = CASE WHEN $6 THEN $7 ELSE replay_set_id END,
			replay_set_state = CASE WHEN $6 THEN $8 ELSE replay_set_state END,
			observed_at = now()
		WHERE id = $1`,
		id, st.AppliedGeneration, st.Etag, unsupported, errs, setReplay, replayID, replayState)
	if err != nil {
		return fmt.Errorf("record status: %w", err)
	}
	return affectedOrNotFound(res)
}
```

with `jsonbOrNull(v any) any` returning `nil` for an empty slice and `json.Marshal(v)` bytes otherwise (a small helper in this file; `jsonbMarshal` would write the string `null`, which is not SQL NULL).

- [ ] **Step 9: Run the store test and the full suite**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ ./internal/db/ -count=1`
Expected: `ok` for both.
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: no gofmt output, `ok` everywhere (the DB suites skip). `go build ./...` fails on `s.sdk.TouchPoll` in `internal/api/sdk_handler.go:208` — replace that call with `s.sdk.RecordPoll(ctx, instanceID, requestedVersion)` now (the handler is rewritten in Task 6).

- [ ] **Step 10: Commit**

```bash
git add internal/db/migrations.go internal/db/migration12_test.go internal/model/sdk.go internal/model/desired.go internal/store/sdk_repo.go internal/store/sdk_repo_test.go internal/testutil/db.go internal/api/sdk_handler.go
git commit -m "feat(store): migration 12 — sdk_instances host/capabilities + observed state, service_generations

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `internal/desired` — Broker (moved), Generations, stores

**Files:**
- Create: `internal/desired/doc.go`, `internal/desired/broker.go`, `internal/desired/generations.go`, `internal/desired/memstore.go`, `internal/desired/generations_test.go`, `internal/desired/generations_pg_test.go`
- Create: `internal/store/generation_repo.go`
- Modify: `internal/api/sse_broker.go` (becomes aliases; its test file is unchanged)

**Interfaces:**
- Consumes: `internal/api/sse_broker.go` (the moved code, 79 lines), `testutil.TestDB`.
- Produces:
  - `desired.Event{Type, Data string}`, `desired.Broker` with `NewBroker()`, `Subscribe(service) chan Event`, `Unsubscribe(service, ch)`, `Broadcast(service, Event)`, `ClientCount() int` (identical bodies to the api broker).
  - `desired.Store interface { Get(ctx, service string) (uint64, error); Bump(ctx, tx *sql.Tx, service string) (uint64, error) }` (tx may be nil).
  - `desired.NewGenerations(store Store, broker *Broker) *Generations`; methods `Broker() *Broker`, `Get(ctx, service) (uint64, error)`, `Bump(ctx, service) (uint64, error)` (bumps on the store with a nil tx, then `Notify`), `BumpTx(ctx, tx, service) (uint64, error)` (no notify — the caller notifies after commit), `Notify(service string, generation uint64)` (wakes waiters, broadcasts `rules_changed {"generation":G}`), `Wait(ctx, service string, after uint64, timeout time.Duration) (uint64, error)`.
  - `desired.NewMemoryStore() Store` (tests, absent = 1, first bump = 2).
  - `store.NewGenerationRepo(db *sql.DB) *GenerationRepo` satisfying `desired.Store` structurally (no import of `desired` from `store`).

- [ ] **Step 1: Move the broker**

Create `internal/desired/doc.go`:

```go
// Package desired is manteion's desired-state materializer (protocol v1,
// docs/plans/2026-10-30-desired-state-protocol.md): per-service generations
// bumped with every input change, the change broker that wakes long-polls and
// SSE subscribers, and the pure function DB → RuleSync document per
// (service, instance) with its content ETag.
package desired
```

Move `internal/api/sse_broker.go` to `internal/desired/broker.go` (`git mv`), renaming `EventBroker` → `Broker`, `NewEventBroker` → `NewBroker`; keep every method body. Replace `internal/api/sse_broker.go` with:

```go
package api

import "manteion-go/internal/desired"

// The SSE broker lives in internal/desired (it also wakes long-polls); these
// aliases keep the handler and its tests on their historical names.
type (
	Event       = desired.Event
	EventBroker = desired.Broker
)

func NewEventBroker() *EventBroker { return desired.NewBroker() }
```

Run: `go test ./internal/api/ -run 'TestEventBroker|TestSSEHandler' -count=1`
Expected: PASS (the api broker tests now exercise the moved code through the aliases).

- [ ] **Step 2: Write the failing Generations tests**

`internal/desired/generations_test.go`:

```go
package desired

import (
	"context"
	"testing"
	"time"
)

func TestGenerations_GetDefaultsToOne(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	if gen, err := g.Get(context.Background(), "cart"); err != nil || gen != 1 {
		t.Fatalf("Get = %d, %v; want 1 (absent row)", gen, err)
	}
}

func TestGenerations_BumpIsMonotonicAndBroadcasts(t *testing.T) {
	broker := NewBroker()
	g := NewGenerations(NewMemoryStore(), broker)
	ch := broker.Subscribe("cart")
	defer broker.Unsubscribe("cart", ch)

	if gen, _ := g.Bump(context.Background(), "cart"); gen != 2 {
		t.Fatalf("first Bump = %d, want 2", gen)
	}
	if gen, _ := g.Bump(context.Background(), "cart"); gen != 3 {
		t.Fatalf("second Bump = %d, want 3", gen)
	}
	select {
	case e := <-ch:
		if e.Type != "rules_changed" || e.Data != `{"generation":2}` {
			t.Fatalf("event = %+v, want rules_changed {\"generation\":2}", e)
		}
	case <-time.After(time.Second):
		t.Fatal("no SSE event after Bump")
	}
}

func TestGenerations_WaitWakesOnBump(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	ctx := context.Background()
	done := make(chan uint64, 1)
	go func() {
		gen, err := g.Wait(ctx, "cart", 1, 5*time.Second)
		if err != nil {
			t.Errorf("Wait: %v", err)
		}
		done <- gen
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if _, err := g.Bump(ctx, "cart"); err != nil {
		t.Fatal(err)
	}
	select {
	case gen := <-done:
		if gen != 2 {
			t.Fatalf("Wait returned %d, want 2", gen)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("Wait took %v after the bump; the waiter was not woken", time.Since(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait never returned after Bump")
	}
}

func TestGenerations_WaitReturnsImmediatelyWhenAlreadyAhead(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	_, _ = g.Bump(context.Background(), "cart") // now 2
	start := time.Now()
	gen, err := g.Wait(context.Background(), "cart", 1, 5*time.Second)
	if err != nil || gen != 2 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Wait = %d, %v in %v; want 2 immediately", gen, err, time.Since(start))
	}
}

func TestGenerations_WaitTimesOutWithoutError(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	start := time.Now()
	gen, err := g.Wait(context.Background(), "cart", 1, 100*time.Millisecond)
	if err != nil || gen != 1 {
		t.Fatalf("Wait = %d, %v; want 1, nil", gen, err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("Wait returned after %v, before the timeout", time.Since(start))
	}
}

func TestGenerations_WaitHonoursContext(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := g.Wait(ctx, "cart", 1, 5*time.Second); err == nil {
		t.Fatal("Wait returned nil error after context cancel")
	}
}

func TestGenerations_BumpTxDoesNotNotifyUntilNotify(t *testing.T) {
	broker := NewBroker()
	g := NewGenerations(NewMemoryStore(), broker)
	ch := broker.Subscribe("cart")
	defer broker.Unsubscribe("cart", ch)
	gen, err := g.BumpTx(context.Background(), nil, "cart")
	if err != nil || gen != 2 {
		t.Fatalf("BumpTx = %d, %v", gen, err)
	}
	select {
	case e := <-ch:
		t.Fatalf("BumpTx broadcast %+v before Notify", e)
	case <-time.After(50 * time.Millisecond):
	}
	g.Notify("cart", gen)
	select {
	case e := <-ch:
		if e.Data != `{"generation":2}` {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify did not broadcast")
	}
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `go test ./internal/desired/ -count=1`
Expected: compile error (`undefined: NewGenerations`, `NewMemoryStore`).

- [ ] **Step 4: Implement Generations and the memory store**

`internal/desired/generations.go`:

```go
package desired

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Store persists per-service generations. Bump increments (creating the row at
// 2 when absent, since absent = 1) inside tx when tx is non-nil.
type Store interface {
	Get(ctx context.Context, service string) (uint64, error)
	Bump(ctx context.Context, tx *sql.Tx, service string) (uint64, error)
}

// waitRecheck bounds how long a waiter can miss a wake-up: Wait re-reads the
// store on this cadence even without a Notify (insurance against a caller
// that bumped in a transaction and forgot to Notify after commit).
const waitRecheck = 2 * time.Second

// Generations is the per-service generation counter plus the wake-up fabric:
// long-polls Wait on it and SSE subscribers receive rules_changed from it.
type Generations struct {
	store  Store
	broker *Broker

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

func NewGenerations(store Store, broker *Broker) *Generations {
	return &Generations{store: store, broker: broker, waiters: map[string]map[chan struct{}]struct{}{}}
}

func (g *Generations) Broker() *Broker { return g.broker }

func (g *Generations) Get(ctx context.Context, service string) (uint64, error) {
	return g.store.Get(ctx, service)
}

// Bump increments the service's generation in its own statement and wakes
// everyone. Use BumpTx + Notify when the bump must commit with other writes.
func (g *Generations) Bump(ctx context.Context, service string) (uint64, error) {
	gen, err := g.store.Bump(ctx, nil, service)
	if err != nil {
		return 0, err
	}
	g.Notify(service, gen)
	return gen, nil
}

// BumpTx increments inside tx and does NOT notify: a waiter woken before the
// commit would re-materialize the old state. Call Notify after Commit.
func (g *Generations) BumpTx(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	return g.store.Bump(ctx, tx, service)
}

// Notify wakes the service's waiters and emits the SSE nudge.
func (g *Generations) Notify(service string, generation uint64) {
	g.mu.Lock()
	for ch := range g.waiters[service] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	g.mu.Unlock()
	g.broker.Broadcast(service, Event{Type: "rules_changed", Data: fmt.Sprintf(`{"generation":%d}`, generation)})
}

// Wait blocks until the service's generation exceeds after, ctx is done, or
// timeout elapses, and returns the generation it last read. A timeout is not
// an error (the caller answers 304); ctx.Err() is.
func (g *Generations) Wait(ctx context.Context, service string, after uint64, timeout time.Duration) (uint64, error) {
	ch := g.subscribe(service)
	defer g.unsubscribe(service, ch)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	recheck := time.NewTicker(waitRecheck)
	defer recheck.Stop()
	for {
		cur, err := g.store.Get(ctx, service)
		if err != nil {
			return 0, err
		}
		if cur > after {
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return cur, ctx.Err()
		case <-deadline.C:
			return cur, nil
		case <-ch:
		case <-recheck.C:
		}
	}
}

func (g *Generations) subscribe(service string) chan struct{} {
	ch := make(chan struct{}, 1)
	g.mu.Lock()
	if g.waiters[service] == nil {
		g.waiters[service] = map[chan struct{}]struct{}{}
	}
	g.waiters[service][ch] = struct{}{}
	g.mu.Unlock()
	return ch
}

func (g *Generations) unsubscribe(service string, ch chan struct{}) {
	g.mu.Lock()
	delete(g.waiters[service], ch)
	if len(g.waiters[service]) == 0 {
		delete(g.waiters, service)
	}
	g.mu.Unlock()
}
```

`internal/desired/memstore.go`: `type memoryStore struct { mu sync.Mutex; gens map[string]uint64 }`; `NewMemoryStore() Store`; `Get` returns `gens[service]` or 1 when absent; `Bump` sets 2 when absent else increments; ignores tx.

- [ ] **Step 5: Run the desired tests to verify they pass**

Run: `go test ./internal/desired/ -count=1 -race`
Expected: PASS

- [ ] **Step 6: Write the failing DB-gated Postgres store test**

`internal/desired/generations_pg_test.go`:

```go
package desired_test

import (
	"context"
	"testing"

	"manteion-go/internal/desired"
	"manteion-go/internal/store"
	"manteion-go/internal/testutil"
)

// TestGenerationRepo_BumpAndGet pins the service_generations SQL: absent = 1,
// first bump = 2, a bump inside a rolled-back transaction leaves no trace, and
// desired.Generations drives it unchanged.
func TestGenerationRepo_BumpAndGet(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := store.NewGenerationRepo(conn)
	g := desired.NewGenerations(repo, desired.NewBroker())

	if gen, err := g.Get(ctx, "cart"); err != nil || gen != 1 {
		t.Fatalf("Get absent = %d, %v; want 1", gen, err)
	}
	if gen, err := g.Bump(ctx, "cart"); err != nil || gen != 2 {
		t.Fatalf("Bump = %d, %v; want 2", gen, err)
	}
	tx, _ := conn.BeginTx(ctx, nil)
	if gen, err := g.BumpTx(ctx, tx, "cart"); err != nil || gen != 3 {
		t.Fatalf("BumpTx = %d, %v; want 3", gen, err)
	}
	_ = tx.Rollback()
	if gen, _ := g.Get(ctx, "cart"); gen != 2 {
		t.Fatalf("Get after rollback = %d, want 2", gen)
	}
}
```

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/desired/ -run TestGenerationRepo -count=1`
Expected: compile error (`store.NewGenerationRepo undefined`).

- [ ] **Step 7: Implement `store.GenerationRepo`**

`internal/store/generation_repo.go`:

```go
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GenerationRepo persists per-service desired-state generations
// (service_generations, migration 12). It satisfies desired.Store.
type GenerationRepo struct{ db *sql.DB }

func NewGenerationRepo(db *sql.DB) *GenerationRepo { return &GenerationRepo{db: db} }

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Get returns the service's generation; a service without a row is at 1.
func (r *GenerationRepo) Get(ctx context.Context, service string) (uint64, error) {
	var gen uint64
	err := r.db.QueryRowContext(ctx, `SELECT generation FROM service_generations WHERE service = $1`, service).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get generation %q: %w", service, err)
	}
	return gen, nil
}

// Bump increments the generation (first bump creates the row at 2) inside tx
// when given, so a rule write and its bump commit or roll back together.
func (r *GenerationRepo) Bump(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	var q rowQuerier = r.db
	if tx != nil {
		q = tx
	}
	var gen uint64
	err := q.QueryRowContext(ctx, `
		INSERT INTO service_generations (service, generation) VALUES ($1, 2)
		ON CONFLICT (service) DO UPDATE
		SET generation = service_generations.generation + 1, updated_at = now()
		RETURNING generation`, service).Scan(&gen)
	if err != nil {
		return 0, fmt.Errorf("bump generation %q: %w", service, err)
	}
	return gen, nil
}
```

- [ ] **Step 8: Run the DB-gated test, then everything**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/desired/ -count=1`
Expected: PASS
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add internal/desired internal/store/generation_repo.go internal/api/sse_broker.go
git commit -m "feat(desired): per-service generations with long-poll waiters; SSE broker moves to internal/desired

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Bumps wired into rule CRUD (same tx), fault configs, kill switch

**Files:**
- Modify: `internal/store/rule_repo.go:12-21, 24-54, 81-127, 215-236`
- Modify: `internal/store/rule_repo_test.go` (add test; constructor call at :59)
- Modify: `internal/api/server.go:33-106`, `internal/api/rule_handler.go:52,138,178,182-232`, `internal/api/fault_config_handler.go:99-103,137`, `internal/api/fault_config_reaper.go:50-54`, `internal/api/sdk_instance_handler.go:176-180`
- Modify: `cmd/manteion/main.go:64, 120-127`; constructor call sites `internal/api/sdk_instance_handler_test.go:505`, `internal/api/sdk_recording_test.go:41`, `internal/orchestrator/orchestrator_test.go:81`

**Interfaces:**
- Consumes: `desired.Generations.BumpTx/Notify/Bump` (Task 2).
- Produces: `store.GenerationBumper interface { BumpTx(ctx, tx *sql.Tx, service string) (uint64, error); Notify(service string, generation uint64) }`; `store.NewRuleRepo(db *sql.DB, gens GenerationBumper) *RuleRepo` (untyped `nil` disables bumping — for tests that do not observe generations); `api.Server.generations *desired.Generations` and `api.NewServer(..., gens *desired.Generations, ...)`.

- [ ] **Step 1: Write the failing DB-gated rule-repo test**

Append to `internal/store/rule_repo_test.go`:

```go
// TestRuleRepo_MutationsBumpServiceGeneration pins desired-state v1: every rule
// mutation bumps the target service's generation in the same transaction, an
// update that moves a rule between services bumps both, and a failed write
// (unknown id) bumps nothing.
func TestRuleRepo_MutationsBumpServiceGeneration(t *testing.T) {
	ctx := context.Background()
	gens := desired.NewGenerations(NewGenerationRepo(testDB), desired.NewBroker())
	repo := NewRuleRepo(testDB, gens)
	tag := fmt.Sprintf("gen-%d", time.Now().UnixNano())
	svcA, svcB := "svca-"+tag, "svcb-"+tag

	spec := &model.FaultSpec{ID: "spec-" + tag, Name: "gen-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now()}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), spec.ID) })

	gen := func(svc string) uint64 {
		t.Helper()
		g, err := gens.Get(ctx, svc)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	a0, b0 := gen(svcA), gen(svcB)

	rule := &model.Rule{ID: "rule-" + tag, Name: "gen-rule", Service: svcA, Enabled: true, Mode: "background",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.Create(ctx, rule); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = repo.Delete(context.Background(), rule.ID) })
	if gen(svcA) != a0+1 {
		t.Fatalf("create: generation(%s) = %d, want %d", svcA, gen(svcA), a0+1)
	}

	rule.Service = svcB // move: both services change
	if err := repo.Update(ctx, rule); err != nil {
		t.Fatalf("update: %v", err)
	}
	if gen(svcA) != a0+2 || gen(svcB) != b0+1 {
		t.Fatalf("move: generations = (%d, %d), want (%d, %d)", gen(svcA), gen(svcB), a0+2, b0+1)
	}

	if err := repo.Update(ctx, &model.Rule{ID: "rule-missing-" + tag, Name: "x", Service: svcA, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID}}); err == nil {
		t.Fatal("update of unknown rule succeeded")
	}
	if gen(svcA) != a0+2 {
		t.Fatalf("failed update bumped %s to %d", svcA, gen(svcA))
	}

	if err := repo.Delete(ctx, rule.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if gen(svcB) != b0+2 {
		t.Fatalf("delete: generation(%s) = %d, want %d", svcB, gen(svcB), b0+2)
	}
}
```

(add `"manteion-go/internal/desired"` to the imports). Update the existing constructor call at `:59` to `NewRuleRepo(testDB, nil)`.

- [ ] **Step 2: Run it to verify it fails**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -run TestRuleRepo_MutationsBumpServiceGeneration -count=1`
Expected: compile error (`too many arguments in call to NewRuleRepo`).

- [ ] **Step 3: Implement the in-tx bumps in `RuleRepo`**

`internal/store/rule_repo.go`:

```go
// GenerationBumper is the slice of desired.Generations the rule repo drives: an
// in-transaction bump and the post-commit wake-up. Nil disables bumping.
type GenerationBumper interface {
	BumpTx(ctx context.Context, tx *sql.Tx, service string) (uint64, error)
	Notify(service string, generation uint64)
}

type RuleRepo struct {
	db   *sql.DB
	gens GenerationBumper
}

func NewRuleRepo(db *sql.DB, gens GenerationBumper) *RuleRepo { return &RuleRepo{db: db, gens: gens} }

// bumpGeneration bumps service inside tx; 0 when generations are disabled.
func (r *RuleRepo) bumpGeneration(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	if r.gens == nil || service == "" {
		return 0, nil
	}
	return r.gens.BumpTx(ctx, tx, service)
}

// notify wakes a service's pollers after the transaction that bumped it committed.
func (r *RuleRepo) notify(service string, generation uint64) {
	if r.gens != nil && generation > 0 {
		r.gens.Notify(service, generation)
	}
}
```

- `Create`: inside the `execTx` closure, after `r.bumpVersion`, `gen, err = r.bumpGeneration(ctx, tx, rule.Service)`; after `execTx` returns nil, `r.notify(rule.Service, gen)`.
- `Update`: inside the closure, first `SELECT service FROM rules WHERE id = $1 FOR UPDATE` into `prev` (`sql.ErrNoRows` → `ErrNotFound`); after the UPDATE and `bumpVersion`, bump `rule.Service`, and when `prev != rule.Service` bump `prev` too; notify both after commit.
- `Delete`: change the statement to `DELETE FROM rules WHERE id = $1 RETURNING service` scanned into `svc` (`sql.ErrNoRows` → `ErrNotFound`, replacing `affectedOrNotFound`); bump `svc`; notify after commit.
- Keep `bumpVersion` (rule_version stays the readiness probe counter) and keep `BumpVersion` for now (the orchestrator still calls it until Task 8).

Update every `NewRuleRepo(` call site: `cmd/manteion/main.go:64` → `store.NewRuleRepo(database, gens)` (create `gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())` just above it); `internal/api/sdk_instance_handler_test.go:505`, `internal/api/sdk_recording_test.go:41`, `internal/orchestrator/orchestrator_test.go:81`, `internal/store/rule_repo_test.go:59` → pass `nil` (Tasks 6 and 8 revisit the api and orchestrator ones).

- [ ] **Step 4: Run the store test**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -count=1`
Expected: PASS

- [ ] **Step 5: Route the API's out-of-band bumps through Generations**

`internal/api/server.go`: add field `generations *desired.Generations` to `Server` and parameter `gens *desired.Generations` to `NewServer` (after `intent`; `intent` is removed in Task 6); set `generations: gens` and `broker: gens.Broker()` (replacing `broker: NewEventBroker()` at :104 — the SSE handler must subscribe to the same broker the bumps broadcast on).

`internal/api/rule_handler.go`: delete the three `s.broadcastRulesChanged(...)` calls (:52, :138, :178) — the repo notifies after commit — and delete `broadcastRulesChanged` (:182-215) and `bumpAndBroadcast` (:217-232); drop the now-unused `encoding/json` import.

`internal/api/fault_config_handler.go`: replace `s.bumpAndBroadcast(ctx, cfg.Service)` (:102, :137) with `s.bumpGeneration(ctx, cfg.Service)`; `internal/api/fault_config_reaper.go:50-54`: `for _, svc := range services { s.bumpGeneration(ctx, svc) }`. Add to `fault_config_handler.go`:

```go
// bumpGeneration bumps a service's desired-state generation after an input
// change made outside the rule repo (fault configs fire/cancel/expire). Pollers
// wake and SSE subscribers are nudged by Generations itself. Best-effort.
func (s *Server) bumpGeneration(ctx context.Context, service string) {
	if _, err := s.generations.Bump(ctx, service); err != nil {
		s.logger.Warn("bump generation failed", "service", service, "error", err)
	}
}
```

`internal/api/sdk_instance_handler.go:176-180`: delete the `s.broadcastRulesChanged(ctx, inst.Service)` call (each `Update` bumps and notifies in the repo); keep the log line.

`cmd/manteion/main.go`: pass `gens` to `api.NewServer`.

- [ ] **Step 6: Verify**

Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean. (`TestHandleInstanceKillSwitch*` still pass: the fake rule store does not bump and the handler no longer needs a broker.)

- [ ] **Step 7: Commit**

```bash
git add internal/store/rule_repo.go internal/store/rule_repo_test.go internal/api internal/orchestrator/orchestrator_test.go cmd/manteion/main.go
git commit -m "feat(desired): rule CRUD bumps the service generation in-tx; fault configs and kill switch bump through Generations

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Materializer with capability filter and ETag

**Files:**
- Create: `internal/desired/capabilities.go`, `internal/desired/capabilities_test.go`, `internal/desired/materializer.go`, `internal/desired/materializer_test.go`

**Interfaces:**
- Consumes: `ruleconv.CompileRules`, `ruleconv.SynthesizeCacheBoxRule`, `ruleconv.FuncResolver`/`FuncCompositionResolver` (`internal/ruleconv/ruleconv.go:39-56, 69-87, 223-247`); `model.FaultConfig` → `atroposdk.FaultRequest` mapping currently at `internal/api/sdk_handler.go:29-50`; `model.CacheBoxRuleContext`.
- Produces:

```go
type Capabilities map[string]bool                 // nil = everything allowed
func CapabilitiesOf(inst *model.SDKInstance) Capabilities // nil inst or empty list → nil
func (c Capabilities) Allows(category string) bool
func RuleCategories(cr atroposdk.CompiledRule) []string   // fault → effective category; composition → every leaf, recursively; cachebox → "cachebox"
func FilterRules(rules []atroposdk.CompiledRule, caps Capabilities) []atroposdk.CompiledRule
func FilterFaults(faults []atroposdk.FaultRequest, caps Capabilities) []atroposdk.FaultRequest

type RuleSource interface { ForService(ctx context.Context, service string) ([]*model.Rule, error) }
type FaultConfigSource interface { ListActiveForService(ctx context.Context, service string) ([]*model.FaultConfig, error) }
type CacheBoxSource interface { ActiveCacheBoxPhaseForService(ctx context.Context, service string) (*model.CacheBoxRuleContext, error) }
type SpecSource interface { SpecResolver(ctx context.Context) model.FaultSpecResolver; CompositionResolver(ctx context.Context) model.CompositionResolver }
type FreezeSource interface { FreezeConfigForService(ctx context.Context, service string) (*atroposdk.DelayRequest, error) }
type ReplaySetSource interface { ReplaySetForService(ctx context.Context, service string) (*model.ReplaySetRef, error) }
type GenerationGetter interface { Get(ctx context.Context, service string) (uint64, error) }

type Materializer struct {
	Generations  GenerationGetter
	Rules        RuleSource
	FaultConfigs FaultConfigSource
	CacheBox     CacheBoxSource
	Specs        SpecSource
	Freeze       FreezeSource    // optional; nil → no freeze_cfg (plan 2 wires the frozen-phase source)
	ReplaySets   ReplaySetSource // optional; nil → no replay_set (plan 2)
}

type Document struct {
	Generation   uint64
	ETag         string
	Rules        []atroposdk.CompiledRule // never nil
	ActiveFaults []atroposdk.FaultRequest // never nil
	FreezeCfg    *atroposdk.DelayRequest
	ReplaySet    *model.ReplaySetRef
}

func (m *Materializer) Materialize(ctx context.Context, service string, inst *model.SDKInstance) (Document, error)
func FaultRequestsOf(configs []*model.FaultConfig) []atroposdk.FaultRequest // composition configs (len(Params)==0) skipped, as today
```

- [ ] **Step 1: Write the failing capability tests**

`internal/desired/capabilities_test.go`:

```go
package desired

import (
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

func fault(cat string) *atroposdk.CompiledFault { return &atroposdk.CompiledFault{Category: cat, FaultType: "x"} }

func TestCapabilitiesOf(t *testing.T) {
	if CapabilitiesOf(nil) != nil {
		t.Error("nil instance must allow everything")
	}
	if CapabilitiesOf(&model.SDKInstance{Capabilities: []string{}}) != nil {
		t.Error("empty list means absent = all")
	}
	c := CapabilitiesOf(&model.SDKInstance{Capabilities: []string{"inline", "cachebox"}})
	if !c.Allows("inline") || !c.Allows("cachebox") || c.Allows("network") || c.Allows("resource") {
		t.Errorf("Allows wrong for %v", c)
	}
	if !Capabilities(nil).Allows("resource") {
		t.Error("nil capabilities must allow every category")
	}
	// Unknown strings are ignored: the sidecar may advertise "netem"; it must
	// neither unlock a known category nor be rejected.
	c = CapabilitiesOf(&model.SDKInstance{Capabilities: []string{"netem", "inline"}})
	if !c.Allows("inline") || c.Allows("network") || c.Allows("netem") {
		t.Errorf("unknown capability string changed the filter: %v", c)
	}
}

func TestRuleCategories(t *testing.T) {
	cases := []struct {
		name string
		rule atroposdk.CompiledRule
		want []string
	}{
		{"empty category is inline", atroposdk.CompiledRule{Fault: fault("")}, []string{"inline"}},
		{"network fault", atroposdk.CompiledRule{Fault: fault("network")}, []string{"network"}},
		{"cachebox", atroposdk.CompiledRule{CacheBox: &atroposdk.CompiledCacheBox{Mode: "replay"}}, []string{"cachebox"}},
		{"nested composition lists every leaf", atroposdk.CompiledRule{Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{
			{Fault: fault("inline")},
			{Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{{Fault: fault("resource")}}}},
		}}}, []string{"inline", "resource"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RuleCategories(tc.rule)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestFilterRulesAndFaults(t *testing.T) {
	rules := []atroposdk.CompiledRule{
		{Name: "inl", Fault: fault("inline")},
		{Name: "net", Fault: fault("network")},
		{Name: "comp-mixed", Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{{Fault: fault("inline")}, {Fault: fault("network")}}}},
		{Name: "cb", CacheBox: &atroposdk.CompiledCacheBox{Mode: "passthrough"}},
	}
	names := func(rs []atroposdk.CompiledRule) []string {
		out := []string{}
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	if got := names(FilterRules(rules, nil)); len(got) != 4 {
		t.Errorf("nil caps dropped rules: %v", got)
	}
	got := names(FilterRules(rules, Capabilities{"inline": true, "cachebox": true}))
	if len(got) != 2 || got[0] != "inl" || got[1] != "cb" {
		t.Errorf("inline+cachebox → %v, want [inl cb] (composition with a network member must go)", got)
	}
	if got := FilterRules(nil, Capabilities{"inline": true}); got == nil {
		t.Error("FilterRules must return an empty, non-nil slice")
	}
	faults := []atroposdk.FaultRequest{{ID: "a", Category: ""}, {ID: "b", Category: "resource"}}
	if got := FilterFaults(faults, Capabilities{"inline": true}); len(got) != 1 || got[0].ID != "a" {
		t.Errorf("FilterFaults = %v, want [a]", got)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/desired/ -run 'TestCapabilitiesOf|TestRuleCategories|TestFilter' -count=1`
Expected: compile error (`undefined: CapabilitiesOf`).

- [ ] **Step 3: Implement `capabilities.go`**

`Capabilities`, `CapabilitiesOf`, `Allows` as in the interface block. `CapabilitiesOf` keeps only the four known strings (`inline`, `network`, `resource`, `cachebox`) from the advertised list and ignores the rest (`netem` etc.); `Allows(category)` is `c == nil || c[category]`, so an unknown string is never a category and never unlocks one. `RuleCategories` walks `Fault` (`EffectiveCategory()`), `Composition` recursively (`Members[i].Fault` / `.Composition`), `CacheBox` → `"cachebox"`. `FilterRules` keeps a rule iff every category in `RuleCategories` is allowed (a rule with no categories is kept; compositions ship as today); `FilterFaults` keeps iff `Allows(f.EffectiveCategory())`. Both return `[]T{}` (non-nil) when nothing passes.

- [ ] **Step 4: Run the capability tests**

Run: `go test ./internal/desired/ -run 'TestCapabilitiesOf|TestRuleCategories|TestFilter' -count=1`
Expected: PASS

- [ ] **Step 5: Write the failing materializer tests**

`internal/desired/materializer_test.go` — fakes are plain structs over maps/slices; the spec source mirrors `fakeFaultRepo` in `internal/api/fault_handler_test.go:110-117`:

```go
package desired

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

type fakeGen struct{ gen uint64 }

func (f *fakeGen) Get(context.Context, string) (uint64, error) { return f.gen, nil }

type fakeRules struct{ rules []*model.Rule }

func (f *fakeRules) ForService(_ context.Context, svc string) ([]*model.Rule, error) {
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == svc {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeConfigs struct{ configs []*model.FaultConfig }

func (f *fakeConfigs) ListActiveForService(context.Context, string) ([]*model.FaultConfig, error) {
	return f.configs, nil
}

type fakeCacheBox struct{ ctx *model.CacheBoxRuleContext }

func (f *fakeCacheBox) ActiveCacheBoxPhaseForService(context.Context, string) (*model.CacheBoxRuleContext, error) {
	return f.ctx, nil
}

type fakeSpecs struct {
	specs map[string]*model.FaultSpec
	comps map[string]*model.FaultComposition
}

func (f *fakeSpecs) SpecResolver(context.Context) model.FaultSpecResolver {
	return func(id string) *model.FaultSpec { return f.specs[id] }
}
func (f *fakeSpecs) CompositionResolver(context.Context) model.CompositionResolver {
	return func(id string) *model.FaultComposition { return f.comps[id] }
}

type fakeFreeze struct{ cfg *atroposdk.DelayRequest }

func (f *fakeFreeze) FreezeConfigForService(context.Context, string) (*atroposdk.DelayRequest, error) {
	return f.cfg, nil
}

func rule(id, svc, typ, ref string) *model.Rule {
	r := &model.Rule{ID: id, Name: id, Service: svc, Enabled: true, Mode: "background"}
	switch typ {
	case "fault_spec":
		r.Action = model.RuleAction{Type: typ, FaultSpecID: ref}
	case "fault_composition":
		r.Action = model.RuleAction{Type: typ, FaultCompID: ref}
	case "cachebox":
		r.Action = model.RuleAction{Type: typ, CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}}
	}
	return r
}

func newTestMaterializer(gen uint64) (*Materializer, *fakeGen) {
	g := &fakeGen{gen: gen}
	m := &Materializer{
		Generations: g,
		Rules: &fakeRules{rules: []*model.Rule{
			rule("r-inline", "cart", "fault_spec", "spec-inline"),
			rule("r-net", "cart", "fault_spec", "spec-net"),
			rule("r-cpu", "cart", "fault_spec", "spec-cpu"),
			rule("r-comp", "cart", "fault_composition", "comp-mixed"),
			rule("r-cb", "cart", "cachebox", ""),
			rule("r-other", "checkout", "fault_spec", "spec-inline"),
		}},
		FaultConfigs: &fakeConfigs{configs: []*model.FaultConfig{
			{ID: "fc-inline", Service: "cart", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1s"}`)},
			{ID: "fc-mem", Service: "cart", Category: "resource", FaultType: "memory", Params: json.RawMessage(`{"target_bytes":1}`)},
			{ID: "fc-comp", Service: "cart", Category: "inline"}, // composition config: no params → skipped
		}},
		CacheBox: &fakeCacheBox{ctx: &model.CacheBoxRuleContext{ExperimentID: "exp-1", PhaseID: "phase-iso", Mode: "replay", KeyStrategy: "canonical_v2", StrategyVersion: 2}},
		Specs: &fakeSpecs{
			specs: map[string]*model.FaultSpec{
				"spec-inline": {ID: "spec-inline", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"100ms"}`)},
				"spec-net":    {ID: "spec-net", Category: "network", FaultType: "blackhole", Network: &model.NetworkEnvelope{Target: "redis", Direction: "upstream"}},
				"spec-cpu":    {ID: "spec-cpu", Category: "resource", FaultType: "cpu", Params: json.RawMessage(`{"target_load":0.5}`)},
			},
			comps: map[string]*model.FaultComposition{
				"comp-mixed": {ID: "comp-mixed", Name: "mixed", ExecutionMode: model.ExecutionParallel,
					Members: []model.FaultCompositionMember{{FaultSpecID: "spec-inline"}, {FaultSpecID: "spec-net"}}},
			},
		},
		Freeze: &fakeFreeze{cfg: &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.4}},
	}
	return m, g
}

func inst(caps ...string) *model.SDKInstance {
	return &model.SDKInstance{ID: "i1", Service: "cart", Capabilities: caps}
}

func ruleNames(d Document) string {
	var n []string
	for _, r := range d.Rules {
		n = append(n, r.Name)
	}
	return strings.Join(n, ",")
}

func TestMaterialize_UnfilteredForNilInstance(t *testing.T) {
	m, _ := newTestMaterializer(5)
	d, err := m.Materialize(context.Background(), "cart", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := ruleNames(d); got != "r-inline,r-net,r-cpu,r-comp,r-cb,cachebox:replay:phase-iso" {
		t.Errorf("rules = %s", got)
	}
	if len(d.ActiveFaults) != 2 || d.ActiveFaults[0].ID != "fc-inline" || d.ActiveFaults[1].ID != "fc-mem" {
		t.Errorf("active faults = %+v", d.ActiveFaults)
	}
	if d.FreezeCfg == nil || d.FreezeCfg.Mu != 1.5 {
		t.Errorf("freeze_cfg = %+v", d.FreezeCfg)
	}
	if d.Generation != 5 || !strings.HasPrefix(d.ETag, "5-") || len(d.ETag) != len("5-")+16 {
		t.Errorf("generation/etag = %d/%q", d.Generation, d.ETag)
	}
}

func TestMaterialize_FiltersByCapabilities(t *testing.T) {
	m, _ := newTestMaterializer(5)
	cases := []struct {
		caps       []string
		wantRules  string
		wantFaults int
		wantFreeze bool
	}{
		{[]string{"inline", "cachebox"}, "r-inline,r-cb,cachebox:replay:phase-iso", 1, true},
		{[]string{"inline", "network", "resource"}, "r-inline,r-net,r-cpu,r-comp", 2, false},
		{[]string{"inline"}, "r-inline", 1, false},
		{[]string{}, "r-inline,r-net,r-cpu,r-comp,r-cb,cachebox:replay:phase-iso", 2, true},
	}
	for _, tc := range cases {
		d, err := m.Materialize(context.Background(), "cart", inst(tc.caps...))
		if err != nil {
			t.Fatal(err)
		}
		if got := ruleNames(d); got != tc.wantRules {
			t.Errorf("caps %v: rules = %s, want %s", tc.caps, got, tc.wantRules)
		}
		if len(d.ActiveFaults) != tc.wantFaults || (d.FreezeCfg != nil) != tc.wantFreeze {
			t.Errorf("caps %v: faults=%d freeze=%v", tc.caps, len(d.ActiveFaults), d.FreezeCfg != nil)
		}
	}
}

func TestMaterialize_ETagStableAndContentSensitive(t *testing.T) {
	m, g := newTestMaterializer(5)
	ctx := context.Background()
	a, _ := m.Materialize(ctx, "cart", inst("inline"))
	b, _ := m.Materialize(ctx, "cart", inst("inline"))
	if a.ETag != b.ETag {
		t.Fatalf("same inputs, different ETags: %s vs %s", a.ETag, b.ETag)
	}
	g.gen = 6 // another instance's capabilities changed: generation moves, document does not
	c, _ := m.Materialize(ctx, "cart", inst("inline"))
	if !strings.HasPrefix(c.ETag, "6-") || c.ETag[2:] != a.ETag[2:] {
		t.Fatalf("hash must survive a generation move: %s vs %s", a.ETag, c.ETag)
	}
	d, _ := m.Materialize(ctx, "cart", inst("inline", "network"))
	if d.ETag[2:] == c.ETag[2:] {
		t.Fatal("different content, same hash")
	}
}

func TestMaterialize_EmptyIsNeverNull(t *testing.T) {
	m, _ := newTestMaterializer(1)
	d, err := m.Materialize(context.Background(), "nobody", inst("inline"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Rules == nil || d.ActiveFaults == nil {
		t.Fatalf("Rules/ActiveFaults nil: %+v", d)
	}
}

func TestMaterialize_CompileErrorSurfaces(t *testing.T) {
	m, _ := newTestMaterializer(1)
	m.Rules = &fakeRules{rules: []*model.Rule{rule("r-bad", "cart", "fault_spec", "spec-missing")}}
	if _, err := m.Materialize(context.Background(), "cart", nil); err == nil {
		t.Fatal("expected a compile error for an unresolvable spec")
	}
}
```

(`model.FaultCompositionMember{FaultSpecID, ChildCompositionID, Direction}` is the member type, `internal/model/fault.go:151-156`.)

- [ ] **Step 6: Run to verify they fail**

Run: `go test ./internal/desired/ -run TestMaterialize -count=1`
Expected: compile error (`undefined: Materializer`).

- [ ] **Step 7: Implement `materializer.go`**

`Materialize` body (the order matters: read the generation first so a document never claims a generation newer than its data):

```go
func (m *Materializer) Materialize(ctx context.Context, service string, inst *model.SDKInstance) (Document, error) {
	gen, err := m.Generations.Get(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("generation: %w", err)
	}
	rules, err := m.Rules.ForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("rules: %w", err)
	}
	compiled, err := ruleconv.CompileRules(rules,
		&ruleconv.FuncResolver{Fn: m.Specs.SpecResolver(ctx)},
		&ruleconv.FuncCompositionResolver{Fn: m.Specs.CompositionResolver(ctx)})
	if err != nil {
		return Document{}, fmt.Errorf("compile rules: %w", err)
	}
	cb, err := m.CacheBox.ActiveCacheBoxPhaseForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("cache-box role: %w", err)
	}
	if cb != nil {
		compiled = append(compiled, ruleconv.SynthesizeCacheBoxRule(*cb))
	}
	configs, err := m.FaultConfigs.ListActiveForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("active faults: %w", err)
	}
	doc := Document{Generation: gen}
	caps := CapabilitiesOf(inst)
	doc.Rules = FilterRules(compiled, caps)
	doc.ActiveFaults = FilterFaults(FaultRequestsOf(configs), caps)
	if caps.Allows("cachebox") {
		if m.Freeze != nil {
			if doc.FreezeCfg, err = m.Freeze.FreezeConfigForService(ctx, service); err != nil {
				return Document{}, fmt.Errorf("freeze config: %w", err)
			}
		}
		if m.ReplaySets != nil {
			if doc.ReplaySet, err = m.ReplaySets.ReplaySetForService(ctx, service); err != nil {
				return Document{}, fmt.Errorf("replay set: %w", err)
			}
		}
	}
	doc.ETag, err = etag(gen, doc)
	return doc, err
}

// etag is "<generation>-<sha256 hex[:16]>" over the canonical JSON of the
// document body (encoding/json is deterministic for these types: struct
// fields in declaration order, map keys sorted, JSONB params as Postgres
// normalized them).
func etag(gen uint64, d Document) (string, error) {
	body, err := json.Marshal(struct {
		Rules        []atroposdk.CompiledRule `json:"rules"`
		ActiveFaults []atroposdk.FaultRequest `json:"active_faults"`
		FreezeCfg    *atroposdk.DelayRequest  `json:"freeze_cfg,omitempty"`
		ReplaySet    *model.ReplaySetRef      `json:"replay_set,omitempty"`
	}{d.Rules, d.ActiveFaults, d.FreezeCfg, d.ReplaySet})
	if err != nil {
		return "", fmt.Errorf("canonical json: %w", err)
	}
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%d-%s", gen, hex.EncodeToString(sum[:])[:16]), nil
}
```

`FaultRequestsOf` is the loop from `internal/api/sdk_handler.go:29-50` (skip `len(c.Params) == 0`, copy ID/Category/FaultType/durations/Params, map `Network`), returning `[]atroposdk.FaultRequest{}` when empty. The freeze config and replay set are gated on `cachebox` because an instance without a cache-box has nowhere to install a delay source or a replay set.

- [ ] **Step 8: Run the desired tests, then everything**

Run: `go test ./internal/desired/ -count=1 -race`
Expected: PASS
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add internal/desired
git commit -m "feat(desired): materializer — phase-aware rules, active faults, cache-box rule, capability filter, content ETag

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Register responds with the materialized document

**Files:**
- Create: `internal/api/sdk_wire.go`, `internal/api/sdk_desired_handler_test.go`
- Modify: `internal/api/sdk_handler.go:65-120`, `internal/api/server.go` (fields + `NewServer`), `internal/api/sdk_instance_handler.go:18-20`
- Modify: `internal/atrocontrol/intent.go`, `internal/atrocontrol/controller.go:55`
- Modify: `cmd/manteion/main.go`

**Interfaces:**
- Consumes: `desired.Materializer`, `desired.Generations`, `store.SDKRepo.Register/Get`.
- Produces:

```go
// internal/api/sdk_wire.go
// TODO(desired-state): swap for atroposdk.RuleSync / RegisterResponse once atropos-go adds Etag and ReplaySet.
type RuleSyncWire struct {
	Version      uint64                   `json:"version"` // the generation, under the historical name
	Etag         string                   `json:"etag"`
	Rules        []atroposdk.CompiledRule `json:"rules"`         // null = "no change" (degraded register only)
	ActiveFaults []atroposdk.FaultRequest `json:"active_faults"` // never null
	FreezeCfg    *atroposdk.DelayRequest  `json:"freeze_cfg,omitempty"`
	ReplaySet    *model.ReplaySetRef      `json:"replay_set,omitempty"`
}
type RegisterResponseWire struct {
	Status string `json:"status"`
	RuleSyncWire
}
func ruleSyncFrom(doc desired.Document) RuleSyncWire

// internal/api/sdk_instance_handler.go — narrow registry view (same object as s.sdk)
type sdkRegistry interface {
	sdkInstanceReader
	Register(ctx context.Context, inst *model.SDKInstance) error
	RecordPoll(ctx context.Context, id string, observedGeneration uint64) error
	RecordStatus(ctx context.Context, id string, st model.ObservedStatus) error
}
type desiredMaterializer interface {
	Materialize(ctx context.Context, service string, inst *model.SDKInstance) (desired.Document, error)
}
// Server gains: registry sdkRegistry; materializer desiredMaterializer
// NewServer(..., gens *desired.Generations, mat *desired.Materializer, ...)
func sameCapabilities(a, b []string) bool // set equality; nil and [] are equal
```

- `(*atrocontrol.IntentTracker).FreezeConfigForService(ctx, service) (*atroposdk.DelayRequest, error)` — the plan-1 `desired.FreezeSource`: returns the intent's `FreezeCfg` (nil when none). `(*Controller).IntentReader()` now returns `*IntentTracker`; the `IntentReader` interface type is deleted (its only user was the api). Plan 2 deletes the tracker and swaps the source for the frozen-phase one.

- [ ] **Step 1: Write the failing register tests**

`internal/api/sdk_desired_handler_test.go` (fakes + helpers used by Tasks 5-7):

```go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeRegistry is the DB-free sdk_instances table for the desired-state
// handlers: Register upserts, RecordPoll/RecordStatus mutate the row in place.
type fakeRegistry struct {
	mu    sync.Mutex
	byID  map[string]*model.SDKInstance
	polls []polled
	err   error
}

type polled struct {
	id  string
	gen uint64
}

func newFakeRegistry(insts ...*model.SDKInstance) *fakeRegistry {
	f := &fakeRegistry{byID: map[string]*model.SDKInstance{}}
	for _, i := range insts {
		f.byID[i.ID] = i
	}
	return f
}

func (f *fakeRegistry) Get(_ context.Context, id string) (*model.SDKInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if i, ok := f.byID[id]; ok {
		cp := *i
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeRegistry) Register(_ context.Context, inst *model.SDKInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	cp := *inst
	cp.ObservedGeneration, cp.ObservedEtag, cp.ReplaySetID, cp.ReplaySetState = 0, "", "", ""
	f.byID[inst.ID] = &cp
	return nil
}

func (f *fakeRegistry) RecordPoll(_ context.Context, id string, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls = append(f.polls, polled{id, gen})
	i, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	i.ObservedGeneration = gen
	return nil
}

func (f *fakeRegistry) RecordStatus(_ context.Context, id string, st model.ObservedStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	i.ObservedGeneration, i.ObservedEtag = st.AppliedGeneration, st.Etag
	i.ObservedUnsupported, i.ObservedErrors = st.Unsupported, st.Errors
	if st.ReplaySet != nil {
		i.ReplaySetID, i.ReplaySetState = st.ReplaySet.ID, st.ReplaySet.State
	}
	return nil
}

// Desired-state fixtures for "cartservice": an inline rule, a network rule, an
// active inline fault config, an active resource fault config, and a running
// replay phase (synthesized cache-box rule).
type fakeDesiredRules struct{ rules []*model.Rule }

func (f *fakeDesiredRules) ForService(_ context.Context, svc string) ([]*model.Rule, error) {
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == svc {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeDesiredConfigs struct{ configs []*model.FaultConfig }

func (f *fakeDesiredConfigs) ListActiveForService(_ context.Context, svc string) ([]*model.FaultConfig, error) {
	var out []*model.FaultConfig
	for _, c := range f.configs {
		if c.Service == svc {
			out = append(out, c)
		}
	}
	return out, nil
}

type fakeDesiredCacheBox struct{ byService map[string]*model.CacheBoxRuleContext }

func (f *fakeDesiredCacheBox) ActiveCacheBoxPhaseForService(_ context.Context, svc string) (*model.CacheBoxRuleContext, error) {
	return f.byService[svc], nil
}

type desiredFixture struct {
	gens  *desired.Generations
	mat   *desired.Materializer
	reg   *fakeRegistry
	rules *fakeDesiredRules
}

func newDesiredFixture(insts ...*model.SDKInstance) *desiredFixture {
	specs := newFakeFaultRepo() // fault_handler_test.go
	specs.specs["spec-inline"] = &model.FaultSpec{ID: "spec-inline", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"100ms"}`)}
	specs.specs["spec-net"] = &model.FaultSpec{ID: "spec-net", Category: "network", FaultType: "blackhole", Network: &model.NetworkEnvelope{Target: "redis", Direction: "upstream"}}
	mk := func(id, spec string) *model.Rule {
		return &model.Rule{ID: id, Name: id, Service: "cartservice", Enabled: true, Mode: "background", Priority: 10,
			Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec}}
	}
	gens := desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())
	rules := &fakeDesiredRules{rules: []*model.Rule{mk("r-inline", "spec-inline"), mk("r-net", "spec-net")}}
	mat := &desired.Materializer{
		Generations: gens, Rules: rules, Specs: specs,
		FaultConfigs: &fakeDesiredConfigs{configs: []*model.FaultConfig{
			{ID: "fc-inline", Service: "cartservice", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1s"}`)},
			{ID: "fc-cpu", Service: "cartservice", Category: "resource", FaultType: "cpu", Params: json.RawMessage(`{"target_load":0.5}`)},
		}},
		CacheBox: &fakeDesiredCacheBox{byService: map[string]*model.CacheBoxRuleContext{
			"cartservice": {ExperimentID: "exp-1", PhaseID: "phase-iso", Mode: "replay", KeyStrategy: "canonical_v2", StrategyVersion: 2},
		}},
	}
	return &desiredFixture{gens: gens, mat: mat, reg: newFakeRegistry(insts...), rules: rules}
}

func (fx *desiredFixture) server() *Server {
	return &Server{registry: fx.reg, sdkInstances: fx.reg, generations: fx.gens, materializer: fx.mat,
		broker: fx.gens.Broker(), logger: discardLogger()}
}

func serveJSON(t *testing.T, s *Server, method, path string, body any, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	mux := http.NewServeMux()
	s.routes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func ruleNamesOf(t *testing.T, body []byte) []string {
	t.Helper()
	var sync RuleSyncWire
	if err := json.Unmarshal(body, &sync); err != nil {
		t.Fatalf("decode RuleSyncWire: %v (%s)", err, body)
	}
	out := []string{}
	for _, r := range sync.Rules {
		out = append(out, r.Name)
	}
	return out
}

func TestHandleRegister_RespondsWithMaterializedDocument(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	body := map[string]any{"id": "cart-1", "service": "cartservice", "address": "10.0.0.5:9090",
		"poll_interval_ms": 10000, "host": "sidecar", "capabilities": []string{"inline", "cachebox"}}
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", body, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	var resp RegisterResponseWire
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "registered" || resp.Version != 1 || !strings.HasPrefix(resp.Etag, "1-") {
		t.Errorf("status/version/etag = %s/%d/%s", resp.Status, resp.Version, resp.Etag)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,cachebox:replay:phase-iso" {
		t.Errorf("rules = %v, want the inline rule and the cache-box rule only (network filtered)", got)
	}
	if len(resp.ActiveFaults) != 1 || resp.ActiveFaults[0].ID != "fc-inline" {
		t.Errorf("active_faults = %+v, want [fc-inline] (resource filtered)", resp.ActiveFaults)
	}
	stored, _ := fx.reg.Get(context.Background(), "cart-1")
	if stored.Host != "sidecar" || len(stored.Capabilities) != 2 {
		t.Errorf("persisted host/capabilities = %q/%v", stored.Host, stored.Capabilities)
	}
	if gen, _ := fx.gens.Get(context.Background(), "cartservice"); gen != 1 {
		t.Errorf("a fresh registration bumped the generation to %d", gen)
	}
}

func TestHandleRegister_CapabilityChangeBumps(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline", "cachebox"}})
	s := fx.server()
	ctx := context.Background()
	register := func(caps []string) {
		t.Helper()
		w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
			map[string]any{"id": "cart-1", "service": "cartservice", "address": "a", "capabilities": caps}, nil)
		if w.Code != http.StatusCreated {
			t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
		}
	}
	register([]string{"cachebox", "inline"}) // same set, other order
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 1 {
		t.Fatalf("re-ordered capabilities bumped to %d", gen)
	}
	register([]string{"inline", "cachebox", "resource"})
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 2 {
		t.Fatalf("changed capabilities: generation = %d, want 2", gen)
	}
	register(nil) // absent = all: a change again
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 3 {
		t.Fatalf("absent capabilities: generation = %d, want 3", gen)
	}
}

type failingMaterializer struct{}

func (failingMaterializer) Materialize(context.Context, string, *model.SDKInstance) (desired.Document, error) {
	return desired.Document{}, errors.New("compile rules: spec missing")
}

func TestHandleRegister_MaterializeFailureIsDegraded201(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	s.materializer = failingMaterializer{}
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-1", "service": "cartservice", "address": "a"}, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (a broken rule must not crash-loop the pod); body = %s", w.Code, w.Body.String())
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(w.Body.Bytes(), &raw)
	if string(raw["rules"]) != "null" || string(raw["active_faults"]) != "[]" || string(raw["version"]) != "0" {
		t.Errorf("degraded body = %s, want rules:null active_faults:[] version:0", w.Body.String())
	}
	if _, err := fx.reg.Get(context.Background(), "cart-1"); err != nil {
		t.Error("registration must be persisted even when materialization fails")
	}
}

func TestHandleRegister_ValidationAndDBErrors(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	if w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", map[string]any{"service": "cartservice"}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("missing id: status = %d, want 400", w.Code)
	}
	if w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", strings.NewReader("{"), nil); w.Code != http.StatusBadRequest {
		t.Errorf("bad json: status = %d, want 400", w.Code)
	}
}
```

(For the bad-JSON case use a raw `*strings.Reader` body — adapt `serveJSON` to accept an `io.Reader` when the body is one.) `atroposdk` is imported for `RuleSyncWire` decoding of `ActiveFaults`.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/api/ -run TestHandleRegister -count=1`
Expected: compile error (`unknown field registry in struct literal`, `undefined: RuleSyncWire`).

- [ ] **Step 3: Implement the wire structs, the registry interface and the handler**

`internal/api/sdk_wire.go` as in the Interfaces block, plus:

```go
func ruleSyncFrom(doc desired.Document) RuleSyncWire {
	return RuleSyncWire{Version: doc.Generation, Etag: doc.ETag, Rules: doc.Rules,
		ActiveFaults: doc.ActiveFaults, FreezeCfg: doc.FreezeCfg, ReplaySet: doc.ReplaySet}
}
```

`internal/api/server.go`: fields `registry sdkRegistry // same as sdk`, `materializer desiredMaterializer`; `NewServer` takes `mat *desired.Materializer` after `gens` and sets `registry: sdk, materializer: mat`.

`internal/api/sdk_handler.go`, replace `handleRegister` (:65-120):

```go
// handleRegister registers (or re-registers) an SDK instance and answers with
// its materialized desired state (desired-state protocol v1 §2 Register).
//
// @Summary      Register SDK instance
// @Description  Registers an atropos-go SDK process (in-process embed or atroposd sidecar)
// @Description  with its host kind and fault capabilities. The 201 response carries
// @Description  status="registered" plus the instance's materialized desired state —
// @Description  the same RuleSync shape a 200 poll serves (version = generation, etag,
// @Description  rules, active_faults, freeze_cfg, replay_set), already filtered by the
// @Description  advertised capabilities — so the SDK converges before its first poll.
// @Description  Registering with capabilities different from the stored ones bumps the
// @Description  service generation. If the desired state cannot be materialized the
// @Description  registration still succeeds and rules is null ("no change").
// @Tags         sdk
// @Accept       json
// @Produce      json
// @Param        instance  body      model.SDKInstance         true  "SDK instance metadata (id, service, address, poll_interval_ms, routes, host, capabilities)"
// @Success      201       {object}  api.RegisterResponseWire
// @Failure      400       {object}  api.ErrorResponse  "invalid JSON or registration error"
// @Router       /sdk/register [post]
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var inst model.SDKInstance
	if err := readJSON(r, &inst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	ctx := r.Context()
	prev, err := s.registry.Get(ctx, inst.ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.logger.Warn("register: read previous registration failed", "id", inst.ID, "error", err)
	}
	if err := s.registry.Register(ctx, &inst); err != nil {
		s.logger.Error("sdk register failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("sdk registered", "id", inst.ID, "service", inst.Service, "host", inst.Host, "capabilities", inst.Capabilities)

	// A changed capability set changes this instance's document; bump so its
	// long-poll (and anyone else's) re-materializes. A fresh registration
	// changes nobody's document: the response below already carries it.
	if prev != nil && !sameCapabilities(prev.Capabilities, inst.Capabilities) {
		s.bumpGeneration(ctx, inst.Service)
	}

	resp := RegisterResponseWire{Status: "registered"}
	doc, err := s.materializer.Materialize(ctx, inst.Service, &inst)
	if err != nil {
		// Never fail registration on a data error: the SDK would refuse to
		// start. rules:null is the SDK's "no change"; the poll reports the
		// error as a 500 until the rule is fixed.
		s.logger.Error("register: materialize desired state failed", "service", inst.Service, "error", err)
		resp.ActiveFaults = []atroposdk.FaultRequest{}
		writeJSON(w, http.StatusCreated, resp)
		return
	}
	resp.RuleSyncWire = ruleSyncFrom(doc)
	writeJSON(w, http.StatusCreated, resp)
}
```

`sameCapabilities(a, b []string) bool`: build sets, compare sizes and membership (`nil` and `[]` compare equal). Delete the `s.intent` read in `activeFaultsForService` only in Task 6; for now `handleRegister` no longer calls it.

`internal/atrocontrol/intent.go`: add

```go
// FreezeConfigForService is the plan-1 desired.FreezeSource: the freeze
// config the orchestrator last pushed for the service. Plan 2 replaces it
// with the frozen phase's config and deletes the tracker.
func (t *IntentTracker) FreezeConfigForService(_ context.Context, service string) (*atroposdk.DelayRequest, error) {
	if i, ok := t.Get(service); ok {
		return i.FreezeCfg, nil
	}
	return nil, nil
}
```

delete the `IntentReader` interface; `controller.go:55` becomes `func (c *Controller) IntentReader() *IntentTracker { return c.intent }`.

`cmd/manteion/main.go`: move `faultConfigRepo := store.NewFaultConfigRepo(database)` up next to the other repos; build

```go
	mat := &desired.Materializer{
		Generations: gens, Rules: ruleRepo, FaultConfigs: faultConfigRepo,
		CacheBox: experimentRepo, Specs: faultRepo,
		Freeze: controller.IntentReader(), // plan 2: the frozen phase's config
	}
```

and pass `gens, mat` to `api.NewServer` (keep passing `controller.IntentReader()` for the `intent` param until Task 6).

- [ ] **Step 4: Run the register tests, then everything**

Run: `go test ./internal/api/ -run TestHandleRegister -count=1`
Expected: PASS
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add internal/api internal/atrocontrol cmd/manteion/main.go
git commit -m "feat(api): register persists host/capabilities and answers with the materialized desired state

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Long-polling `GET /api/v1/sdk/rules` with ETag and generation headers

**Files:**
- Modify: `internal/api/sdk_handler.go:1-63, 171-322` (delete `activeFaultsForService`, `cacheBoxEffectiveVersion`; rewrite `handlePollRules`)
- Modify: `internal/api/server.go` (delete `intent` field + param), `cmd/manteion/main.go`
- Modify: `internal/api/sdk_desired_handler_test.go` (add poll tests), `internal/api/sdk_instance_handler_test.go:489-700`, `internal/api/sdk_recording_test.go:41-63`

**Interfaces:**
- Consumes: `desired.Generations.Wait`, `desiredMaterializer`, `sdkRegistry.RecordPoll`.
- Produces: the poll contract; helpers `parseWait(q string) time.Duration` (0 ≤ wait ≤ 60 s; garbage → 0), `etagMatches(ifNoneMatch, etag string) bool` (strips `W/` and quotes; comma-separated lists are matched element-wise), `const maxPollWait = 60 * time.Second`.

- [ ] **Step 1: Write the failing poll tests**

Append to `internal/api/sdk_desired_handler_test.go`:

```go
func pollWith(t *testing.T, s *Server, query string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	return serveJSON(t, s, http.MethodGet, "/api/v1/sdk/rules?"+query, nil, hdr)
}

func TestHandlePollRules_200CarriesEtagAndGeneration(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline"}})
	s := fx.server()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=0", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	etag := w.Header().Get("ETag")
	if !strings.HasPrefix(etag, `"1-`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("ETag = %q, want a quoted 1-<hash>", etag)
	}
	if got := w.Header().Get("X-Atropos-Generation"); got != "1" {
		t.Errorf("X-Atropos-Generation = %q, want 1", got)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline" {
		t.Errorf("rules = %v, want [r-inline] (network and cache-box filtered for an inline-only host)", got)
	}
	var sync RuleSyncWire
	_ = json.Unmarshal(w.Body.Bytes(), &sync)
	if `"`+sync.Etag+`"` != etag || sync.Version != 1 {
		t.Errorf("body etag/version = %s/%d, header = %s", sync.Etag, sync.Version, etag)
	}
	if len(fx.reg.polls) != 1 || fx.reg.polls[0] != (polled{"cart-1", 0}) {
		t.Errorf("observed generation not recorded before answering: %+v", fx.reg.polls)
	}
}

func TestHandlePollRules_304OnMatchingEtag(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	first := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil)
	etag := first.Header().Get("ETag")
	for _, inm := range []string{etag, "W/" + etag, strings.Trim(etag, `"`)} {
		w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1", map[string]string{"If-None-Match": inm})
		if w.Code != http.StatusNotModified {
			t.Errorf("If-None-Match %s: status = %d, want 304", inm, w.Code)
		}
		if w.Header().Get("ETag") != etag || w.Header().Get("X-Atropos-Generation") != "1" {
			t.Errorf("304 headers = %v", w.Header())
		}
		if w.Body.Len() != 0 {
			t.Errorf("304 carried a body: %s", w.Body.String())
		}
	}
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1", map[string]string{"If-None-Match": `"garbage"`})
	if w.Code != http.StatusOK {
		t.Errorf("foreign ETag: status = %d, want 200", w.Code)
	}
}

func TestHandlePollRules_LongPollWakesOnBump(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")

	go func() {
		time.Sleep(100 * time.Millisecond)
		fx.rules.rules = fx.rules.rules[:1] // the network rule is deleted...
		_, _ = fx.gens.Bump(context.Background(), "cartservice") // ...and the repo bumped
	}()
	start := time.Now()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=30", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after the bump; body = %s", w.Code, w.Body.String())
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("long-poll took %v; it must return on the bump, not on wait", time.Since(start))
	}
	if w.Header().Get("X-Atropos-Generation") != "2" || !strings.HasPrefix(w.Header().Get("ETag"), `"2-`) {
		t.Errorf("headers after bump = %v", w.Header())
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,cachebox:replay:phase-iso" {
		t.Errorf("rules = %v, want the post-bump set", got)
	}
}

func TestHandlePollRules_LongPollTimesOutWith304(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	start := time.Now()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=1", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusNotModified || time.Since(start) < time.Second {
		t.Fatalf("status = %d after %v, want 304 after ~1s", w.Code, time.Since(start))
	}
	if w.Header().Get("X-Atropos-Generation") != "1" {
		t.Errorf("X-Atropos-Generation = %q", w.Header().Get("X-Atropos-Generation"))
	}
}

func TestHandlePollRules_GenerationMovesButDocumentUnchanged(t *testing.T) {
	// Another instance's capabilities changed: the generation moves, this
	// instance's content does not — the ETag still differs (generation prefix),
	// so it gets a 200 that it can recognise as same-content by the hash.
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	_, _ = fx.gens.Bump(context.Background(), "cartservice")
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=1", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (generation prefix changed)", w.Code)
	}
	if got := w.Header().Get("ETag"); got[len(`"2-`):] != etag[len(`"1-`):] {
		t.Errorf("hash changed although the document did not: %s vs %s", got, etag)
	}
}

func TestHandlePollRules_UnknownInstanceStillServed(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	w := pollWith(t, s, "service=cartservice&instance_id=gone-1&version=0", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for an unknown instance (unfiltered document)", w.Code)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); len(got) != 3 {
		t.Errorf("rules = %v, want all 3 (no capability filter without a registration)", got)
	}
}

func TestHandlePollRules_Validation(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	if w := pollWith(t, s, "instance_id=cart-1", nil); w.Code != http.StatusBadRequest {
		t.Errorf("missing service: status = %d, want 400", w.Code)
	}
	if got := parseWait("61"); got != maxPollWait {
		t.Errorf("parseWait(61) = %v, want clamp to %v", got, maxPollWait)
	}
	if got := parseWait("-3"); got != 0 {
		t.Errorf("parseWait(-3) = %v, want 0", got)
	}
	if got := parseWait("abc"); got != 0 {
		t.Errorf("parseWait(abc) = %v, want 0", got)
	}
	s.materializer = failingMaterializer{}
	if w := pollWith(t, s, "service=cartservice", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("materialize failure: status = %d, want 500", w.Code)
	}
}

// Review Focus 1: cmd/manteion sets WriteTimeout=30s; a held poll longer than
// that must extend its write deadline or the 200 is written into a dead
// connection. A real server with a 200ms WriteTimeout and a 2s hold proves it.
func TestHandlePollRules_HoldSurvivesServerWriteTimeout(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	mux := http.NewServeMux()
	s.routes(mux)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.WriteTimeout = 200 * time.Millisecond
	srv.Start()
	t.Cleanup(srv.Close)

	first, err := http.Get(srv.URL + "/api/v1/sdk/rules?service=cartservice&instance_id=cart-1")
	if err != nil {
		t.Fatal(err)
	}
	first.Body.Close()
	etag := first.Header.Get("ETag")

	go func() {
		time.Sleep(700 * time.Millisecond)
		_, _ = fx.gens.Bump(context.Background(), "cartservice")
	}()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/sdk/rules?service=cartservice&instance_id=cart-1&version=1&wait=5", nil)
	req.Header.Set("If-None-Match", etag)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("held poll failed (write deadline not extended?): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Atropos-Generation") != "2" {
		t.Fatalf("status = %d gen = %s, want 200 / 2", resp.StatusCode, resp.Header.Get("X-Atropos-Generation"))
	}
}
```

(add `"time"` to the imports.)

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/api/ -run TestHandlePollRules -count=1`
Expected: FAIL (`undefined: parseWait`; the 304/long-poll cases fail against the old handler).

- [ ] **Step 3: Rewrite the poll handler**

`internal/api/sdk_handler.go`: delete `activeFaultsForService` (:16-63), `cacheBoxEffectiveVersion` (:302-322) and the `hash/fnv` import; replace `handlePollRules`:

```go
// maxPollWait caps the long-poll hold (contract §2: wait ≤ 60 s).
const maxPollWait = 60 * time.Second

// handlePollRules serves an instance's desired state (desired-state protocol v1).
//
// @Summary      Poll for desired state (long-poll)
// @Description  Records the instance's observed generation (version) and poll time,
// @Description  materializes DesiredState(service, instance) and compares its ETag with
// @Description  If-None-Match. With wait>0 (seconds, max 60) and a matching ETag the
// @Description  request is held until the service's generation changes or wait elapses,
// @Description  then re-materialized. 200 carries the RuleSync body (version = generation,
// @Description  etag, rules, active_faults, freeze_cfg, replay_set) with ETag and
// @Description  X-Atropos-Generation headers; 304 carries the same headers and no body —
// @Description  the instance adopts X-Atropos-Generation without re-applying.
// @Tags         sdk
// @Produce      json
// @Param        service        query   string  true   "service name"
// @Param        instance_id    query   string  false  "SDK instance ID; selects the capability filter and records observed state"
// @Param        version        query   integer false  "generation the instance currently holds (recorded as observed_generation)"
// @Param        wait           query   integer false  "long-poll hold in seconds (0-60, default 0)"
// @Param        If-None-Match  header  string  false  "ETag of the last document the instance applied or confirmed"
// @Success      200  {object}  api.RuleSyncWire  "desired state; headers ETag, X-Atropos-Generation"
// @Success      304  "unchanged; headers ETag, X-Atropos-Generation"
// @Failure      400  {object}  api.ErrorResponse  "service query parameter required"
// @Failure      500  {object}  api.ErrorResponse  "desired state could not be materialized"
// @Router       /sdk/rules [get]
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	service := q.Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query parameter required")
		return
	}
	ctx := r.Context()
	observed, _ := strconv.ParseUint(q.Get("version"), 10, 64)
	wait := parseWait(q.Get("wait"))

	// Observed state first (contract §2): the generation the instance holds and
	// its poll time, before any 304 decision. Best-effort — a purged row must
	// not break polling.
	var inst *model.SDKInstance
	if id := q.Get("instance_id"); id != "" {
		if err := s.registry.RecordPoll(ctx, id, observed); err != nil {
			s.logger.Warn("record poll failed", "instance_id", id, "error", err)
		}
		if got, err := s.registry.Get(ctx, id); err == nil {
			inst = got
		}
	}

	doc, err := s.materializer.Materialize(ctx, service, inst)
	if err != nil {
		// Never fall back to a partial document: a 500 makes the SDK keep its
		// stale state and log degraded, its designed failure mode.
		s.logger.Error("poll: materialize desired state failed", "service", service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to materialize desired state")
		return
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if wait > 0 && etagMatches(ifNoneMatch, doc.ETag) {
		// Hold. The server's WriteTimeout (30s in cmd/manteion) would kill a
		// longer hold, so push the deadline past it, as the SSE handler does.
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(wait + 15*time.Second)); err != nil {
			s.logger.Debug("poll: extend write deadline failed", "error", err) // httptest.ResponseRecorder: unsupported
		}
		if _, err := s.generations.Wait(ctx, service, doc.Generation, wait); err != nil {
			return // client went away
		}
		if doc, err = s.materializer.Materialize(ctx, service, inst); err != nil {
			s.logger.Error("poll: re-materialize after hold failed", "service", service, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to materialize desired state")
			return
		}
	}

	w.Header().Set("ETag", `"`+doc.ETag+`"`)
	w.Header().Set("X-Atropos-Generation", strconv.FormatUint(doc.Generation, 10))
	if etagMatches(ifNoneMatch, doc.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, ruleSyncFrom(doc))
}

// parseWait parses the wait query param: seconds, clamped to [0, maxPollWait];
// absent, negative or unparseable → 0.
func parseWait(s string) time.Duration {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}
	return min(time.Duration(n)*time.Second, maxPollWait)
}

// etagMatches reports whether the If-None-Match header names etag: weak
// validators (W/) and quotes are stripped, a comma-separated list is matched
// element-wise, anything else is a mismatch.
func etagMatches(header, etag string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(part), "W/"))
		if strings.Trim(part, `"`) == etag && etag != "" {
			return true
		}
	}
	return false
}
```

`internal/api/server.go`: delete the `intent` field and `NewServer` parameter, drop the `atrocontrol` import if now unused; `cmd/manteion/main.go`: stop passing `controller.IntentReader()` to `NewServer` (it still feeds `mat.Freeze`).

- [ ] **Step 4: Run the poll tests**

Run: `go test ./internal/api/ -run TestHandlePollRules -count=1 -race`
Expected: PASS (the two 1 s cases and the write-timeout case take ~2 s together).

- [ ] **Step 5: Update the DB-gated API tests to the protocol**

`internal/api/sdk_instance_handler_test.go:505-513` (`TestSDKInstanceHandlers_DB`): build `gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())`, `rules := store.NewRuleRepo(database, gens)`, `faultConfigs := store.NewFaultConfigRepo(database)` and the server literal

```go
	s := &Server{
		rules: rules, rulever: rules, faults: faults, sdk: sdk, experiments: exp,
		sdkInstances: sdk, registry: sdk, serviceRules: rules, phaseHistory: exp,
		generations: gens, broker: gens.Broker(),
		materializer: &desired.Materializer{Generations: gens, Rules: rules, FaultConfigs: faultConfigs, CacheBox: exp, Specs: faults},
		logger: discardLogger(),
	}
```

Replace the `poll` helper (:602-612) and the pickup assertions (:627-681) with the ETag protocol:

```go
	poll := func(ifNoneMatch string) (*httptest.ResponseRecorder, RuleSyncWire) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/rules?service="+svc+"&instance_id="+inst.ID+"&version=0", nil)
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		var sync RuleSyncWire
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
				t.Fatalf("decode RuleSyncWire: %v", err)
			}
		}
		return w, sync
	}
	gen := func() uint64 {
		t.Helper()
		g, err := gens.Get(ctx, svc)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	// Before: the poll serves only the unattached rule (lo is gated behind its
	// pending phase) — "active" is broader than "served", by design.
	before := gen()
	w0, sync0 := poll("")
	if w0.Code != http.StatusOK || len(sync0.Rules) != 1 || sync0.Rules[0].Name != hi || sync0.Version != before {
		t.Fatalf("poll before kill-switch: code=%d rules=%+v version=%d, want 200 [%s] gen %d", w0.Code, sync0.Rules, sync0.Version, hi, before)
	}
	etag0 := w0.Header().Get("ETag")
	if w, _ := poll(etag0); w.Code != http.StatusNotModified {
		t.Fatalf("poll(same etag) = %d, want 304", w.Code)
	}

	// Kill-switch: one rule_version bump AND one generation bump per rule, exactly as PUT /rules/{id}.
	w := do(http.MethodPost, "/api/v1/sdk/instances/"+inst.ID+"/kill-switch")
	... (existing disabled_rule_ids / rule_version +2 / enabled-flag assertions unchanged) ...
	if gen() != before+2 {
		t.Errorf("generation %d → %d, want +2 (one bump per disabled rule)", before, gen())
	}

	// Poll pickup: the old ETag no longer matches → 200 with an empty set (the
	// reconciler clears on []) at the new generation; the new ETag → 304.
	w1, sync1 := poll(etag0)
	if w1.Code != http.StatusOK || len(sync1.Rules) != 0 || sync1.Version != before+2 {
		t.Errorf("poll(old etag) = %d version=%d rules=%d, want 200 gen %d rules 0", w1.Code, sync1.Version, len(sync1.Rules), before+2)
	}
	if w, _ := poll(w1.Header().Get("ETag")); w.Code != http.StatusNotModified {
		t.Errorf("poll(new etag) = %d, want 304", w.Code)
	}
	// The poll recorded what the instance holds.
	if d := detail(); d.ObservedGeneration != 0 || d.LastPollAt.Before(before0) {
		t.Errorf("detail after polls: observed_generation=%d last_poll_at=%v", d.ObservedGeneration, d.LastPollAt)
	}

	// Idempotent: nothing left to disable, no generation churn.
	w = do(http.MethodPost, "/api/v1/sdk/instances/"+inst.ID+"/kill-switch")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"disabled_rule_ids":[]`) {
		t.Errorf("second kill-switch: status = %d, body = %s", w.Code, w.Body.String())
	}
	if gen() != before+2 {
		t.Errorf("second kill-switch bumped the generation to %d", gen())
	}
```

(`before0 := time.Now()` captured before the first poll; `version=0` in the query keeps `observed_generation` at 0 — pass `&version=`+gen to assert the recorded value instead if preferred.) Keep the existing `rule_version` `+2` assertion: rule CRUD still bumps it.

`internal/api/sdk_recording_test.go:41-48`: `rules := store.NewRuleRepo(database, nil)` and add to the server literal `generations: desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())` (its memory store is fine: the test asserts rule content, not generations) and `materializer: &desired.Materializer{Generations: <same gens>, Rules: rules, FaultConfigs: store.NewFaultConfigRepo(database), CacheBox: exp, Specs: faults}` with `faults := store.NewFaultRepo(database)` hoisted; the `poll` helper's `atroposdk.RuleSync` decode still works (extra fields ignored).

- [ ] **Step 6: Run the DB-gated API tests and everything**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/api/ -run 'TestSDKInstanceHandlers_DB|TestPoll_RulesCarryPhaseContext' -count=1`
Expected: PASS
Run: `gofmt -l . ; go vet ./... && go test ./... -race`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add internal/api internal/api/server.go cmd/manteion/main.go
git commit -m "feat(api): long-polling GET /api/v1/sdk/rules with If-None-Match, ETag and X-Atropos-Generation; observed generation recorded per poll

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: `POST /api/v1/sdk/status`

**Files:**
- Create: `internal/api/sdk_status_handler.go`
- Modify: `internal/api/server.go:244-252` (route), `internal/api/sdk_desired_handler_test.go`

**Interfaces:**
- Consumes: `sdkRegistry.RecordStatus`, `model.ObservedStatus`, `model.ValidReplaySetState`.
- Produces: route `POST /api/v1/sdk/status` → 204; body type

```go
// statusReport is the POST /api/v1/sdk/status body (contract §2).
// TODO(desired-state): swap for the atropos-go wire alias when it lands.
type StatusReport struct {
	InstanceID        string                  `json:"instance_id"`
	Service           string                  `json:"service"`
	AppliedGeneration uint64                  `json:"applied_generation"`
	Etag              string                  `json:"etag"`
	Unsupported       []model.UnsupportedRule `json:"unsupported"`
	Errors            []string                `json:"errors"`
	ReplaySet         *model.ReplaySetStatus  `json:"replay_set,omitempty"`
}
```

- [ ] **Step 1: Write the failing tests**

Append to `internal/api/sdk_desired_handler_test.go`:

```go
func TestHandleSDKStatus(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	post := func(body any) *httptest.ResponseRecorder {
		return serveJSON(t, s, http.MethodPost, "/api/v1/sdk/status", body, nil)
	}
	w := post(StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 3, Etag: "3-abc",
		Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}, Errors: []string{"rule x: decode: boom"},
		ReplaySet: &model.ReplaySetStatus{ID: "sha256:aa", State: "committed"}})
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d body = %s, want 204", w.Code, w.Body.String())
	}
	got, _ := fx.reg.Get(context.Background(), "cart-1")
	if got.ObservedGeneration != 3 || got.ObservedEtag != "3-abc" || len(got.ObservedUnsupported) != 1 ||
		len(got.ObservedErrors) != 1 || got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "committed" {
		t.Errorf("stored = %+v", got)
	}
	if w := post(StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 4}); w.Code != http.StatusNoContent {
		t.Errorf("minimal report: status = %d", w.Code)
	}
	got, _ = fx.reg.Get(context.Background(), "cart-1")
	if got.ObservedGeneration != 4 || len(got.ObservedUnsupported) != 0 || got.ReplaySetState != "committed" {
		t.Errorf("after minimal report: %+v (empty lists clear, absent replay_set keeps)", got)
	}
	if w := post(StatusReport{Service: "cartservice"}); w.Code != http.StatusBadRequest {
		t.Errorf("missing instance_id: status = %d, want 400", w.Code)
	}
	if w := post(StatusReport{InstanceID: "cart-1", ReplaySet: &model.ReplaySetStatus{ID: "x", State: "done"}}); w.Code != http.StatusBadRequest {
		t.Errorf("bad replay state: status = %d, want 400", w.Code)
	}
	if w := post(StatusReport{InstanceID: "nope", Service: "cartservice"}); w.Code != http.StatusNotFound {
		t.Errorf("unknown instance: status = %d, want 404", w.Code)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/api/ -run TestHandleSDKStatus -count=1`
Expected: compile error (`undefined: StatusReport`).

- [ ] **Step 3: Implement the handler and route**

`internal/api/sdk_status_handler.go`:

```go
// handleSDKStatus stores an instance's applied-state report.
//
// @Summary      Report applied desired state
// @Description  Sent by an SDK instance after every apply and whenever its replay-set
// @Description  state changes (fetching|committed|failed). Stores applied_generation,
// @Description  etag, unsupported rules and errors (each report replaces the previous;
// @Description  empty lists clear) and the replay-set state on the instance row, where
// @Description  GET /sdk/instances exposes them and the orchestrator's convergence
// @Description  barrier reads them.
// @Tags         sdk
// @Accept       json
// @Param        report  body  api.StatusReport  true  "applied-state report"
// @Success      204  "stored"
// @Failure      400  {object}  api.ErrorResponse  "invalid JSON, missing instance_id or unknown replay_set.state"
// @Failure      404  {object}  api.ErrorResponse  "unknown instance"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/status [post]
func (s *Server) handleSDKStatus(w http.ResponseWriter, r *http.Request) {
	var rep StatusReport
	if err := readJSON(r, &rep); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if rep.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id required")
		return
	}
	if rep.ReplaySet != nil && !model.ValidReplaySetState(rep.ReplaySet.State) {
		writeError(w, http.StatusBadRequest, "replay_set.state must be fetching, committed or failed")
		return
	}
	err := s.registry.RecordStatus(r.Context(), rep.InstanceID, model.ObservedStatus{
		AppliedGeneration: rep.AppliedGeneration, Etag: rep.Etag,
		Unsupported: rep.Unsupported, Errors: rep.Errors, ReplaySet: rep.ReplaySet,
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "instance not found")
	case err != nil:
		s.logger.Error("sdk status: record failed", "instance_id", rep.InstanceID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to record status")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
```

Route in `internal/api/server.go` next to `/sdk/rules` (:250): `mux.HandleFunc("POST /api/v1/sdk/status", s.handleSDKStatus)`.

- [ ] **Step 4: Run and commit**

Run: `go test ./internal/api/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/api/sdk_status_handler.go internal/api/server.go internal/api/sdk_desired_handler_test.go
git commit -m "feat(api): POST /api/v1/sdk/status stores the instance's applied generation, unsupported rules and replay-set state

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Orchestrator bumps generations on phase transitions

**Files:**
- Modify: `internal/orchestrator/orchestrator.go:41-116` (field + `New` param), `internal/orchestrator/runner.go:142-158, 348-358`, `internal/orchestrator/drain.go:23-29`
- Modify: `internal/store/rule_repo.go:215-226` (delete `BumpVersion`)
- Modify: `internal/orchestrator/orchestrator_test.go:96-118`, `internal/orchestrator/runner_test.go:106-150`, `cmd/manteion/main.go:100`

**Interfaces:**
- Consumes: `desired.Generations.Bump`, `phaseServices` (`runner.go:943-974`), `experimentFrozenServices` (`drain.go:159-176`).
- Produces: `orchestrator.New(..., faultEvents, gens *desired.Generations, logger)`; `(*Orchestrator).desiredServices(ctx, p *model.ExperimentPhase) []string` — the de-duplicated union of `phaseServices(p)` and, for a `persist_cache` phase, `experimentFrozenServices(p.ExperimentID)` (the services whose record rule the baseline synthesizes); `(*Orchestrator).bumpDesired(ctx, p) map[string]uint64` — bumps each once, returns service → new generation (plan 2's convergence targets), warns on error. `RuleRepo.BumpVersion` is gone; `rule_version` is bumped only by rule CRUD (readiness probe + kill-switch invariant).

- [ ] **Step 1: Rewrite the failing DB-gated test**

Replace `TestEnterPhase_BumpsVersionOnResume` (`internal/orchestrator/runner_test.go:106-150`) with:

```go
// TestEnterPhase_BumpsGenerationOnEveryEnter pins M5 on desired-state v1: every
// enter of a phase (fresh AND resume) bumps the generation of each service the
// phase's desired state touches — the frozen services of the phase, the rules'
// target services, and for a baseline the services frozen anywhere in the
// experiment (their record rule is what the baseline synthesizes). A resume
// that skipped the bump would leave a long-polling SDK holding until its wait
// elapsed with no replay rule, running live.
func TestEnterPhase_BumpsGenerationOnEveryEnter(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")    // nil zeus → no load drivers
	o.autoComplete = false // don't spawn an async finishPhase that would also bump
	exp := mkExp(t)

	gen := func(svc string) uint64 {
		t.Helper()
		g, err := o.generations.Get(ctx, svc)
		if err != nil {
			t.Fatalf("read generation: %v", err)
		}
		return g
	}

	// A baseline records the services frozen later in the experiment: entering
	// it bumps "frontend" (frozen in the isolation phase) and nothing else.
	base := mkRunningBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	f0, c0 := gen("frontend"), gen("checkoutservice")
	if err := o.enterPhase(ctx, base, true); err != nil {
		t.Fatalf("enterPhase fresh baseline: %v", err)
	}
	if gen("frontend") != f0+1 || gen("checkoutservice") != c0 {
		t.Fatalf("fresh baseline enter: frontend %d→%d, checkout %d→%d; want frontend +1 only", f0, gen("frontend"), c0, gen("checkoutservice"))
	}

	// Resume (fresh=false) of the frozen isolation phase bumps again.
	f1 := gen("frontend")
	if err := o.enterPhase(ctx, iso, false); err != nil {
		t.Fatalf("enterPhase resume isolation: %v", err)
	}
	if gen("frontend") != f1+1 {
		t.Fatalf("resume of frozen phase did not bump frontend: %d → %d", f1, gen("frontend"))
	}
}
```

(`mkRunningBaseline` is in `drain_test.go`, `mkRunningIsolation` in `preload_gate_test.go`; the resume path skips the preload gate, so no SDK is needed in plan 1.)

- [ ] **Step 2: Run to verify it fails**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -run TestEnterPhase_BumpsGenerationOnEveryEnter -count=1`
Expected: compile error (`o.generations undefined`).

- [ ] **Step 3: Implement**

`internal/orchestrator/orchestrator.go`: field `generations *desired.Generations` and `New(..., faultEvents *store.PhaseFaultEventRepo, gens *desired.Generations, logger *slog.Logger)`.

`internal/orchestrator/runner.go` — add near `phaseServices`:

```go
// desiredServices is every service whose materialized desired state this
// phase changes: the frozen services and rule targets (phaseServices), plus,
// for a baseline, the services frozen anywhere in the experiment — the
// baseline is what synthesizes their record rule (ActiveCacheBoxPhaseForService).
func (o *Orchestrator) desiredServices(ctx context.Context, p *model.ExperimentPhase) []string {
	svcs := o.phaseServices(ctx, p)
	if !p.PersistCache {
		return svcs
	}
	seen := make(map[string]struct{}, len(svcs))
	for _, s := range svcs {
		seen[s] = struct{}{}
	}
	for _, s := range o.experimentFrozenServices(ctx, p.ExperimentID) {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			svcs = append(svcs, s)
		}
	}
	return svcs
}

// bumpDesired bumps the generation of every service this phase's desired
// state touches (one bump each, in the same order every time) and returns the
// targets — the generations instances must observe to have converged.
func (o *Orchestrator) bumpDesired(ctx context.Context, p *model.ExperimentPhase) map[string]uint64 {
	targets := map[string]uint64{}
	for _, svc := range o.desiredServices(ctx, p) {
		gen, err := o.generations.Bump(ctx, svc)
		if err != nil {
			o.logger.Warn("orchestrator: bump generation failed", "phase_id", p.ID, "service", svc, "error", err)
			continue
		}
		targets[svc] = gen
	}
	return targets
}
```

Replace the three `rule_version` bumps: `runner.go:142-158` (enter; now unconditional — a fault-rule phase changes desired state too, and this is what wakes its long-pollers) → `o.bumpDesired(ctx, p)` with the comment rewritten accordingly; `runner.go:348-358` (end) → `o.bumpDesired(ctx, p)`; `drain.go:27-29` → `o.bumpDesired(ctx, p)`. Delete `RuleRepo.BumpVersion` (`internal/store/rule_repo.go:215-226`; `bumpVersion` stays).

`internal/orchestrator/orchestrator_test.go:96-118` `newOrch`: `gens := desired.NewGenerations(store.NewGenerationRepo(testDB), desired.NewBroker())`, `testRuleRepo` stays constructed with `nil` in `TestMain` (its bumps are not under test here) and `New(..., store.NewPhaseFaultEventRepo(testDB), gens, logger)`. `cmd/manteion/main.go:100`: pass `gens`.

- [ ] **Step 4: Run the orchestrator suite (DB) and everything**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -count=1 -p 1`
Expected: PASS (every existing test; the push channel is untouched in plan 1).
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add internal/orchestrator internal/store/rule_repo.go cmd/manteion/main.go
git commit -m "feat(orchestrator): phase enter/exit/drain bump the per-service generation; rule_version out-of-band bumps removed

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Instance surfaces, OpenAPI, docs, final verification

**Files:**
- Modify: `internal/api/sdk_instance_handler_test.go:170-298` (one more case), `internal/api/sdk_handler.go` (list annotation), `internal/api/sse_handler.go:9-12` (comment + swag block)
- Regenerate: `docs/swagger.yaml`, `docs/swagger.json`
- Modify: `README.md:33, 42-57`, `CLAUDE.md` ("Intent tracker" and "Rule version in PostgreSQL" bullets, `internal/atrocontrol/` row)

- [ ] **Step 1: Pin the new fields on list/detail**

Add a case to `TestHandleGetInstance` whose fixture sets `Host: "sidecar", Capabilities: []string{"inline"}, ObservedGeneration: 7, ReplaySetState: "committed"` and assert the raw body has `"host":"sidecar"`, `"capabilities":["inline"]`, `"observed_generation":7`, `"replay_set_state":"committed"`, and that `observed_etag`/`observed_at` are absent when zero (the `last_error`/`last_rule_version_acked` absence assertions stay).

Run: `go test ./internal/api/ -run TestHandleGetInstance -count=1`
Expected: PASS (json tags from Task 1 do the work; the test pins the names).

- [ ] **Step 2: Annotate and regenerate OpenAPI**

- `handleListInstances` description: add "including host, capabilities and observed state (observed_generation, observed_etag, observed_unsupported, observed_errors, replay_set_id/state, observed_at)".
- `handleSSEEvents`: add a swag block (`@Summary Rule-change nudges (SSE)`, `@Produce text/event-stream`, event type `rules_changed` with data `{"generation":G}`, emitted on every generation change including orchestrator transitions, `@Router /sdk/events [get]`).
- Register/poll/status blocks were written in Tasks 5-7.

Run: `make openapi && git diff --stat -- docs/swagger.yaml docs/swagger.json`
Expected: both files change (new `api.RuleSyncWire`, `api.RegisterResponseWire`, `api.StatusReport`, `model.UnsupportedRule`, `model.ReplaySetRef` schemas; `/sdk/status`, `/sdk/events`; `model.SDKInstance` new properties). If `swag` is missing: `go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5` (docs/openapi-conventions.md).
Run: `make openapi-check`
Expected: exits 0.

- [ ] **Step 3: Docs**

`README.md` "Wire contracts": Poll → `GET /api/v1/sdk/rules?service=&instance_id=&version=&wait=` with `If-None-Match`; 200 = full desired state (`version` is the per-service generation, `etag`), 304 on a matching ETag, long-poll up to 60 s; Register → response is the materialized desired state filtered by the advertised capabilities; add Status → `POST /api/v1/sdk/status`; SSE data is `{"generation":G}`. `CLAUDE.md`: replace the "Rule version in PostgreSQL" bullet with the generation/ETag model, drop the "Intent tracker" bullet's piggyback claim (the register response is materialized from the DB; the tracker only still feeds freeze config until plan 2), add `internal/desired/` to the package table.

- [ ] **Step 4: Final verification**

Run: `gofmt -l . ; go vet ./... && go build ./... && go test ./... -race`
Expected: clean.
Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test -count=1 -timeout 300s -p 1 ./...` (= `make integration`)
Expected: `ok` for every package (store, desired, db, api, orchestrator DB suites included).

- [ ] **Step 5: Commit**

```bash
git add internal/api docs/swagger.yaml docs/swagger.json README.md CLAUDE.md
git commit -m "docs(api): desired-state v1 OpenAPI (register, long-poll, status, SSE) and protocol notes

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review (done while writing; recorded for the executor)

- **Spec coverage.** §1 model: generation table (T1), bumps on rule CRUD/fault config/kill switch (T3), phase transitions (T8), capability change (T5), ETag formula and capability filter incl. compositions (T4). Freeze/thaw and artifact assignment bumps, plus the DB-derived freeze config and replay-set ref, are plan 2 (the hooks `Materializer.Freeze`/`ReplaySets` exist here). §2 poll (T6), status (T7), SSE on every bump incl. orchestrator (T2+T8), register (T5). §2 ingest `epoch` and `/cache/artifacts` are plan 2. §5: `version` keeps its name (T5/T6).
- **Type consistency.** `RecordPoll(ctx, id, uint64)` and `RecordStatus(ctx, id, model.ObservedStatus)` are the names used by the store (T1), the `sdkRegistry` interface (T5), the fake (T5) and the handlers (T6/T7). `desired.Document` fields match `ruleSyncFrom` (T5). `New(..., gens, logger)` for the orchestrator matches `newOrch` and `main.go` (T8). `NewRuleRepo(db, gens)` call sites are enumerated in T3.
- **Review Focus pins:** 1 → T6 write-timeout test; 2 → T6 unknown instance; 3 → T6 weak/foreign ETag; 4 → T5 capability-change test; 5 → T5 degraded 201.
- **Amendment of 2026-10-30 applied:** `ReplaySetRef` carries `experiment_id`/`phase_id` (T1); plan 2 fills them from the frozen phase and stores the pair on the artifact record. Unknown capability strings are ignored (T4 test); compositions ship as today.
- **Open questions for the human (none block execution):** (a) a fresh registration does not bump (the response already carries the document) — the contract's wording only covers "different from the stored ones"; (b) `freeze_cfg`/`replay_set` are gated on the `cachebox` capability (not stated in the contract, implied by what an instance without a cache-box can hold); (c) `rule_version` is kept as the readiness probe and the kill-switch "+2" invariant; it no longer drives any poll decision.
