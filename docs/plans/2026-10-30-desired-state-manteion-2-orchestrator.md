# Desired-State Protocol v1 — manteion plan 2: orchestrator convergence, artifacts, freeze, ingest, cleanup

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The orchestrator stops pushing to SDK instances: a phase transition writes its inputs (rules attached, replay-set artifact assigned, freeze config), bumps every affected service's generation once and waits for every live instance to report the target generation (and a committed replay set on frozen services); replay sets become immutable, content-addressed NDJSON artifacts served over HTTP with Range; ingest dedupes on `epoch`; the push/intent machinery in `atrocontrol` is deleted.

**Architecture:** Plan 1's materializer hooks (`Materializer.Freeze`, `Materializer.ReplaySets`) are filled by `desired.PhaseSources`, which derives `freeze_cfg` and `replay_set` from the running phase that freezes a service and the `replay_artifacts` row assigned to `(phase, service)`. `enterPhase` becomes build-artifacts → bump → `waitConverged`; `endPhase` becomes stop-load → verdict (fidelity pulled while the replay set is still held) → bump → non-fatal wait; the drain barrier bumps and waits non-fatally before its report gate. Observed state comes from `sdk_instances` (plan 1's `RecordPoll`/`RecordStatus`), read through the existing live-instance filter. The policy engine's push/clear actions route through rule CRUD (which bumps generations) so it still compiles under the 2026-06 freeze.

**Tech Stack:** Go 1.25, database/sql + pgx, `net/http.ServeContent` for Range/HEAD, swaggo v2, DB-free handler tests with fakes, DB-gated orchestrator/store tests (`MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1`).

**Spec:** `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0/docs/plans/2026-10-30-desired-state-protocol.md` §2 (`/cache/artifacts/{id}`, ingest `epoch`, `replay_set` with `experiment_id`/`phase_id`), §4 (orchestrator behaviour). Prerequisite: plan 1 (`2026-10-30-desired-state-manteion-1-materializer.md`) is merged on `feat/atropos-daemon`: `internal/desired` (Generations, Materializer, `FreezeSource`, `ReplaySetSource`), `model.ReplaySetRef{ID, ExperimentID, PhaseID, KeyStrategy, Count, SHA256, SizeBytes, URL}`, `SDKRepo.RecordPoll/RecordStatus`, `Orchestrator.bumpDesired/desiredServices`, `Server.bumpGeneration`.

Repo: `/Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon`. Line numbers are as of a85ee9e; plan 1 shifts some by a few lines — anchor on the function names given.

## Global Constraints

- Phase entry (contract §4): write inputs, bump each service in the phase once, then wait until every live instance of those services has `observed_generation >= target` and, for frozen services, `replay_set_state = committed` for the assigned artifact, within `MANTEION_CONVERGE_TIMEOUT` (default 30 s). Timeout fails the phase with the list of lagging instances. This replaces `pushPhaseRules`, `freezeServices`, `preloadCacheEntries`.
- Phase exit / drain / teardown: bump-and-wait with a non-fatal, logged timeout. Kill switch: writes the killed state (no rules, no faults), bumps; the SSE nudge stays. Fidelity pull and the drain barrier stay as they are.
- `GET /api/v1/cache/artifacts/{id}`: immutable NDJSON (one `CacheBoxWireEntry` per line), `Range: bytes=a-b` → 206, `HEAD` → `Content-Length` + `ETag: "<id>"`; `id` = `sha256:<hex>` of the file bytes; `replay_set.sha256` = the W5 digest (`cachestore.SetChecksum`, unchanged), `count` = entry count.
- `replay_set` carries `experiment_id`/`phase_id` (the pair the set is installed under — the frozen phase, not the baseline) — contract §2 amendment of 2026-10-30; the artifact record stores the pair.
- Ingest dedupe key `(experiment, phase, instance, epoch, batch_seq)`; missing `epoch` = 0 keeps the old behaviour.
- Remove `atrocontrol.PushRules/PushRulesToInstance/FreezeService/ClearService/FreezeInstance/ClearInstance/PreloadService`, `InjectFault*`/`ClearFault*`, the `IntentTracker`, and the frozen policy engine's `PushRules` calls (route through rule CRUD; keep the engine opt-in per `docs/decisions/2026-06-policy-engine-freeze.md`).
- Migrations: append-only embedded Go, contiguous numbering; this plan adds migration 13.
- Conventional commits ending with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; `gofmt -l .` empty, `go vet ./...`, `go test ./...` green after every task; `make openapi-check` green at the end.
- Do not touch `.claude/` or `vendor/`.

## Review Focus

1. **The verdict must pull fidelity before the bump that thaws.** After the exit bump an instance clears its replay set (contract §2: absent = none desired), so a pull after it reads `Preload.Committed=false` and every isolation phase becomes INVALID(`preload_incomplete`). Pinned in Task 5 (`TestFinishPhase_VerdictBeforeBump`).
2. **A frozen service with no live instance must fail the phase (INV-4), not converge vacuously.** Pinned in Task 5 (`TestEnterPhase_FrozenServiceWithoutInstancesAborts`).
3. **An instance reporting `replay_set_state = failed` for the assigned artifact should fail fast**, not burn the whole timeout: the error is already known. Pinned in Task 5 (`TestEnterPhase_ReplaySetFailedAbortsEarly`).
4. **Artifact row present, file gone (CACHE_DIR wiped across a restart).** The handler must answer an error status the SDK reports as `failed`, never a 200 with an empty body. Pinned in Task 3 (`missing file → 500`). Phase entry always rebuilds the artifact fresh, so the next phase heals.
5. **A `Range` past EOF** must be 416, not a 200 of the whole file the SDK then double-counts. Pinned in Task 3.

---

## File structure

Created:
- `internal/model/replay_artifact.go` — `ReplayArtifact`.
- `internal/store/replay_artifact_repo.go` (+ `_test.go`) — `ReplayArtifactRepo`.
- `internal/cachestore/artifact.go` (+ `_test.go`) — `BuildArtifact`, `ArtifactInfo`.
- `internal/api/cache_artifact_handler.go` (+ `_test.go`) — `GET|HEAD /api/v1/cache/artifacts/{id}`.
- `internal/desired/phase_sources.go` (+ `_test.go`), `internal/desired/freeze.go` (+ `freeze_test.go`, moved from `internal/orchestrator/freeze_test.go`).
- `internal/orchestrator/converge.go` — `waitConverged`, `laggingInstances`, `awaitConvergedNonFatal`.
- `internal/orchestrator/converge_test.go`.

Modified: `internal/db/migrations.go`, `internal/testutil/db.go`, `internal/store/experiment_repo.go`, `internal/store/rule_repo.go`, `internal/model/rule.go`, `internal/ruleconv/ruleconv.go`, `internal/cachestore/store.go`, `internal/api/{server.go,cache_handler.go,sdk_instance_handler.go,sdk_instance_handler_test.go,cache_handler_test.go}`, `internal/orchestrator/{orchestrator.go,runner.go,drain.go,fake_sdk_test.go,orchestrator_test.go,preload_gate_test.go,runner_test.go,verdict_test.go,drain_test.go,harvest_test.go}`, `internal/policy/{engine.go,engine_test.go}`, `cmd/manteion/main.go`, docs.

Deleted: `internal/orchestrator/rules.go`, `push_rules_test.go`; `internal/atrocontrol/{rules.go,freeze.go,fault.go,preload.go,preload_test.go,intent.go,fanout.go,errors.go}` (the status/fidelity/instances surface stays).

---

### Task 1: Migration 13 `replay_artifacts`, model and store

**Files:**
- Modify: `internal/db/migrations.go` (append `migration13`, list entry after `{12, ...}`)
- Create: `internal/model/replay_artifact.go`, `internal/store/replay_artifact_repo.go`, `internal/store/replay_artifact_repo_test.go`
- Modify: `internal/testutil/db.go:43-70` (add `"replay_artifacts"` before `"experiment_phases"`)

**Interfaces:**
- Produces:

```go
// internal/model/replay_artifact.go
// ReplayArtifact is one immutable replay-set artifact assigned to a frozen
// (phase, service): ID is "sha256:<hex>" of the file bytes (content-addressed:
// two phases replaying the same recording share the id and the file), the
// pair is the isolation phase the set is installed under.
type ReplayArtifact struct {
	ID           string    `json:"id"`
	ExperimentID string    `json:"experiment_id"`
	PhaseID      string    `json:"phase_id"`
	Service      string    `json:"service"`
	EntryCount   int64     `json:"count"`
	W5SHA256     string    `json:"sha256"`
	SizeBytes    int64     `json:"size_bytes"`
	Path         string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

// internal/store/replay_artifact_repo.go
func NewReplayArtifactRepo(db *sql.DB) *ReplayArtifactRepo
func (r *ReplayArtifactRepo) Upsert(ctx context.Context, a *model.ReplayArtifact) error              // keyed (phase_id, service); re-assigning replaces
func (r *ReplayArtifactRepo) GetByID(ctx context.Context, id string) (*model.ReplayArtifact, error)   // ErrNotFound; LIMIT 1 (duplicates are byte-identical)
func (r *ReplayArtifactRepo) ForPhaseService(ctx context.Context, phaseID, service string) (*model.ReplayArtifact, error) // nil, nil when none assigned
```

- [ ] **Step 1: Write the failing store test**

`internal/store/replay_artifact_repo_test.go`:

```go
package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/testutil"
)

func TestReplayArtifactRepo_UpsertGetForPhase(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	exp := NewExperimentRepo(conn)
	repo := NewReplayArtifactRepo(conn)

	e := &model.Experiment{ID: id.New("exp"), Name: "art", Status: "planned", CreatedAt: time.Now()}
	if err := exp.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "iso", Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", MutationPolicy: "deny"}}}
	if err := exp.CreatePhase(ctx, p); err != nil {
		t.Fatal(err)
	}

	if got, err := repo.ForPhaseService(ctx, p.ID, "frontend"); err != nil || got != nil {
		t.Fatalf("ForPhaseService before upsert = %v, %v; want nil, nil", got, err)
	}
	a := &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: p.ID, Service: "frontend",
		EntryCount: 3, W5SHA256: "w5", SizeBytes: 120, Path: "/tmp/x.ndjson"}
	if err := repo.Upsert(ctx, a); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := repo.GetByID(ctx, a.ID)
	if err != nil || got.PhaseID != p.ID || got.EntryCount != 3 || got.Path != "/tmp/x.ndjson" || got.CreatedAt.IsZero() {
		t.Fatalf("GetByID = %+v, %v", got, err)
	}
	a.ID, a.EntryCount = "sha256:"+id.New("h"), 4 // re-assign the pair to a new build
	if err := repo.Upsert(ctx, a); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if got, _ := repo.ForPhaseService(ctx, p.ID, "frontend"); got == nil || got.ID != a.ID || got.EntryCount != 4 {
		t.Fatalf("ForPhaseService after re-assign = %+v", got)
	}
	if _, err := repo.GetByID(ctx, "sha256:nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID unknown = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -run TestReplayArtifactRepo -count=1`
Expected: compile error (`undefined: NewReplayArtifactRepo`).

- [ ] **Step 3: Implement**

`internal/db/migrations.go`:

```go
// migration13 records replay-set artifacts (desired-state v1 §2): the
// content-addressed NDJSON file built from a baseline recording at isolation
// phase entry and assigned to the (phase, service) it is installed under. The
// id repeats across phases that replay the same recording (same bytes, same
// file); the pair is the key. Rows go with their phase; the file on disk is
// left for the operator (artifacts are small and immutable).
const migration13 = `
CREATE TABLE IF NOT EXISTS replay_artifacts (
    id            TEXT NOT NULL,                       -- 'sha256:<hex>' of the file bytes
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    phase_id      TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    service       TEXT NOT NULL,
    entry_count   BIGINT NOT NULL,
    w5_sha256     TEXT NOT NULL,                       -- §W5 digest over the entries
    size_bytes    BIGINT NOT NULL,
    path          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_id, service)
);
CREATE INDEX IF NOT EXISTS idx_replay_artifacts_id ON replay_artifacts(id);
`
```

list entry `{13, "replay_artifacts: content-addressed replay sets per (phase, service) (desired-state v1)", migration13}`. The repo: `Upsert` = `INSERT ... ON CONFLICT (phase_id, service) DO UPDATE SET id, experiment_id, entry_count, w5_sha256, size_bytes, path = EXCLUDED.*, created_at = now()`; `GetByID` = `SELECT ... WHERE id = $1 ORDER BY created_at DESC LIMIT 1` (`sql.ErrNoRows` → `ErrNotFound`); `ForPhaseService` = `... WHERE phase_id = $1 AND service = $2` (`sql.ErrNoRows` → `nil, nil`); one `scanReplayArtifact` shared by both.

- [ ] **Step 4: Run and commit**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/db/migrations.go internal/model/replay_artifact.go internal/store/replay_artifact_repo.go internal/store/replay_artifact_repo_test.go internal/testutil/db.go
git commit -m "feat(store): migration 13 — replay_artifacts per (phase, service)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Build content-addressed artifacts in `cachestore`

**Files:**
- Create: `internal/cachestore/artifact.go`, `internal/cachestore/artifact_test.go`

**Interfaces:**
- Consumes: `(*Store).Read` (`internal/cachestore/store.go:265-288`), `SetChecksum` (`checksum.go:36-52`), `s.root`.
- Produces:

```go
// ArtifactInfo describes a built replay-set artifact.
type ArtifactInfo struct {
	ID        string // "sha256:<hex>" of the file bytes
	Path      string // {root}/artifacts/<hex>.ndjson
	Count     int64
	W5SHA256  string // SetChecksum over the entries
	SizeBytes int64
}
// BuildArtifact materializes the (exp, phase, service) recording as an immutable
// NDJSON file (one WireEntry per line, the lines preload chunks carried) and
// returns its descriptor. Idempotent: the same recording yields the same id and
// file. Errors: no recording / zero entries.
func (s *Store) BuildArtifact(experimentID, phaseID, service string) (ArtifactInfo, error)
// ArtifactPath maps a valid id ("sha256:" + 64 hex) to its file path; any other
// id is an error (never a path traversal).
func (s *Store) ArtifactPath(id string) (string, error)
```

- [ ] **Step 1: Write the failing tests**

`internal/cachestore/artifact_test.go`:

```go
package cachestore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

func TestBuildArtifact_ContentAddressedAndIdempotent(t *testing.T) {
	s := New(t.TempDir())
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "k1", StatusCode: 200, Body: []byte("one")},
		{Key: "k2", StatusCode: 404, Body: []byte("two")},
	}
	if err := s.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	info, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	raw, err := os.ReadFile(info.Path)
	if err != nil {
		t.Fatalf("artifact file: %v", err)
	}
	sum := sha256.Sum256(raw)
	if info.ID != "sha256:"+hex.EncodeToString(sum[:]) || info.SizeBytes != int64(len(raw)) {
		t.Errorf("id/size = %s/%d, want sha256 of the %d file bytes", info.ID, info.SizeBytes, len(raw))
	}
	if info.Count != 2 || info.W5SHA256 != SetChecksum(entries) {
		t.Errorf("count/w5 = %d/%s", info.Count, info.W5SHA256)
	}
	// One WireEntry per line, decodable with the same type preload chunks carried.
	sc := bufio.NewScanner(strings.NewReader(string(raw)))
	var lines int
	for sc.Scan() {
		var e atroposdk.CacheBoxWireEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %d: %v", lines, err)
		}
		lines++
	}
	if lines != 2 {
		t.Errorf("lines = %d, want 2", lines)
	}
	again, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil || again.ID != info.ID || again.Path != info.Path {
		t.Errorf("rebuild = %+v, %v; want the same artifact", again, err)
	}
	if p, err := s.ArtifactPath(info.ID); err != nil || p != info.Path {
		t.Errorf("ArtifactPath = %s, %v", p, err)
	}
	for _, bad := range []string{"../../etc/passwd", "sha256:zz", "sha256:" + strings.Repeat("a", 63), ""} {
		if _, err := s.ArtifactPath(bad); err == nil {
			t.Errorf("ArtifactPath(%q) accepted", bad)
		}
	}
}

func TestBuildArtifact_RefusesEmptyRecording(t *testing.T) {
	s := New(t.TempDir())
	if _, err := s.BuildArtifact("exp-1", "phase-none", "frontend"); err == nil {
		t.Fatal("expected an error for a recording with no entries")
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/cachestore/ -run TestBuildArtifact -count=1`
Expected: compile error (`s.BuildArtifact undefined`).

- [ ] **Step 3: Implement `artifact.go`**

Algorithm (the only part the signature does not determine): `entries, err := s.Read(...)`; `len == 0` → `fmt.Errorf("no recorded entries for (%s, %s, %s)", ...)`; `os.MkdirAll(dir, 0o755)` with `dir = filepath.Join(s.root, "artifacts")`; `tmp, _ := os.CreateTemp(dir, ".build-*")`; `h := sha256.New()`; `enc := json.NewEncoder(io.MultiWriter(tmp, h))` (one `Encode` per entry — the encoder appends the newline); count bytes with a small counting writer in the same `MultiWriter`; close; `hexsum := hex.EncodeToString(h.Sum(nil))`; `final := filepath.Join(dir, hexsum+".ndjson")`; if `final` exists remove `tmp`, else `os.Rename(tmp, final)`; return `ArtifactInfo{ID: "sha256:" + hexsum, Path: final, Count: int64(len(entries)), W5SHA256: SetChecksum(entries), SizeBytes: n}`. `ArtifactPath` validates with a `regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)` and returns `filepath.Join(dir, hex+".ndjson")`.

- [ ] **Step 4: Run and commit**

Run: `go test ./internal/cachestore/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/cachestore/artifact.go internal/cachestore/artifact_test.go
git commit -m "feat(cachestore): build immutable, content-addressed replay-set artifacts from a recording

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `GET|HEAD /api/v1/cache/artifacts/{id}`

**Files:**
- Create: `internal/api/cache_artifact_handler.go`, `internal/api/cache_artifact_handler_test.go`
- Modify: `internal/api/server.go` (field `artifacts artifactReader`, `NewServer(..., artifacts *store.ReplayArtifactRepo, ...)`, route next to `/cache/entries` at :256), `cmd/manteion/main.go`

**Interfaces:**
- Consumes: `store.ReplayArtifactRepo.GetByID`.
- Produces: `type artifactReader interface { GetByID(ctx context.Context, id string) (*model.ReplayArtifact, error) }`; handler `handleCacheArtifact` — 404 unknown id, 500 `artifact file unavailable` when the path cannot be opened, else `Content-Type: application/x-ndjson`, `ETag: "<id>"`, `Cache-Control: public, max-age=31536000, immutable`, body via `http.ServeContent` (Range → 206/416, HEAD → headers only, `If-None-Match` → 304).

- [ ] **Step 1: Write the failing tests**

`internal/api/cache_artifact_handler_test.go`:

```go
package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

type fakeArtifacts struct{ byID map[string]*model.ReplayArtifact }

func (f fakeArtifacts) GetByID(_ context.Context, id string) (*model.ReplayArtifact, error) {
	if a, ok := f.byID[id]; ok {
		return a, nil
	}
	return nil, store.ErrNotFound
}

func artifactServer(t *testing.T) (*Server, string, []byte) {
	t.Helper()
	body := []byte("{\"key\":\"k1\"}\n{\"key\":\"k2\"}\n")
	path := filepath.Join(t.TempDir(), "a.ndjson")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	id := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	s := &Server{logger: discardLogger(), artifacts: fakeArtifacts{byID: map[string]*model.ReplayArtifact{
		id:             {ID: id, Path: path, SizeBytes: int64(len(body))},
		"sha256:gone": {ID: "sha256:gone", Path: filepath.Join(t.TempDir(), "missing.ndjson")},
	}}}
	return s, id, body
}

func getArtifact(t *testing.T, s *Server, method, id string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.routes(mux)
	req := httptest.NewRequest(method, "/api/v1/cache/artifacts/"+id, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestHandleCacheArtifact(t *testing.T) {
	s, id, body := artifactServer(t)

	w := getArtifact(t, s, http.MethodGet, id, nil)
	if w.Code != http.StatusOK || w.Body.String() != string(body) {
		t.Fatalf("GET: %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("ETag") != `"`+id+`"` || w.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Errorf("GET headers = %v", w.Header())
	}

	w = getArtifact(t, s, http.MethodHead, id, nil)
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("Content-Length") != "26" || w.Header().Get("ETag") == "" {
		t.Errorf("HEAD: %d len=%d headers=%v", w.Code, w.Body.Len(), w.Header())
	}

	w = getArtifact(t, s, http.MethodGet, id, map[string]string{"Range": "bytes=0-12"})
	if w.Code != http.StatusPartialContent || w.Body.String() != string(body[:13]) || w.Header().Get("Content-Range") != "bytes 0-12/26" {
		t.Errorf("Range: %d %q %v", w.Code, w.Body.String(), w.Header())
	}
	if w = getArtifact(t, s, http.MethodGet, id, map[string]string{"Range": "bytes=999-"}); w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("Range past EOF: %d, want 416", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, id, map[string]string{"If-None-Match": `"` + id + `"`}); w.Code != http.StatusNotModified {
		t.Errorf("If-None-Match: %d, want 304", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, "sha256:unknown", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown: %d, want 404", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, "sha256:gone", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("missing file: %d, want 500 (never a 200 with an empty body)", w.Code)
	}
}
```

(no `io` import is needed; `getArtifact` builds the request without a body.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/api/ -run TestHandleCacheArtifact -count=1`
Expected: compile error (`unknown field artifacts`).

- [ ] **Step 3: Implement**

`internal/api/cache_artifact_handler.go`:

```go
// handleCacheArtifact serves an immutable replay-set artifact.
//
// @Summary      Fetch a replay-set artifact
// @Description  Immutable NDJSON replay set (one CacheBoxWireEntry per line) named by
// @Description  RuleSync.replay_set.id ("sha256:<hex>" of the file bytes). Supports
// @Description  HEAD (Content-Length, ETag) and Range (206); the instance fetches it in
// @Description  ≤4 MiB ranges and commits when count and the W5 digest match.
// @Tags         cache
// @Produce      application/x-ndjson
// @Param        id  path  string  true  "artifact id (sha256:<hex>)"
// @Success      200  {string}  binary  "the artifact; headers ETag, Content-Length"
// @Success      206  {string}  binary  "the requested byte range"
// @Failure      404  {object}  api.ErrorResponse  "unknown artifact"
// @Failure      416  "range not satisfiable"
// @Failure      500  {object}  api.ErrorResponse  "artifact file unavailable"
// @Router       /cache/artifacts/{id} [get]
func (s *Server) handleCacheArtifact(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a, err := s.artifacts.GetByID(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "artifact not found")
		return
	}
	if err != nil {
		s.logger.Error("artifact: lookup failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	f, err := os.Open(a.Path)
	if err != nil {
		s.logger.Error("artifact: file unavailable", "id", id, "path", a.Path, "error", err)
		writeError(w, http.StatusInternalServerError, "artifact file unavailable")
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "artifact file unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("ETag", `"`+a.ID+`"`)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeContent(w, r, "", st.ModTime(), f) // HEAD, Range (206/416), If-None-Match (304)
}
```

Route: `mux.HandleFunc("GET /api/v1/cache/artifacts/{id}", s.handleCacheArtifact)` (a `GET` pattern also serves HEAD). `NewServer` gains `artifacts *store.ReplayArtifactRepo`; `main.go` constructs `artifactRepo := store.NewReplayArtifactRepo(database)` and passes it.

- [ ] **Step 4: Run and commit**

Run: `go test ./internal/api/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/api/cache_artifact_handler.go internal/api/cache_artifact_handler_test.go internal/api/server.go cmd/manteion/main.go
git commit -m "feat(api): GET /api/v1/cache/artifacts/{id} with HEAD, Range and ETag

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Freeze config and replay-set ref from the frozen phase

**Files:**
- Modify: `internal/store/experiment_repo.go` (after `ActiveCacheBoxPhaseForService`, :571)
- Create: `internal/desired/freeze.go` (moved `freezeDelayRequest` from `internal/orchestrator/runner.go:750-769`), `internal/desired/freeze_test.go` (moved from `internal/orchestrator/freeze_test.go`), `internal/desired/phase_sources.go`, `internal/desired/phase_sources_test.go`
- Modify: `internal/orchestrator/runner.go` (`freezeServices` calls `desired.FreezeDelayRequest` until Task 5 deletes it), `cmd/manteion/main.go`

**Interfaces:**
- Consumes: `ListRunningPhases` (`experiment_repo.go:491-511`), `model.ResolveKeyStrategy/KeyStrategyVersion`, `store.ReplayArtifactRepo.ForPhaseService`, plan 1's `desired.FreezeSource`/`ReplaySetSource`.
- Produces:

```go
// store
// FrozenServiceForService returns the running phase that freezes service and
// its cache-box config (the replay half of ActiveCacheBoxPhaseForService), or
// nil, nil when the service is not frozen right now.
func (r *ExperimentRepo) FrozenServiceForService(ctx context.Context, service string) (*model.ExperimentPhase, *model.CacheBoxConfig, error)

// desired
const ArtifactURLPrefix = "/api/v1/cache/artifacts/"
func FreezeDelayRequest(p *model.ExperimentPhase, fs model.CacheBoxConfig) atroposdk.DelayRequest // body of the old freezeDelayRequest
type FrozenPhaseSource interface { FrozenServiceForService(ctx context.Context, service string) (*model.ExperimentPhase, *model.CacheBoxConfig, error) }
type ArtifactSource interface { ForPhaseService(ctx context.Context, phaseID, service string) (*model.ReplayArtifact, error) } // nil, nil when none
type PhaseSources struct { Phases FrozenPhaseSource; Artifacts ArtifactSource }
func (s *PhaseSources) FreezeConfigForService(ctx, service) (*atroposdk.DelayRequest, error) // FreezeSource
func (s *PhaseSources) ReplaySetForService(ctx, service) (*model.ReplaySetRef, error)       // ReplaySetSource
```

`ReplaySetForService` returns `nil, nil` when the service is not frozen or no artifact is assigned yet (the setup window before the artifact build; load has not started), else `&model.ReplaySetRef{ID: a.ID, ExperimentID: p.ExperimentID, PhaseID: p.ID, KeyStrategy: model.ResolveKeyStrategy(fs.KeyStrategy), Count: a.EntryCount, SHA256: a.W5SHA256, SizeBytes: a.SizeBytes, URL: ArtifactURLPrefix + a.ID}` — the pair is the frozen (isolation) phase, matching the synthesized replay rule's `CacheBoxContext`.

- [ ] **Step 1: Move the freeze test and write the failing source tests**

`git mv internal/orchestrator/freeze_test.go internal/desired/freeze_test.go`; change its package to `desired`, call `FreezeDelayRequest` (exported) and keep every assertion.

`internal/desired/phase_sources_test.go`:

```go
package desired

import (
	"context"
	"testing"

	"manteion-go/internal/model"
)

type fakeFrozen struct {
	phase *model.ExperimentPhase
	cfg   *model.CacheBoxConfig
}

func (f *fakeFrozen) FrozenServiceForService(context.Context, string) (*model.ExperimentPhase, *model.CacheBoxConfig, error) {
	return f.phase, f.cfg, nil
}

type fakeArtifacts struct{ byKey map[string]*model.ReplayArtifact }

func (f *fakeArtifacts) ForPhaseService(_ context.Context, phaseID, service string) (*model.ReplayArtifact, error) {
	return f.byKey[phaseID+"/"+service], nil
}

func TestPhaseSources_NotFrozen(t *testing.T) {
	s := &PhaseSources{Phases: &fakeFrozen{}, Artifacts: &fakeArtifacts{}}
	if cfg, err := s.FreezeConfigForService(context.Background(), "frontend"); err != nil || cfg != nil {
		t.Errorf("freeze = %v, %v; want nil", cfg, err)
	}
	if ref, err := s.ReplaySetForService(context.Background(), "frontend"); err != nil || ref != nil {
		t.Errorf("replay set = %v, %v; want nil", ref, err)
	}
}

func TestPhaseSources_FrozenPhase(t *testing.T) {
	mu, sigma := 1.5, 0.4
	p := &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"}
	cfg := &model.CacheBoxConfig{Service: "frontend", Mode: "replay_with_delay", KeyHeaders: []string{"x-tenant"},
		SyntheticDelay: &model.SyntheticDelayConfig{FitMu: &mu, FitSigma: &sigma}}
	s := &PhaseSources{Phases: &fakeFrozen{phase: p, cfg: cfg}, Artifacts: &fakeArtifacts{byKey: map[string]*model.ReplayArtifact{}}}

	fc, err := s.FreezeConfigForService(context.Background(), "frontend")
	if err != nil || fc == nil || fc.Mu != 1.5 || fc.Context == nil || fc.Context.PhaseID != "phase-iso" || fc.Context.KeyStrategy != "canonical_v2" {
		t.Fatalf("freeze = %+v, %v", fc, err)
	}
	if ref, _ := s.ReplaySetForService(context.Background(), "frontend"); ref != nil {
		t.Fatalf("replay set before the artifact is assigned = %+v, want nil", ref)
	}
	s.Artifacts.(*fakeArtifacts).byKey["phase-iso/frontend"] = &model.ReplayArtifact{ID: "sha256:aa", EntryCount: 12, W5SHA256: "w5", SizeBytes: 300}
	ref, err := s.ReplaySetForService(context.Background(), "frontend")
	if err != nil || ref == nil {
		t.Fatalf("replay set = %v, %v", ref, err)
	}
	if ref.ID != "sha256:aa" || ref.ExperimentID != "exp-1" || ref.PhaseID != "phase-iso" || ref.KeyStrategy != "canonical_v2" ||
		ref.Count != 12 || ref.SHA256 != "w5" || ref.SizeBytes != 300 || ref.URL != "/api/v1/cache/artifacts/sha256:aa" {
		t.Errorf("replay set = %+v", ref)
	}
}
```

Also extend `internal/desired/materializer_test.go` (plan 1): with `ReplaySets: &fakeReplaySets{ref: &model.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso"}}` the document carries `replay_set` for `inst("inline", "cachebox")` and not for `inst("inline")`, and the ETag hash differs between "with" and "without" the ref.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/desired/ -count=1`
Expected: compile error (`undefined: PhaseSources`, `FreezeDelayRequest`).

- [ ] **Step 3: Implement**

`internal/desired/freeze.go`: the body of `freezeDelayRequest` (`runner.go:750-769`) as `FreezeDelayRequest`. `internal/orchestrator/runner.go`: `freezeServices` calls `desired.FreezeDelayRequest(p, fs)`; delete the local function. `internal/desired/phase_sources.go` as in the Interfaces block (`FreezeConfigForService`: phase nil → `nil, nil`; else `req := FreezeDelayRequest(p, *fs); return &req, nil`).

`internal/store/experiment_repo.go`:

```go
func (r *ExperimentRepo) FrozenServiceForService(ctx context.Context, service string) (*model.ExperimentPhase, *model.CacheBoxConfig, error) {
	running, err := r.ListRunningPhases(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, p := range running {
		for i := range p.FrozenServices {
			if p.FrozenServices[i].Service == service {
				return p, &p.FrozenServices[i], nil
			}
		}
	}
	return nil, nil, nil
}
```

(same precedence as the replay branch of `ActiveCacheBoxPhaseForService` at :540-546; add a DB-gated case to `internal/api/sdk_recording_test.go`'s fixture or a store test asserting the isolation phase is returned once running and nil before).

`cmd/manteion/main.go`: `mat.Freeze` and `mat.ReplaySets` become `&desired.PhaseSources{Phases: experimentRepo, Artifacts: artifactRepo}` (one value for both fields); stop wiring `controller.IntentReader()` into the materializer. Delete `(*IntentTracker).FreezeConfigForService` (plan 1's adapter) now.

- [ ] **Step 4: Run and commit**

Run: `go test ./internal/desired/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/desired internal/store/experiment_repo.go internal/orchestrator/runner.go internal/orchestrator/freeze_test.go internal/atrocontrol/intent.go cmd/manteion/main.go
git commit -m "feat(desired): freeze_cfg and replay_set materialized from the frozen phase and its assigned artifact

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Orchestrator — artifacts at entry, bump-and-wait convergence, verdict before thaw

**Files:**
- Modify: `internal/orchestrator/orchestrator.go:41-116` (fields, `New`, options), `internal/orchestrator/runner.go` (`enterPhase` :129-247, `endPhase` :288-396, delete `pushPhaseRules` :429-473, `freezeServices` :731-743, `thawServices` :792-799, `preloadCacheEntries` :811-869, `verifyPreload` :891-909), `internal/orchestrator/drain.go:23-38`
- Delete: `internal/orchestrator/rules.go`, `internal/orchestrator/push_rules_test.go`
- Create: `internal/orchestrator/converge.go`, `internal/orchestrator/converge_test.go`
- Modify: `internal/orchestrator/fake_sdk_test.go`, `orchestrator_test.go:96-118`, `preload_gate_test.go:58-120`, `runner_test.go:21-78` and the plan-1 `TestEnterPhase_BumpsGenerationOnEveryEnter`, `verdict_test.go:13-21`, `harvest_test.go` (every `newFakeSDK(t, true)`), `failure_reason_test.go:160-177` (unchanged assertions)
- Modify: `cmd/manteion/main.go` (`MANTEION_CONVERGE_TIMEOUT`, `New` args)

**Interfaces:**
- Consumes: `cachestore.BuildArtifact`, `store.ReplayArtifactRepo.Upsert/ForPhaseService`, `bumpDesired` (plan 1), `controller.InstancesForService` (live filter, `atrocontrol/controller.go:90-102`), `SDKInstance.ObservedGeneration/ReplaySetID/ReplaySetState/ObservedErrors`.
- Produces:

```go
// orchestrator.go
// New(experiments, rules, faults, workloads, workflows, controller, prom, zeusClient, cs, faultEvents, artifacts *store.ReplayArtifactRepo, gens *desired.Generations, logger)
func (o *Orchestrator) WithConvergeTimeout(d time.Duration)      // MANTEION_CONVERGE_TIMEOUT, default 30s
func (o *Orchestrator) WithConvergePollInterval(d time.Duration) // test seam, default 500ms

// converge.go
// waitConverged blocks until every live instance of each service in targets
// reports observed_generation >= target and, for services in artifacts, holds
// that artifact committed; a frozen service with no live instance, or an
// instance reporting the artifact failed, errors immediately. On timeout the
// error lists every lagging instance.
func (o *Orchestrator) waitConverged(ctx context.Context, targets map[string]uint64, artifacts map[string]string, timeout time.Duration) error
func (o *Orchestrator) laggingInstances(ctx context.Context, targets map[string]uint64, artifacts map[string]string) (lagging []string, fatal error)
func (o *Orchestrator) awaitConvergedNonFatal(ctx context.Context, p *model.ExperimentPhase, targets map[string]uint64)

// runner.go
func (o *Orchestrator) buildReplayArtifacts(ctx context.Context, p *model.ExperimentPhase) (map[string]string, error) // service → artifact id; the old preload gate's refusals keep their texts
func (o *Orchestrator) assignedArtifacts(ctx context.Context, p *model.ExperimentPhase) (map[string]string, error)   // resume: rows from the fresh start
```

Test helpers (`fake_sdk_test.go`): `newFakeSDK(t) *fakeSDK` serving only `GET /cachebox/fidelity`; `registerSDK(t, service, addr) string` (returns the instance id); `ackGenerations(t, o, instanceID, service, artifact string)` — a goroutine that every 20 ms records the service's current generation as observed (`testSDKRepo.RecordPoll`) and, when `artifact != ""`, reports it committed (`testSDKRepo.RecordStatus`); stops at cleanup.

- [ ] **Step 1: Write the failing converge tests**

`internal/orchestrator/converge_test.go`:

```go
package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// seedBaseline completes a baseline with n recorded entries for service.
func seedBaseline(t *testing.T, o *Orchestrator, expID, service string, n int) *model.ExperimentPhase {
	t.Helper()
	base := mkCompletedBaseline(t, expID, 0)
	entries := make([]atroposdk.CacheBoxWireEntry, n)
	for i := range entries {
		entries[i] = atroposdk.CacheBoxWireEntry{Key: "k" + string(rune('a'+i)), StatusCode: 200, Body: []byte("v")}
	}
	if err := o.cacheStore.Append(expID, base.ID, service, entries); err != nil {
		t.Fatal(err)
	}
	return base
}

func TestEnterPhase_ConvergesWhenInstancesAck(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 2)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	sdk := newFakeSDK(t)
	instID := registerSDK(t, "frontend", sdk.server.URL)

	// The artifact id is only known once built; ack whatever is assigned.
	go func() {
		waitFor(t, "artifact assigned", 3*time.Second, func() bool {
			a, _ := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
			return a != nil
		})
		a, _ := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
		ackGenerations(t, o, instID, "frontend", a.ID)
	}()

	if err := o.enterPhase(ctx, iso, true); err != nil {
		t.Fatalf("enterPhase: %v", err)
	}
	a, err := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
	if err != nil || a == nil || a.EntryCount != 2 || a.ExperimentID != exp.ID || !strings.HasPrefix(a.ID, "sha256:") {
		t.Fatalf("artifact = %+v, %v", a, err)
	}
	if got := getPhase(t, iso.ID).Status; got != "running" {
		t.Fatalf("phase status = %q, want running", got)
	}
}

func TestEnterPhase_ReplaySetNotCommittedAborts(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	ackGenerations(t, o, instID, "frontend", "") // generation yes, replay set never

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "converge gate") || !strings.Contains(err.Error(), instID) || !strings.Contains(err.Error(), "replay_set") {
		t.Fatalf("enterPhase err = %v, want a converge-gate abort naming %s and its replay_set state", err, instID)
	}
	if got := getPhase(t, iso.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, instID) {
		t.Fatalf("phase = %s / %q, want failed with the lagging instance", got.Status, got.FailureReason)
	}
}

func TestEnterPhase_ReplaySetFailedAbortsEarly(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithConvergeTimeout(5 * time.Second)
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	go func() {
		waitFor(t, "artifact assigned", 3*time.Second, func() bool {
			a, _ := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
			return a != nil
		})
		a, _ := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
		gen, _ := o.generations.Get(ctx, "frontend")
		_ = testSDKRepo.RecordPoll(ctx, instID, gen)
		_ = testSDKRepo.RecordStatus(ctx, instID, model.ObservedStatus{AppliedGeneration: gen, Errors: []string{"fetch: 416"},
			ReplaySet: &model.ReplaySetStatus{ID: a.ID, State: "failed"}})
	}()
	start := time.Now()
	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "failed") || !strings.Contains(err.Error(), "fetch: 416") {
		t.Fatalf("enterPhase err = %v, want an early abort carrying the instance's error", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatalf("took %v: a failed replay set must abort before the converge timeout", time.Since(start))
	}
}

func TestEnterPhase_FrozenServiceWithoutInstancesAborts(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "no live instances") {
		t.Fatalf("enterPhase err = %v, want 'no live instances' (INV-4)", err)
	}
}

func TestEnterPhase_LaggingInstanceFailsWithList(t *testing.T) {
	// A fault-rule phase (no freeze): a registered instance that never polls
	// the new generation fails the phase, named with observed/target.
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	ph := mkRunningIsolation(t, exp.ID, "", 0) // adapt: a plain running phase with one attached rule for "frontend"
	_ = ph
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	err := o.enterPhase(ctx, ph, true)
	if err == nil || !strings.Contains(err.Error(), instID) || !strings.Contains(err.Error(), "observed=") {
		t.Fatalf("enterPhase err = %v, want the lagging instance with observed/target", err)
	}
}

func TestFinishPhase_VerdictBeforeBump(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	mkCompletedBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	g0, _ := o.generations.Get(ctx, "frontend")

	sdk := newFakeSDK(t)
	sdk.fidelity = cleanSnapshot()
	// The fake records the service generation at the moment of the fidelity pull.
	sdk.onFidelity = func() { g, _ := o.generations.Get(context.Background(), "frontend"); sdk.genAtPull.Store(g) }
	registerSDK(t, "frontend", sdk.server.URL)

	if !o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("finishPhase returned false")
	}
	if got := sdk.genAtPull.Load(); got != g0 {
		t.Fatalf("fidelity pulled at generation %d, want %d (before the thaw bump)", got, g0)
	}
	if g1, _ := o.generations.Get(ctx, "frontend"); g1 <= g0 {
		t.Fatalf("phase exit did not bump: %d → %d", g0, g1)
	}
	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil || v.Verdict != "VALID" {
		t.Fatalf("verdict = %+v, %v", v, err)
	}
}
```

For `TestEnterPhase_LaggingInstanceFailsWithList`, build the phase the way `push_rules_test.go:22-56` did (a running phase with an attached enabled rule targeting `frontend`, seeded spec `spec-...`) — copy that fixture into the test and delete `push_rules_test.go`. The `fakeSDK` gains `onFidelity func()` and `genAtPull atomic.Uint64` (called from the fidelity handler before writing the snapshot).

- [ ] **Step 2: Run to verify they fail**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -run 'TestEnterPhase_Converges|TestEnterPhase_ReplaySet|TestEnterPhase_Frozen|TestEnterPhase_Lagging|TestFinishPhase_VerdictBeforeBump' -count=1`
Expected: compile errors (`o.artifacts undefined`, `ackGenerations undefined`, `newFakeSDK` arity).

- [ ] **Step 3: Implement the orchestrator changes**

`orchestrator.go`: fields `artifacts *store.ReplayArtifactRepo`, `convergeTimeout time.Duration` (30 s), `convergePollInterval time.Duration` (500 ms); `New` gains `artifacts` and the two options exist as in the Interfaces block.

`converge.go`:

```go
func (o *Orchestrator) waitConverged(ctx context.Context, targets map[string]uint64, artifacts map[string]string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		lagging, err := o.laggingInstances(ctx, targets, artifacts)
		if err != nil {
			return err
		}
		if len(lagging) == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%d instance(s) still lagging after %s: %s", len(lagging), timeout, strings.Join(lagging, "; "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.convergePollInterval):
		}
	}
}

func (o *Orchestrator) laggingInstances(ctx context.Context, targets map[string]uint64, artifacts map[string]string) ([]string, error) {
	var lagging []string
	for _, svc := range slices.Sorted(maps.Keys(targets)) {
		instances, err := o.controller.InstancesForService(ctx, svc) // live: alive|stale
		if err != nil {
			return nil, fmt.Errorf("list instances for %q: %w", svc, err)
		}
		artifact, frozen := artifacts[svc]
		if frozen && len(instances) == 0 {
			return nil, fmt.Errorf("%s: no live instances to hold the replay set", svc)
		}
		for _, inst := range instances {
			switch {
			case inst.ObservedGeneration < targets[svc]:
				lagging = append(lagging, fmt.Sprintf("%s(%s): observed=%d target=%d", inst.ID, svc, inst.ObservedGeneration, targets[svc]))
			case frozen && inst.ReplaySetID == artifact && inst.ReplaySetState == "failed":
				return nil, fmt.Errorf("%s(%s): replay set %s failed: %v", inst.ID, svc, artifact, inst.ObservedErrors)
			case frozen && (inst.ReplaySetID != artifact || inst.ReplaySetState != "committed"):
				lagging = append(lagging, fmt.Sprintf("%s(%s): replay_set=%s/%s target=%s/committed", inst.ID, svc, inst.ReplaySetID, inst.ReplaySetState, artifact))
			}
		}
	}
	return lagging, nil
}

func (o *Orchestrator) awaitConvergedNonFatal(ctx context.Context, p *model.ExperimentPhase, targets map[string]uint64) {
	if err := o.waitConverged(ctx, targets, nil, o.convergeTimeout); err != nil {
		o.logger.Warn("orchestrator: fleet did not converge at phase exit; next poll converges stragglers",
			"phase_id", p.ID, "error", err)
	}
}
```

`runner.go`:
- `enterPhase`: after loading the experiment and plan 1's bump block is removed, insert

```go
	// Replay sets (INV-4): a fresh frozen phase builds one immutable artifact
	// per frozen service from the baseline recording and assigns it; a resume
	// re-reads the assignment. Both refusals (no baseline, zero entries,
	// degraded recording) keep the preload-gate wording.
	var artifacts map[string]string
	if len(p.FrozenServices) > 0 {
		var err error
		if fresh {
			artifacts, err = o.buildReplayArtifacts(ctx, p)
		} else {
			artifacts, err = o.assignedArtifacts(ctx, p)
		}
		if err != nil {
			return fail(fmt.Errorf("orchestrator: preload gate: %w", err))
		}
	}
	// Inputs are written (phase running, rules attached, artifacts assigned,
	// freeze config derived from the phase). Bump every service once and wait
	// for the fleet to observe it (contract §4) — this replaces freeze, rule
	// push and preload; a timeout fails the phase naming the laggards.
	targets := o.bumpDesired(ctx, p)
	if err := o.waitConverged(ctx, targets, artifacts, o.convergeTimeout); err != nil {
		return fail(fmt.Errorf("orchestrator: converge gate: %w", err))
	}
	if fresh {
		o.recordFreezeEvents(ctx, p)
		o.recordRuleEvents(ctx, p)
	}
```

  replacing the preload gate (:160-170), freeze (:172-180) and push (:182-187) blocks. `buildReplayArtifacts` is `preloadCacheEntries` (:811-869) with the per-service loop body replaced by `info, err := o.cacheStore.BuildArtifact(baseline.ExperimentID, baseline.ID, fs.Service)` → `o.artifacts.Upsert(ctx, &model.ReplayArtifact{ID: info.ID, ExperimentID: p.ExperimentID, PhaseID: p.ID, Service: fs.Service, EntryCount: info.Count, W5SHA256: info.W5SHA256, SizeBytes: info.SizeBytes, Path: info.Path})` → `out[fs.Service] = info.ID`; keep the baseline/degraded/zero-entries/`verifyRecordedStrategy` checks and texts. `assignedArtifacts`: `ForPhaseService` per frozen service; `nil` → `fmt.Errorf("no replay set assigned for frozen service %q (resume before a fresh start?)", fs.Service)`.
- `endPhase`: after `stopPhaseRuns` (:346) the sequence becomes: verdict (`if status == "completed" && len(p.FrozenServices) > 0 { fidelity = o.collectFidelityVerdict(ctx, p) }` — BEFORE any bump: instances still hold the replay set) → `targets := o.bumpDesired(ctx, p)` → `o.awaitConvergedNonFatal(ctx, p, targets)` → `EndOpenForPhase` → harvest → recompute → advance. Delete the `PushRules(nil)` loop (:360-365) and the `thawServices` call (:376); delete `pushPhaseRules`, `freezeServices`, `thawServices`, `preloadCacheEntries`, `verifyPreload`, `rules.go`; keep `verifyRecordedStrategy`, `baselinePhase`, `recordFreezeEvents`, `recordRuleEvents`. Update the `StartPhase` doc comment (:21-28) and the `atrocontrol` import (no longer needed in `runner.go`).
- `drain.go` `runDrainBarrier`: `targets := o.bumpDesired(ctx, p); o.awaitConvergedNonFatal(ctx, p, targets)` before `awaitDrain` (the drain report remains the real acknowledgement; this keeps the contract's bump-and-wait wording and costs nothing when instances are quick).

`cmd/manteion/main.go`: `orch.WithConvergeTimeout(envDurationOr("MANTEION_CONVERGE_TIMEOUT", 30*time.Second))`; pass `artifactRepo` to `orchestrator.New`.

- [ ] **Step 4: Adapt the test harness**

`fake_sdk_test.go`: strip the preload/rules/delay/clear routes and the `commitOK`/`freezeHit`/`pushed` state; `newFakeSDK(t *testing.T) *fakeSDK`; add `onFidelity func()` + `genAtPull atomic.Uint64` invoked in the fidelity handler; `registerSDK` returns `inst.ID`; add `ackGenerations`:

```go
// ackGenerations simulates a converging instance: every 20ms it records the
// service's current generation as observed and, when artifact != "", reports
// the replay set committed under it. Stops at test cleanup.
func ackGenerations(t *testing.T, o *Orchestrator, instanceID, service, artifact string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				gen, err := o.generations.Get(ctx, service)
				if err != nil {
					continue
				}
				_ = testSDKRepo.RecordPoll(ctx, instanceID, gen)
				if artifact != "" {
					_ = testSDKRepo.RecordStatus(ctx, instanceID, model.ObservedStatus{AppliedGeneration: gen,
						ReplaySet: &model.ReplaySetStatus{ID: artifact, State: "committed"}})
				}
			}
		}
	}()
}
```

`orchestrator_test.go` `newOrch`: `New(..., store.NewReplayArtifactRepo(testDB), gens, logger)`, `o.WithConvergeTimeout(300 * time.Millisecond)`, `o.WithConvergePollInterval(20 * time.Millisecond)`.
`preload_gate_test.go`: keep `TestEnterPhase_NoBaselineAbortsFrozenPhase` and `TestEnterPhase_ZeroEntriesAborts` unchanged; delete `TestEnterPhase_PreloadMismatchAborts` (superseded by the converge tests). `runner_test.go`: delete `TestFinishPhase_StopsLoadBeforeThawAndClear` (its "clear_rules"/"thaw" routes no longer exist; the ordering it protected — load stops before the bump — is pinned by `TestFinishPhase_VerdictBeforeBump` plus a `TestFinishPhase_StopsLoadBeforeBump` that records the zeus stop time and subscribes to `o.generations.Broker()` for `frontend`, asserting the `rules_changed` event arrives after the stop). Plan 1's `TestEnterPhase_BumpsGenerationOnEveryEnter`: the resume of the frozen phase now needs an assigned artifact and an acking instance — seed the baseline (`seedBaseline`), `o.buildReplayArtifacts(ctx, iso)` before the resume, register + `ackGenerations(..., artifactID)`. `verdict_test.go`/`harvest_test.go`: `newFakeSDK(t)`; nothing else (exit convergence is non-fatal and bounded by 300 ms in tests). `failure_reason_test.go:160-177` stays green (the texts "preload gate" and "baseline" are kept).

- [ ] **Step 5: Run the orchestrator suite**

Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -count=1 -p 1 -timeout 300s`
Expected: PASS.
Run: `gofmt -l . ; go vet ./... && go test ./...`
Expected: clean (the atrocontrol push methods still exist until Task 9; the orchestrator no longer calls them).

- [ ] **Step 6: Commit**

```bash
git add internal/orchestrator cmd/manteion/main.go
git commit -m "feat(orchestrator): phase entry builds replay artifacts, bumps once and waits for fleet convergence; verdict before the thaw bump; push/freeze/preload removed

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Kill switch — no rules, no faults, one bump

**Files:**
- Modify: `internal/api/sdk_instance_handler.go:18-29, 55-59, 117-188`, `internal/api/server.go` (field `serviceFaultConfigs serviceFaultConfigStore` = `faultConfigs`), `internal/api/sdk_instance_handler_test.go:143-145, 304-482, 637-684`

**Interfaces:**
- Produces: `type serviceFaultConfigStore interface { ListActiveForService(ctx, service string) ([]*model.FaultConfig, error); MarkCancelled(ctx, id string) error }`; `KillSwitchResult.CancelledFaultConfigIDs []string json:"cancelled_fault_config_ids"`; handler: disable every enabled rule (as today), cancel every active fault config for the service, then — when anything changed — `s.bumpGeneration(ctx, inst.Service)` once (the SSE nudge rides on the bump; each rule update already bumped in-tx, this final bump guarantees a wake after the last write).

- [ ] **Step 1: Write the failing tests**

In `sdk_instance_handler_test.go`: a `fakeServiceFaultConfigs{active []*model.FaultConfig; cancelled []string; cancelErr map[string]error}`; `newSDKInstanceServer(insts, rules, phases, configs)` also sets `generations: desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())`. Add cases to `TestHandleInstanceKillSwitch`: "cancels active fault configs for the service and bumps once" (two active configs for cartservice, one for checkoutservice → `cancelled_fault_config_ids` = the two, generation of `cartservice` = 2, checkout untouched); "cancel failure → 500, rules still disabled"; the idempotent test asserts `cancelled_fault_config_ids:[]` and no second bump. In `TestSDKInstanceHandlers_DB`: create + fire a fault config for `svc` before the kill switch, assert it is `cancelled` afterwards and the generation moved by 3 (two rule updates + the final bump).

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/api/ -run TestHandleInstanceKillSwitch -count=1`
Expected: compile error (`undefined: fakeServiceFaultConfigs` field / arity).

- [ ] **Step 3: Implement**

After the rule loop in `handleInstanceKillSwitch`: `configs, err := s.serviceFaultConfigs.ListActiveForService(ctx, inst.Service)` (error → 500 as for rules); for each `MarkCancelled` (`ErrNotFound` skip, other error `failed++`); `if len(disabled)+len(cancelled) > 0 { s.bumpGeneration(ctx, inst.Service) }`; include `CancelledFaultConfigIDs: cancelled` (never nil) in the result; update the swag description (“and cancels every active fault config for the service; bumps the service generation once”).

- [ ] **Step 4: Run and commit**

Run: `go test ./internal/api/ -count=1 && MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/api/ -run TestSDKInstanceHandlers_DB -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/api
git commit -m "feat(api): kill switch cancels active fault configs and bumps the service generation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Ingest dedupes on `(experiment, phase, instance, epoch, batch_seq)`

**Files:**
- Modify: `internal/cachestore/store.go:137-173`, `internal/cachestore/store_test.go` (nine `Ingest` calls), `internal/api/cache_handler.go:26-39, 105`, `internal/api/cache_handler_test.go`, `internal/orchestrator/drain_test.go:30-41`, `internal/orchestrator/verdict_test.go:146-153`

**Interfaces:**
- Produces: `(*Store).Ingest(experimentID, phaseID, service, instance string, epoch int64, batchSeq int, entries []atroposdk.CacheBoxWireEntry) (IngestResult, error)` with dedupe key `instance + "\x00" + strconv.FormatInt(epoch, 10) + "\x00" + strconv.Itoa(batchSeq)`; `ingestEnvelope.Epoch int64 json:"epoch,omitempty"` (absent → 0, the old key).

- [ ] **Step 1: Write the failing tests**

`internal/cachestore/store_test.go`:

```go
// TestIngest_EpochSeparatesRestarts pins desired-state v1 ingest: a restarted
// instance restarts batch_seq at 1 under a new epoch, so (instance, epoch, seq)
// must not collide with the previous process's batches; the same epoch and seq
// is still a duplicate, and epoch 0 (old clients) behaves as before.
func TestIngest_EpochSeparatesRestarts(t *testing.T) {
	s := New(t.TempDir())
	e := []atroposdk.CacheBoxWireEntry{{Key: "k1"}}
	if r, _ := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 100, 1, e); r.Duplicate {
		t.Fatal("first batch reported duplicate")
	}
	if r, _ := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 200, 1, e); r.Duplicate || r.Accepted != 1 {
		t.Fatalf("new epoch, same seq = %+v, want accepted", r)
	}
	if r, _ := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 200, 1, e); !r.Duplicate {
		t.Fatal("same epoch and seq must be a duplicate")
	}
	if got := s.ReceivedCount("exp-1", "phase-1", "frontend", "inst-a"); got != 2 {
		t.Fatalf("received = %d, want 2", got)
	}
}
```

`internal/api/cache_handler_test.go`: `TestIngest_EpochInDedupeKey` — same `BatchSeq: 1` with `Epoch: 1` then `Epoch: 2` → both `accepted:1`; a body without `epoch` twice → second is `duplicate:true`.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/cachestore/ ./internal/api/ -run 'TestIngest_Epoch' -count=1`
Expected: compile error (arity).

- [ ] **Step 3: Implement**

Add `epoch int64` to `Ingest` and the key; `ingestEnvelope.Epoch`; pass `env.Epoch` at `cache_handler.go:105`; update the nine `store_test.go` calls, `ingestN` and `ingestSig` with `0`. Update the `ingestEnvelope` comment (dedupe key now includes `epoch`, the instance's start time in unix nanoseconds; 0 for old clients).

- [ ] **Step 4: Run and commit**

Run: `gofmt -l . ; go vet ./... && go test ./... && MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -count=1 -p 1`
Expected: clean, PASS.

```bash
git add internal/cachestore internal/api internal/orchestrator
git commit -m "feat(cachestore): ingest dedupes on (experiment, phase, instance, epoch, batch_seq)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: User-authored cache-box rules carry the attached phase's context

**Decision:** compile with a context derived from the running phase the rule is attached to; an unattached cache-box rule keeps compiling without a context (as today). Rejecting at save would break the leak-safe create → attach → enable lifecycle (`rule_handler.go:19-22` — a rule is created before it can be attached) and the UI/expctl flows built on it, while a rule served only through a running phase (`ForService`, `rule_repo.go:152-170`) already has exactly one such phase to take the pair from. The synthesized rule remains the orchestrator's provenance; this only stops a user-authored cache-box rule from recording nowhere / replaying without an ownership check.

**Files:**
- Modify: `internal/model/rule.go:18-34`, `internal/store/rule_repo.go` (`ForService` :152-170 + a `scanServedRules`), `internal/ruleconv/ruleconv.go:126-131`, `internal/ruleconv/ruleconv_test.go`, `internal/store/rule_repo_test.go`

**Interfaces:**
- Produces: `model.Rule.ServedPhaseID string json:"-"` and `ServedExperimentID string json:"-"` ("set only by `RuleRepo.ForService`: the running phase the rule is served through; empty for unattached rules"); `ForService` selects them with `LEFT JOIN LATERAL (SELECT p.id, p.experiment_id FROM phase_rules pr JOIN experiment_phases p ON p.id = pr.phase_id WHERE pr.rule_id = r.id AND p.status = 'running' ORDER BY p.started_at DESC NULLS LAST LIMIT 1) rp ON true`; `compileRule` case `"cachebox"` sets `cr.CacheBox.Context = &atroposdk.CacheBoxContext{ExperimentID: r.ServedExperimentID, PhaseID: r.ServedPhaseID, KeyStrategy: strat, StrategyVersion: model.KeyStrategyVersion(strat)}` with `strat := model.ResolveKeyStrategy(r.Action.CacheBox.KeyStrategy)` when `r.ServedPhaseID != ""`.

- [ ] **Step 1: Write the failing tests**

`internal/ruleconv/ruleconv_test.go`: `TestCompileRule_CacheBoxAttachedPhaseContext` — a cachebox rule with `ServedPhaseID: "phase-iso", ServedExperimentID: "exp-1"`, `KeyStrategy: "exact"` compiles to `CacheBox.Context{ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "exact", StrategyVersion: 1}`; the same rule without served fields compiles with `Context == nil`. `internal/store/rule_repo_test.go`: DB-gated `TestRuleRepo_ForServiceCarriesRunningPhase` — a rule attached to a pending phase is not served; once the phase is `running` it is served with `ServedPhaseID == phase.ID`; an unattached rule has empty served fields.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/ruleconv/ -count=1`
Expected: compile error (`unknown field ServedPhaseID`).

- [ ] **Step 3: Implement, run, commit**

Run: `go test ./internal/ruleconv/ -count=1 && MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test ./internal/store/ -count=1 && gofmt -l . ; go vet ./... && go test ./...`
Expected: PASS, clean.

```bash
git add internal/model/rule.go internal/store/rule_repo.go internal/store/rule_repo_test.go internal/ruleconv
git commit -m "feat(ruleconv): user-authored cache-box rules carry the running phase they are attached to as their context

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Delete the push machinery, re-route the frozen policy engine, docs, OpenAPI

**Files:**
- Delete: `internal/atrocontrol/{rules.go,freeze.go,fault.go,preload.go,preload_test.go,intent.go,fanout.go,errors.go}`
- Modify: `internal/atrocontrol/controller.go` (drop `intent`, `preloadTx`, `IntentReader`, `resolveInstance`, `logFanout`), `options.go` (drop `preloadTx`, `WithPreloadTransport`, `callOpts.runID`, `WithRunID`), `controller_test.go` (keep `TestStatusByService`; rewrite `TestNoInstancesError` on `StatusByService`; delete the freeze/inject/push/partial-failure/log-fanout tests)
- Modify: `internal/policy/engine.go:46-77, 195-254`, `internal/policy/engine_test.go:60-91, 154-300`
- Modify: `cmd/manteion/main.go:75-88, 111, 123-127`
- Modify: `docs/sdk-liveness.md`, `README.md:30-57`, `CLAUDE.md` (package table, "Intent tracker"/"Fanout concurrency" bullets, env table), `docs/decisions/2026-06-policy-engine-freeze.md`
- Regenerate `docs/swagger.{yaml,json}`

**Interfaces:**
- Produces: `policy.New(policies policyLister, rules ruleStore, prom *promql.Client, logger *slog.Logger)` with `type ruleStore interface { Get(ctx, id string) (*model.Rule, error); Update(ctx, rule *model.Rule) error; EnabledForService(ctx, service string) ([]*model.Rule, error) }`; `push_rules` = enable each listed rule through `Update` (bumps in-tx), `clear_rules` = disable every enabled rule for the service; `attack`/`cachebox_mode_change` unchanged (logged no-ops). `atrocontrol.Controller` keeps `New(tx, resolver, opts...)`, `StatusByService`, `InstancesForService`, `FetchFidelity`, `FilterLive/FilterAll`, `RepoResolver`.

- [ ] **Step 1: Rewrite the policy engine tests**

Replace `fakeSDKServer`/`buildController` with `fakeRuleStore{rules map[string]*model.Rule; updates []model.Rule}` and `updateCount()`; the five tests assert the number of `Update` calls (and `Enabled` flips) instead of `POST /admin/rules` counts: `TestConditionFires_AboveThreshold` → 1 update enabling the listed rule; `TestClearRulesAction_*` → every enabled rule of the service updated to `Enabled=false`.

Run: `go test ./internal/policy/ -count=1`
Expected: compile error (`New` arity / `controller` field).

- [ ] **Step 2: Implement the engine change and the deletions**

`engine.go`: fields `rules ruleStore`, drop `faults`/`controller`; `doPushRules` → for each id `Get`, `Enabled = true`, `UpdatedAt = time.Now()`, `Update`; `clear_rules` → `EnabledForService` → disable each; delete `loadCompiledRules`. Then delete the atrocontrol files and trim `controller.go`/`options.go`; `go build ./...` names every remaining reference (`main.go` preload client / `WithPreloadTransport` / `MANTEION_PRELOAD_TIMEOUT` → delete; `policy.New` args; `README`/`CLAUDE` mentions are prose).

Run: `go test ./internal/policy/ ./internal/atrocontrol/ -count=1 && gofmt -l . ; go vet ./... && go build ./... && go test ./...`
Expected: PASS, clean.

- [ ] **Step 3: Docs**

- `docs/sdk-liveness.md`: add "## Observed state (desired-state v1)" — `observed_generation` from every poll's `version`, `observed_etag`/`observed_unsupported`/`observed_errors`/`replay_set_id`/`replay_set_state`/`observed_at` from `POST /api/v1/sdk/status`, exposed on `GET /api/v1/sdk/instances[/{id}]`; the orchestrator's convergence barrier (`MANTEION_CONVERGE_TIMEOUT`, default 30 s) reads them; a re-register resets them. Note that `status` liveness is unchanged and that a `stale` instance still counts as live for convergence (so a slow poller can fail a phase entry — set the SDK poll interval accordingly).
- `README.md`: orchestrator row (`enterPhase`: artifacts → bump → converge; `finishPhase`: verdict before the bump), `internal/atrocontrol/` row (status, fidelity pulls, live-instance resolver), `internal/atropos/` row, `internal/desired/` row; wire contracts: Preload → "Replay sets — immutable artifacts at `GET /api/v1/cache/artifacts/{id}` (HEAD, Range), pulled by the instance; `replay_set` in the poll carries id, pair, count, W5 digest", Ingest → add `epoch`.
- `CLAUDE.md`: drop the "Intent tracker" and "Fanout concurrency" bullets, add "Desired state" (generation + ETag, convergence barrier), env table: add `MANTEION_CONVERGE_TIMEOUT` (30s), remove `MANTEION_PRELOAD_TIMEOUT` if listed, note `internal/desired/`.
- `docs/decisions/2026-06-policy-engine-freeze.md`: append "2026-10: push_rules/clear_rules now route through rule CRUD (enable/disable), which bumps the service generation; the engine stays opt-in."

- [ ] **Step 4: OpenAPI and final verification**

Run: `make openapi && make openapi-check`
Expected: spec regenerated (new `/cache/artifacts/{id}`, `cancelled_fault_config_ids`, ingest `epoch`), check exits 0.
Run: `gofmt -l . ; go vet ./... && go build ./... && go test ./... -race`
Expected: clean.
Run: `MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test -count=1 -timeout 300s -p 1 ./...`
Expected: `ok` for every package.

- [ ] **Step 5: Commit**

```bash
git add -A internal/atrocontrol internal/policy cmd/manteion/main.go docs README.md CLAUDE.md
git commit -m "refactor: remove atrocontrol push/freeze/fault/preload and the intent tracker; policy engine routes through rule CRUD; desired-state docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

- **Spec coverage.** §2 artifacts (T1-T3), `replay_set` with pair (T4), ingest `epoch` (T7); §4 phase entry bump-and-wait with lagging list and `MANTEION_CONVERGE_TIMEOUT` (T5), exit/drain/teardown non-fatal (T5), kill switch (T6), removals (T9), fidelity pull and drain barrier unchanged (T5 keeps `collectFidelityVerdict`, `awaitDrain`). Freeze/thaw and artifact-assignment bumps happen through `bumpDesired` at entry/exit (the inputs are written before the bump).
- **Type consistency.** `model.ReplayArtifact` fields (`EntryCount`, `W5SHA256`, `SizeBytes`, `Path`) are used identically in T1, T2 (`ArtifactInfo` → artifact), T4 (`ReplaySetRef`) and T5. `newFakeSDK(t)`, `registerSDK(...) string`, `ackGenerations` are the names every T5 test uses. `New(..., artifacts, gens, logger)` matches `newOrch` and `main.go`.
- **Review Focus pins:** 1 → T5 `TestFinishPhase_VerdictBeforeBump`; 2 → T5 `TestEnterPhase_FrozenServiceWithoutInstancesAborts`; 3 → T5 `TestEnterPhase_ReplaySetFailedAbortsEarly`; 4 and 5 → T3.
- **Known gaps / decisions for the human:** (a) artifact files are never deleted (content-addressed, small; an operator can prune `{CACHE_DIR}/artifacts`); (b) the drain barrier gains a bounded non-fatal converge wait before the report gate — worst case adds `MANTEION_CONVERGE_TIMEOUT` to a degraded drain; (c) a fresh registration mid-wait resets `observed_generation` to 0, so the barrier waits for that instance's first poll — intended; (d) `internal/atropos` client methods for rules/faults/preload are left in place (no non-test callers) as a follow-up cleanup.
