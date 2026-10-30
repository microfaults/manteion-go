package store

import (
	"context"
	"errors"
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

	// The report is a full snapshot, so one carrying a replay_set overwrites a
	// row that already holds one: the same artifact going fetching -> committed.
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{
		AppliedGeneration: 7, Etag: "7-abcdef0123456789",
		Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}},
		Errors:      []string{"rule x: decode: boom"},
		ReplaySet:   &model.ReplaySetStatus{ID: "sha256:aa", State: "committed"},
	}); err != nil {
		t.Fatalf("record status (commit): %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "committed" ||
		len(got.ObservedUnsupported) != 1 || len(got.ObservedErrors) != 1 {
		t.Errorf("after commit report: %+v", got)
	}

	// A report without replay_set means the instance holds none (after a thaw or
	// phase end): both replay columns clear, and so do the empty lists and the
	// etag while the generation moves on. Keeping the stored committed state
	// would let a recurring artifact id (a content hash) look already held.
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{AppliedGeneration: 8}); err != nil {
		t.Fatalf("record status 2: %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.ReplaySetID != "" || got.ReplaySetState != "" || len(got.ObservedUnsupported) != 0 || len(got.ObservedErrors) != 0 ||
		got.ObservedGeneration != 8 || got.ObservedEtag != "" {
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
	if err := repo.RecordPoll(ctx, "inst-missing", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("RecordPoll on an unknown instance = %v, want ErrNotFound", err)
	}
	if err := repo.RecordStatus(ctx, "inst-missing", model.ObservedStatus{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("RecordStatus on an unknown instance = %v, want ErrNotFound", err)
	}
}

// TestSDKRepo_RecordPollIsLivenessAndGeneration pins RecordPoll on its own:
// the round-trip test above follows it with a RecordStatus that overwrites the
// generation and cannot tell a poll that never touched last_poll_at (Register
// just set it). Aging the timestamp first makes the liveness flip observable,
// through the server-computed status so no host/database clock comparison is
// involved.
func TestSDKRepo_RecordPollIsLivenessAndGeneration(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewSDKRepo(conn)

	inst := &model.SDKInstance{ID: "inst-poll", Service: "cartservice", Address: "10.0.0.2:9090", PollIntervalMs: 60000}
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE sdk_instances SET last_poll_at = now() - interval '1 hour' WHERE id = $1`, inst.ID); err != nil {
		t.Fatalf("age last_poll_at: %v", err)
	}
	if got, _ := repo.Get(ctx, inst.ID); got.Status != "dead" {
		t.Fatalf("aged instance status = %q, want dead (premise of the test)", got.Status)
	}

	if err := repo.RecordPoll(ctx, inst.ID, 7); err != nil {
		t.Fatalf("record poll: %v", err)
	}
	got, _ := repo.Get(ctx, inst.ID)
	if got.Status != "alive" {
		t.Errorf("after poll: status = %q, want alive (last_poll_at %v)", got.Status, got.LastPollAt)
	}
	// The poll carries the generation only; etag, lists and observed_at come from the status report.
	if got.ObservedGeneration != 7 || got.ObservedEtag != "" || got.ObservedAt != nil {
		t.Errorf("after poll: %+v, want observed_generation=7 and nothing else observed", got)
	}
}

// TestSDKRepo_ReRegisterResetsObservedState pins "a re-registering instance
// starts from nothing" for every observed column, not just the ones the
// round-trip test happens to leave non-empty, and that List/ListLive scan the
// widened column list.
func TestSDKRepo_ReRegisterResetsObservedState(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewSDKRepo(conn)

	inst := &model.SDKInstance{ID: "inst-rereg", Service: "cartservice", Address: "10.0.0.3:9090",
		Host: "in-process", Capabilities: []string{"inline"}}
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := repo.RecordPoll(ctx, inst.ID, 4); err != nil {
		t.Fatalf("record poll: %v", err)
	}
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{
		AppliedGeneration: 4, Etag: "4-0123456789abcdef",
		Unsupported: []model.UnsupportedRule{{Rule: "mem", Category: "resource"}},
		Errors:      []string{"boom"},
		ReplaySet:   &model.ReplaySetStatus{ID: "sha256:bb", State: "committed"},
	}); err != nil {
		t.Fatalf("record status: %v", err)
	}
	got, _ := repo.Get(ctx, inst.ID)
	if got.ObservedGeneration != 4 || got.ObservedEtag == "" || len(got.ObservedUnsupported) != 1 ||
		len(got.ObservedErrors) != 1 || got.ReplaySetID == "" || got.ReplaySetState != "committed" || got.ObservedAt == nil {
		t.Fatalf("premise: observed state not fully populated: %+v", got)
	}

	inst.Host, inst.Capabilities = "sidecar", []string{"network", "resource"}
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	got, _ = repo.Get(ctx, inst.ID)
	if got.Host != "sidecar" || len(got.Capabilities) != 2 || got.Capabilities[0] != "network" {
		t.Errorf("re-register host/capabilities = %q/%v, want sidecar/[network resource]", got.Host, got.Capabilities)
	}
	if got.ObservedGeneration != 0 || got.ObservedEtag != "" || got.ObservedUnsupported != nil || got.ObservedErrors != nil ||
		got.ReplaySetID != "" || got.ReplaySetState != "" || got.ObservedAt != nil {
		t.Errorf("re-register left observed state behind: %+v", got)
	}

	if all, err := repo.List(ctx); err != nil || len(all) != 1 || all[0].Host != "sidecar" {
		t.Errorf("List = %d err=%v, want the one re-registered instance", len(all), err)
	}
	if live, err := repo.ListLive(ctx, time.Minute); err != nil || len(live) != 1 {
		t.Errorf("ListLive = %d err=%v, want 1", len(live), err)
	}
}

// TestSDKRepo_EmptyListsAreSQLNull pins that an empty list is stored as SQL
// NULL, not the JSON value null: "capabilities IS NULL" is how the schema says
// absent = all, and a registration or status report carrying an empty list
// must land there. The model cannot tell the two apart (both scan to a nil
// slice), so this reads the columns raw.
func TestSDKRepo_EmptyListsAreSQLNull(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewSDKRepo(conn)

	inst := &model.SDKInstance{ID: "inst-null", Service: "cartservice", Address: "10.0.0.4:9090",
		Capabilities: []string{}} // empty, non-nil: still "absent"
	if err := repo.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := repo.RecordStatus(ctx, inst.ID, model.ObservedStatus{
		AppliedGeneration: 1, Unsupported: []model.UnsupportedRule{}, Errors: []string{},
	}); err != nil {
		t.Fatalf("record status: %v", err)
	}

	var capsNull, unsupportedNull, errorsNull bool
	if err := conn.QueryRowContext(ctx, `SELECT capabilities IS NULL, observed_unsupported IS NULL, observed_errors IS NULL
		FROM sdk_instances WHERE id = $1`, inst.ID).Scan(&capsNull, &unsupportedNull, &errorsNull); err != nil {
		t.Fatalf("read raw columns: %v", err)
	}
	if !capsNull || !unsupportedNull || !errorsNull {
		t.Errorf("IS NULL: capabilities=%v observed_unsupported=%v observed_errors=%v, want all true",
			capsNull, unsupportedNull, errorsNull)
	}
}
