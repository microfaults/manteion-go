package cachestore

import (
	"os"
	"path/filepath"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// TestCacheStore_PathLayout pins the (exp, phase) NDJSON layout introduced by
// MANT-3: recorded entries land at
// experiments/{experiment_id}/phases/{phase_id}/{service}.ndjson, and Read /
// Services round-trip against that layout.
func TestCacheStore_PathLayout(t *testing.T) {
	root := t.TempDir()
	s := New(root)

	entries := []atroposdk.CacheBoxWireEntry{{Key: "k1"}, {Key: "k2"}}
	if err := s.Append("exp-1", "phase-1", "frontend", entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := filepath.Join(root, "experiments", "exp-1", "phases", "phase-1", "frontend.ndjson")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected file at %s: %v", want, err)
	}

	got, err := s.Read("exp-1", "phase-1", "frontend")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 || got[0].Key != "k1" || got[1].Key != "k2" {
		t.Fatalf("Read = %+v, want k1,k2", got)
	}

	svcs, err := s.Services("exp-1", "phase-1")
	if err != nil {
		t.Fatalf("Services: %v", err)
	}
	if len(svcs) != 1 || svcs[0] != "frontend" {
		t.Fatalf("Services = %v, want [frontend]", svcs)
	}

	// A different (exp, phase) pair is isolated on disk.
	if svcs, _ := s.Services("exp-1", "phase-2"); len(svcs) != 0 {
		t.Fatalf("Services(phase-2) = %v, want empty", svcs)
	}
}

// TestIngest_DuplicateBatchSeqNotDoubleCounted pins MANT-3 dedupe: a retried
// batch (same exp, phase, instance, batch_seq) is reported duplicate, is NOT
// re-appended to the NDJSON file, and does NOT double the per-instance
// received count.
func TestIngest_DuplicateBatchSeqNotDoubleCounted(t *testing.T) {
	s := New(t.TempDir())

	entries := []atroposdk.CacheBoxWireEntry{{Key: "k1"}, {Key: "k2"}, {Key: "k3"}}

	first, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 1, entries)
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	if first.Duplicate || first.Accepted != 3 {
		t.Fatalf("first Ingest = %+v, want {Accepted:3, Duplicate:false}", first)
	}

	// Same batch_seq redelivered (SDK retry): duplicate, nothing appended.
	dup, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 1, entries)
	if err != nil {
		t.Fatalf("dup Ingest: %v", err)
	}
	if !dup.Duplicate || dup.Accepted != 0 {
		t.Fatalf("dup Ingest = %+v, want {Accepted:0, Duplicate:true}", dup)
	}

	if got := s.ReceivedCount("exp-1", "phase-1", "frontend", "inst-a"); got != 3 {
		t.Fatalf("ReceivedCount = %d, want 3 (retry must not double-count)", got)
	}

	stored, err := s.Read("exp-1", "phase-1", "frontend")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("stored entries = %d, want 3 (one copy, not doubled)", len(stored))
	}

	// A new batch_seq from the same instance accumulates.
	next, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 2, []atroposdk.CacheBoxWireEntry{{Key: "k4"}})
	if err != nil {
		t.Fatalf("next Ingest: %v", err)
	}
	if next.Duplicate || next.Accepted != 1 {
		t.Fatalf("next Ingest = %+v, want {Accepted:1, Duplicate:false}", next)
	}
	if got := s.ReceivedCount("exp-1", "phase-1", "frontend", "inst-a"); got != 4 {
		t.Fatalf("ReceivedCount after seq 2 = %d, want 4", got)
	}
}

// TestIngest_CollisionStats pins MANT-3 ingest-side collision accounting (Q3):
// a repeated key with a differing (status, response_body_sha256) is a
// divergent collision; a repeated key matching the last-seen signature is an
// identical collision. Cross-instance divergence is caught the same way.
func TestIngest_CollisionStats(t *testing.T) {
	s := New(t.TempDir())

	// First sighting of key K — no collision.
	if _, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 1,
		[]atroposdk.CacheBoxWireEntry{{Key: "K", StatusCode: 200, ResponseBodySHA256: "sha-A"}}); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 0 || i != 0 {
		t.Fatalf("after first sighting: divergent=%d identical=%d, want 0,0", d, i)
	}

	// Same key, different body sha — divergent collision.
	if _, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 2,
		[]atroposdk.CacheBoxWireEntry{{Key: "K", StatusCode: 200, ResponseBodySHA256: "sha-B"}}); err != nil {
		t.Fatalf("ingest 2: %v", err)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 1 || i != 0 {
		t.Fatalf("after divergent: divergent=%d identical=%d, want 1,0", d, i)
	}

	// Same key from a different instance, matching the latest signature —
	// identical collision (cross-instance, still counted).
	if _, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-b", 1,
		[]atroposdk.CacheBoxWireEntry{{Key: "K", StatusCode: 200, ResponseBodySHA256: "sha-B"}}); err != nil {
		t.Fatalf("ingest 3: %v", err)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 1 || i != 1 {
		t.Fatalf("after identical: divergent=%d identical=%d, want 1,1", d, i)
	}

	// A different status on the same key is also divergent.
	if _, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-b", 2,
		[]atroposdk.CacheBoxWireEntry{{Key: "K", StatusCode: 500, ResponseBodySHA256: "sha-B"}}); err != nil {
		t.Fatalf("ingest 4: %v", err)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 2 || i != 1 {
		t.Fatalf("after status divergence: divergent=%d identical=%d, want 2,1", d, i)
	}

	// A brand-new key is never a collision.
	if _, err := s.Ingest("exp-1", "phase-1", "frontend", "inst-a", 3,
		[]atroposdk.CacheBoxWireEntry{{Key: "OTHER", StatusCode: 200, ResponseBodySHA256: "sha-C"}}); err != nil {
		t.Fatalf("ingest 5: %v", err)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 2 || i != 1 {
		t.Fatalf("after new key: divergent=%d identical=%d, want 2,1", d, i)
	}

	// Per-service isolation: the same key colliding under ANOTHER service
	// tallies against that service only (exp6 finding 7 — a phase-wide tally
	// let one service's divergence contaminate another's verdict).
	for seq := 1; seq <= 2; seq++ {
		sha := "sha-X"
		if seq == 2 {
			sha = "sha-Y"
		}
		if _, err := s.Ingest("exp-1", "phase-1", "checkoutservice", "inst-c", seq,
			[]atroposdk.CacheBoxWireEntry{{Key: "K", StatusCode: 200, ResponseBodySHA256: sha}}); err != nil {
			t.Fatalf("ingest checkout %d: %v", seq, err)
		}
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "checkoutservice"); d != 1 || i != 0 {
		t.Fatalf("checkoutservice tally: divergent=%d identical=%d, want 1,0", d, i)
	}
	if d, i := s.CollisionStats("exp-1", "phase-1", "frontend"); d != 2 || i != 1 {
		t.Fatalf("frontend tally polluted by checkoutservice: divergent=%d identical=%d, want 2,1", d, i)
	}
}

// TestEnsureRootCreatesNestedDir: EnsureRoot must create the (possibly
// nested) store root and prove it writable — the boot-time guard that turns
// an unwritable cache dir into an immediate exit instead of per-batch
// ingest 500s minutes into a recording phase.
func TestEnsureRootCreatesNestedDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "cache")
	if err := New(root).EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot on creatable root: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		t.Fatalf("root not created: info=%v err=%v", info, err)
	}
}

func TestEnsureRootFailsOnUnwritableParent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	if err := New(filepath.Join(parent, "cache")).EnsureRoot(); err == nil {
		t.Fatal("EnsureRoot on unwritable parent: want error, got nil")
	}
}
