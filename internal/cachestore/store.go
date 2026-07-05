// Package cachestore persists recorded cache-box entries as NDJSON files on
// the local filesystem and tracks the ingest accounting manteion needs to
// verify a recording is complete and non-degraded.
//
// Layout: {root}/experiments/{experiment_id}/phases/{phase_id}/{service}.ndjson
// Every recorded entry, count, and collision stat is scoped to the
// (experiment_id, phase_id) pair (INV-5) — there is no ambient/global state.
package cachestore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// Store manages recorded NDJSON cache files plus the in-memory ingest
// accounting (dedupe set, per-instance received counts, per-phase collision
// stats) rooted at a configurable directory.
//
// A single mutex serializes every file append together with the accounting
// updates so a push batch is deduped, persisted, counted, and collision-scanned
// atomically — a single-writer discipline. Correctness is chosen over append
// parallelism here (the prime directive: a verifiable fidelity guarantee beats
// throughput).
//
// SINGLE-REPLICA: the dedupe set, received counts, and collision stats live in
// process memory and are correct only under one manteion replica. A second
// replica would not observe the first's dedupe/accounting state; recorded
// coverage would double-count and drain accounting would diverge. A future HA
// effort must externalize this state.
type Store struct {
	root string

	mu         sync.Mutex
	dedup      map[string]map[string]struct{} // pair -> "instance\x00batch_seq" -> seen
	received   map[string]map[string]int64    // pair -> "service\x00instance"   -> entries persisted
	seenKeys   map[string]map[string]entrySig // pair -> cache key -> last-seen signature (collision detection)
	collisions map[string]*collisionCounts    // pair -> divergent/identical tallies
}

// entrySig is the (status, body digest) fingerprint a recorded entry must keep
// stable across sightings of the same key; a change is a divergent collision.
type entrySig struct {
	status  int
	bodySHA string
}

type collisionCounts struct {
	divergent int64
	identical int64
}

// IngestResult reports the outcome of an Ingest call.
type IngestResult struct {
	Accepted  int  // entries appended (0 on a duplicate)
	Duplicate bool // true when (instance, batch_seq) was already ingested
}

// New creates a Store backed by the given root directory. The directory and
// any experiment/phase sub-directories are created lazily on first write.
func New(root string) *Store {
	return &Store{
		root:       root,
		dedup:      map[string]map[string]struct{}{},
		received:   map[string]map[string]int64{},
		seenKeys:   map[string]map[string]entrySig{},
		collisions: map[string]*collisionCounts{},
	}
}

func pairKey(experimentID, phaseID string) string {
	return experimentID + "\x00" + phaseID
}

func (s *Store) phaseDir(experimentID, phaseID string) string {
	return filepath.Join(s.root, "experiments", experimentID, "phases", phaseID)
}

// Append writes entries directly to the (exp, phase, service) NDJSON file,
// bypassing dedupe and ingest accounting. It exists to seed recordings in
// tests and internal flows; the SDK push path uses Ingest.
func (s *Store) Append(experimentID, phaseID, service string, entries []atroposdk.CacheBoxWireEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(experimentID, phaseID, service, entries)
}

// appendLocked performs the serialized NDJSON append. Caller holds s.mu, so no
// two appends ever interleave and O_APPEND atomicity limits are irrelevant.
func (s *Store) appendLocked(experimentID, phaseID, service string, entries []atroposdk.CacheBoxWireEntry) error {
	dir := s.phaseDir(experimentID, phaseID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, service+".ndjson")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

// Ingest atomically dedupes, persists, counts, and collision-scans one push
// batch for (experiment_id, phase_id, service, instance) at batch_seq. A
// batch_seq already seen for the (exp, phase, instance) is a no-op reported as
// a duplicate: SDKs retry batches, and this keeps received counts exact and the
// NDJSON file free of duplicate lines (INV-7).
func (s *Store) Ingest(experimentID, phaseID, service, instance string, batchSeq int, entries []atroposdk.CacheBoxWireEntry) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pair := pairKey(experimentID, phaseID)
	dk := instance + "\x00" + strconv.Itoa(batchSeq)
	if seen := s.dedup[pair]; seen != nil {
		if _, ok := seen[dk]; ok {
			return IngestResult{Accepted: 0, Duplicate: true}, nil
		}
	}

	// Persist before recording dedupe/accounting: a failed append leaves the
	// batch_seq unseen so the SDK's retry is accepted rather than swallowed.
	if err := s.appendLocked(experimentID, phaseID, service, entries); err != nil {
		return IngestResult{}, err
	}

	if s.dedup[pair] == nil {
		s.dedup[pair] = map[string]struct{}{}
	}
	s.dedup[pair][dk] = struct{}{}

	if s.received[pair] == nil {
		s.received[pair] = map[string]int64{}
	}
	s.received[pair][service+"\x00"+instance] += int64(len(entries))

	s.scanCollisionsLocked(pair, entries)

	return IngestResult{Accepted: len(entries), Duplicate: false}, nil
}

// scanCollisionsLocked tallies per-phase key collisions (Q3): a repeated key
// whose (status, body sha) differs from the last sighting is divergent, one
// that matches is identical. Latest-wins — the signature is always updated.
// Caller holds s.mu.
func (s *Store) scanCollisionsLocked(pair string, entries []atroposdk.CacheBoxWireEntry) {
	keys := s.seenKeys[pair]
	if keys == nil {
		keys = map[string]entrySig{}
		s.seenKeys[pair] = keys
	}
	cc := s.collisions[pair]
	if cc == nil {
		cc = &collisionCounts{}
		s.collisions[pair] = cc
	}
	for _, e := range entries {
		sig := entrySig{status: e.StatusCode, bodySHA: entryBodySHA(e)}
		if prev, ok := keys[e.Key]; ok {
			if prev == sig {
				cc.identical++
			} else {
				cc.divergent++
			}
		}
		keys[e.Key] = sig
	}
}

// entryBodySHA returns the entry's recorded body digest, computing it from the
// body when the SDK left the field empty (robustness; matched-version SDKs set it).
func entryBodySHA(e atroposdk.CacheBoxWireEntry) string {
	if e.ResponseBodySHA256 != "" {
		return e.ResponseBodySHA256
	}
	sum := sha256.Sum256(e.Body)
	return hex.EncodeToString(sum[:])
}

// ReceivedCount returns the number of entries manteion has persisted for
// (exp, phase, service, instance) — the drain gate's per-instance input (MANT-2).
func (s *Store) ReceivedCount(experimentID, phaseID, service, instance string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received[pairKey(experimentID, phaseID)][service+"\x00"+instance]
}

// CollisionStats returns the per-phase (divergent, identical) key-collision
// tallies observed at ingest — the collision-rate input to the phase verdict (MANT-6).
func (s *Store) CollisionStats(experimentID, phaseID string) (divergent, identical int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cc := s.collisions[pairKey(experimentID, phaseID)]; cc != nil {
		return cc.divergent, cc.identical
	}
	return 0, 0
}

// Services lists the services that have recorded cache entries for (exp, phase)
// (one {service}.ndjson file each). Returns nil, nil when the phase dir is
// absent (nothing recorded yet).
func (s *Store) Services(experimentID, phaseID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.phaseDir(experimentID, phaseID)
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".ndjson") {
			continue
		}
		out = append(out, strings.TrimSuffix(name, ".ndjson"))
	}
	return out, nil
}

// Read returns all entries stored for (exp, phase, service). Returns nil, nil
// when no file exists yet. Serialized against appends so a concurrent recording
// write is never observed torn.
func (s *Store) Read(experimentID, phaseID, service string) ([]atroposdk.CacheBoxWireEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.phaseDir(experimentID, phaseID), service+".ndjson")
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []atroposdk.CacheBoxWireEntry
	dec := json.NewDecoder(f)
	for dec.More() {
		var e atroposdk.CacheBoxWireEntry
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}
