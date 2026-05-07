// Package cachestore persists baseline cache-box entries as NDJSON files on
// the local filesystem. Layout: {root}/{runID}/{service}.jsonl
// Multiple ingest batches append to the same file; Read streams all lines.
package cachestore

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// Store manages NDJSON cache files rooted at a configurable directory.
//
// Concurrent ingest batches for the same (runID, service) pair must be
// serialized: O_APPEND writes are only atomic up to PIPE_BUF (typically 4 KB)
// on Linux, and JSON-encoded cache entries can exceed that. A per-pair mutex
// guards each file. Different (runID, service) pairs proceed in parallel.
type Store struct {
	root string

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

// New creates a Store backed by the given root directory.
// The directory and any run sub-directories are created lazily on first Write.
func New(root string) *Store {
	return &Store{
		root:  root,
		locks: make(map[string]*sync.Mutex),
	}
}

// lockFor returns a per-pair mutex, creating one on first use. The locks map
// itself is protected by locksMu — held only long enough to look up or
// allocate the per-pair mutex, never across the actual write.
func (s *Store) lockFor(key string) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	l, ok := s.locks[key]
	if !ok {
		l = &sync.Mutex{}
		s.locks[key] = l
	}
	return l
}

// Write appends entries for a (runID, service) pair to the corresponding .jsonl
// file. Safe to call concurrently for different pairs; calls for the same pair
// are serialized internally so JSON lines never interleave.
func (s *Store) Write(runID, service string, entries []atroposdk.CacheBoxWireEntry) error {
	key := runID + "/" + service
	l := s.lockFor(key)
	l.Lock()
	defer l.Unlock()

	dir := filepath.Join(s.root, runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, service+".jsonl")
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

// Read returns all entries stored for a (runID, service) pair.
// Returns nil, nil if no file exists yet.
func (s *Store) Read(runID, service string) ([]atroposdk.CacheBoxWireEntry, error) {
	path := filepath.Join(s.root, runID, service+".jsonl")
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
