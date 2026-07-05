package atrocontrol

import (
	"context"
	"fmt"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

const (
	// Chunk sizing (wire spec §W4 / Q4): cap each preload chunk at 500 entries
	// or ~4 MiB serialized, whichever is hit first.
	maxPreloadChunkEntries = 500
	maxPreloadChunkBytes   = 4 << 20
	// DefaultPreloadMaxBytes is the SDK-side staging guard (Q4): 256 MiB.
	DefaultPreloadMaxBytes = 256 << 20
)

// PreloadSpec is the staged-preload request for one frozen service: the entries
// to install plus the (experiment_id, phase_id) scope, key strategy, and the
// manteion-computed §W5 checksum the SDK verifies its staged set against.
type PreloadSpec struct {
	ExperimentID    string
	PhaseID         string
	SourcePhaseID   string
	KeyStrategy     string
	StrategyVersion int
	KeyHeaders      []string
	MaxBytes        int64
	Entries         []atroposdk.CacheBoxWireEntry
	Checksum        string
}

// InstancePreloadResult is one instance's outcome of a staged preload.
type InstancePreloadResult struct {
	InstanceID string
	Address    string
	Committed  bool   // the SDK's commit returned ok (checksum matched, swap happened)
	Loaded     int    // entries the SDK swapped in
	Checksum   string // the SDK's own set checksum (for diagnosing a mismatch)
	Err        error  // transport/protocol failure (nil on a clean commit OR a clean mismatch)
}

// PreloadService installs a service's replay set on every live instance via the
// staged begin→chunk→commit protocol (wire spec §W4) and returns the
// per-instance outcome. The caller (MANT-1) may freeze the phase only when
// EVERY instance reports Committed with Loaded == len(entries) and a matching
// checksum. Runs on the dedicated preload transport, never the 2s fanout.
func (c *Controller) PreloadService(ctx context.Context, service string, spec PreloadSpec) ([]InstancePreloadResult, error) {
	targets, err := c.resolveTargets(ctx, service, c.defaults.filter)
	if err != nil {
		return nil, err
	}
	chunks := chunkEntries(spec.Entries, maxPreloadChunkEntries, maxPreloadChunkBytes)
	results := make([]InstancePreloadResult, len(targets))
	for i, t := range targets {
		results[i] = c.preloadInstance(ctx, t, spec, chunks)
	}
	c.logger.Info("atrocontrol.preload",
		"service", service, "instances", len(targets),
		"entries", len(spec.Entries), "chunks", len(chunks))
	return results, nil
}

func (c *Controller) preloadInstance(ctx context.Context, t target, spec PreloadSpec, chunks [][]atroposdk.CacheBoxWireEntry) InstancePreloadResult {
	res := InstancePreloadResult{InstanceID: t.instanceID, Address: t.address}
	maxBytes := spec.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultPreloadMaxBytes
	}

	if err := c.preloadTx.PreloadBegin(ctx, t.address, atroposdk.PreloadBeginRequest{
		ExperimentID:    spec.ExperimentID,
		PhaseID:         spec.PhaseID,
		SourcePhaseID:   spec.SourcePhaseID,
		TotalEntries:    len(spec.Entries),
		TotalChunks:     len(chunks),
		KeyStrategy:     spec.KeyStrategy,
		StrategyVersion: spec.StrategyVersion,
		KeyHeaders:      spec.KeyHeaders,
		MaxBytes:        maxBytes,
	}); err != nil {
		res.Err = fmt.Errorf("begin: %w", err)
		return res
	}

	for i, chunk := range chunks {
		if _, err := c.preloadTx.PreloadChunk(ctx, t.address, atroposdk.PreloadChunkRequest{
			ExperimentID: spec.ExperimentID,
			PhaseID:      spec.PhaseID,
			ChunkSeq:     i + 1,
			Entries:      chunk,
		}); err != nil {
			res.Err = fmt.Errorf("chunk %d/%d: %w", i+1, len(chunks), err)
			c.abortPreload(ctx, t.address, spec)
			return res
		}
	}

	commit, err := c.preloadTx.PreloadCommit(ctx, t.address, atroposdk.PreloadCommitRequest{
		ExperimentID: spec.ExperimentID,
		PhaseID:      spec.PhaseID,
		TotalEntries: len(spec.Entries),
		Checksum:     spec.Checksum,
	})
	if err != nil {
		res.Err = fmt.Errorf("commit: %w", err)
		return res
	}
	res.Committed = commit.OK
	res.Loaded = commit.Loaded
	res.Checksum = commit.Checksum
	return res
}

// abortPreload best-effort drops staging on an instance after a mid-stream
// failure so a retry's begin starts clean (begin also clears — belt and braces).
func (c *Controller) abortPreload(ctx context.Context, addr string, spec PreloadSpec) {
	if err := c.preloadTx.PreloadAbort(ctx, addr, atroposdk.PreloadAbortRequest{
		ExperimentID: spec.ExperimentID, PhaseID: spec.PhaseID,
	}); err != nil {
		c.logger.Warn("atrocontrol: preload abort failed", "address", addr, "error", err)
	}
}

// chunkEntries splits entries into chunks of at most maxEntries or ~maxBytes
// serialized, whichever bound is hit first. maxBytes is enforced against a
// per-entry size estimate, so a chunk overshoots it by at most a single entry;
// a lone oversized entry still ships in its own chunk.
func chunkEntries(entries []atroposdk.CacheBoxWireEntry, maxEntries, maxBytes int) [][]atroposdk.CacheBoxWireEntry {
	if len(entries) == 0 {
		return nil
	}
	var chunks [][]atroposdk.CacheBoxWireEntry
	start, size := 0, 0
	for i := range entries {
		est := estimateEntrySize(entries[i])
		if i > start && (i-start >= maxEntries || size+est > maxBytes) {
			chunks = append(chunks, entries[start:i])
			start, size = i, 0
		}
		size += est
	}
	return append(chunks, entries[start:])
}

// estimateEntrySize approximates an entry's serialized JSON size: base64-inflated
// body (~4/3) plus key, headers, and fixed structural overhead.
func estimateEntrySize(e atroposdk.CacheBoxWireEntry) int {
	size := len(e.Body)*4/3 + len(e.Key) + 256
	for k, vs := range e.Header {
		size += len(k)
		for _, v := range vs {
			size += len(v) + 8
		}
	}
	return size
}
