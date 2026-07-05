package atrocontrol

import (
	"fmt"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// TestPreload_ChunkingBoundaries pins the §W4/Q4 chunk bounds: 500 entries or
// ~4 MiB serialized, whichever is hit first, with every entry preserved.
func TestPreload_ChunkingBoundaries(t *testing.T) {
	// 501 small entries → 2 chunks (500 + 1) by the entry-count bound.
	entries := make([]atroposdk.CacheBoxWireEntry, 501)
	for i := range entries {
		entries[i] = atroposdk.CacheBoxWireEntry{Key: fmt.Sprintf("k%d", i), StatusCode: 200, Body: []byte("x")}
	}
	chunks := chunkEntries(entries, maxPreloadChunkEntries, maxPreloadChunkBytes)
	if len(chunks) != 2 || len(chunks[0]) != 500 || len(chunks[1]) != 1 {
		t.Fatalf("501 entries → chunks of %v, want [500 1]", chunkSizes(chunks))
	}
	if total := countEntries(chunks); total != 501 {
		t.Fatalf("entries across chunks = %d, want 501", total)
	}

	// Large entries force a byte-bound cut well before 500 entries.
	big := make([]atroposdk.CacheBoxWireEntry, 10)
	for i := range big {
		big[i] = atroposdk.CacheBoxWireEntry{Key: fmt.Sprintf("b%d", i), StatusCode: 200, Body: make([]byte, 1<<20)} // ~1 MiB
	}
	bigChunks := chunkEntries(big, maxPreloadChunkEntries, maxPreloadChunkBytes)
	if len(bigChunks) < 3 {
		t.Fatalf("10×~1.3 MiB entries under a 4 MiB cap → %d chunks, want ≥3", len(bigChunks))
	}
	if total := countEntries(bigChunks); total != 10 {
		t.Fatalf("entries across big chunks = %d, want 10", total)
	}
	for _, c := range bigChunks {
		if len(c) <= 1 {
			continue // a lone oversized entry may exceed the cap
		}
		sz := 0
		for _, e := range c {
			sz += estimateEntrySize(e)
		}
		if sz > maxPreloadChunkBytes {
			t.Fatalf("multi-entry chunk = %d bytes exceeds cap %d", sz, maxPreloadChunkBytes)
		}
	}

	if got := chunkEntries(nil, maxPreloadChunkEntries, maxPreloadChunkBytes); got != nil {
		t.Fatalf("empty entries → %v, want nil chunks", got)
	}
}

func chunkSizes(chunks [][]atroposdk.CacheBoxWireEntry) []int {
	sizes := make([]int, len(chunks))
	for i, c := range chunks {
		sizes[i] = len(c)
	}
	return sizes
}

func countEntries(chunks [][]atroposdk.CacheBoxWireEntry) int {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	return n
}
