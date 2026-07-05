package cachestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// entryDigest computes the wire spec §W5 per-entry digest:
// SHA-256(key || 0x00 || uint16_be(status) || 0x00 || SHA-256(body)).
// This MUST stay byte-identical to the SDK's cachebox.EntryDigest — the preload
// commit gate (MANT-1) compares manteion's SetChecksum to the SDK's.
func entryDigest(key string, statusCode int, body []byte) [32]byte {
	bodyHash := sha256.Sum256(body)

	var buf bytes.Buffer
	buf.WriteString(key)
	buf.WriteByte(0)
	var statusBytes [2]byte
	binary.BigEndian.PutUint16(statusBytes[:], uint16(statusCode))
	buf.Write(statusBytes[:])
	buf.WriteByte(0)
	buf.Write(bodyHash[:])

	return sha256.Sum256(buf.Bytes())
}

// SetChecksum computes the wire spec §W5 order-independent checksum over a set
// of recorded entries: hex(SHA-256(concat(entry digests sorted lexicographically
// as byte strings))). Sorting the digests (not the entries) makes it
// order-independent, so it matches regardless of chunk/stage ordering.
func SetChecksum(entries []atroposdk.CacheBoxWireEntry) string {
	digests := make([][]byte, len(entries))
	for i := range entries {
		d := entryDigest(entries[i].Key, entries[i].StatusCode, entries[i].Body)
		digests[i] = d[:]
	}
	sort.Slice(digests, func(i, j int) bool {
		return bytes.Compare(digests[i], digests[j]) < 0
	})

	var buf bytes.Buffer
	for _, d := range digests {
		buf.Write(d)
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}
