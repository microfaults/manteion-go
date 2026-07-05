package cachestore

import (
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// TestSetChecksum_GoldenFixture pins the wire spec §W5 byte layout against the
// SAME golden value the SDK's cachebox.SetChecksum pins (atropos
// internal/cachebox/checksum_test.go). Byte-parity here is load-bearing: a
// drift means every preload commit mismatches and all isolation phases abort.
func TestSetChecksum_GoldenFixture(t *testing.T) {
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
	}
	const want = "b39b7e27ad99477228741443d70b1a721d946dddaf65e3526d25a2b1c9bbe035"
	if got := SetChecksum(entries); got != want {
		t.Fatalf("SetChecksum golden mismatch: got %s, want %s", got, want)
	}
}

func TestSetChecksum_OrderIndependent(t *testing.T) {
	a := []atroposdk.CacheBoxWireEntry{
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
	}
	b := []atroposdk.CacheBoxWireEntry{
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
	}
	if SetChecksum(a) != SetChecksum(b) {
		t.Fatal("checksum must not depend on entry order")
	}
}

// TestSetChecksum_CrossRepoVector pins the §W5 checksum to a fixed vector.
// atropos-go/internal/cachebox/checksum_test.go pins the SAME vector: the
// two implementations are intentionally duplicated (the SDK's lives in an
// internal/ package this repo cannot import), and the preload commit gate
// 409s every isolation phase if they ever diverge by a byte. If this test
// needs a new expected value, the wire spec changed -- update BOTH repos
// and the spec together.
func TestSetChecksum_CrossRepoVector(t *testing.T) {
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "v2:alpha", StatusCode: 200, Body: []byte("hello world")},
		{Key: "v2:beta", StatusCode: 404, Body: nil},
		{Key: "v2:gamma", StatusCode: 503, Body: []byte{0x00, 0x01, 0xFF}},
	}
	const want = "823fb309f1dc167e10405d0f425b06cc48f0e847431b3a57a6e29a3e788d8032"

	if got := SetChecksum(entries); got != want {
		t.Fatalf("W5 vector drifted:\n got %s\nwant %s", got, want)
	}
	shuffled := []atroposdk.CacheBoxWireEntry{entries[2], entries[0], entries[1]}
	if got := SetChecksum(shuffled); got != want {
		t.Fatalf("W5 checksum is order-dependent: got %s", got)
	}
}
