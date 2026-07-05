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
