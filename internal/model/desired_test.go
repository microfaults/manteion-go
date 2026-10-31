package model

import "testing"

// The status endpoint accepts exactly the contract's replay-set states. Plan 2's
// phase-entry barrier fails fast on "failed", so it must be accepted as much as
// the other two; anything else, the empty string and other spellings included,
// is refused.
func TestValidReplaySetState(t *testing.T) {
	for state, want := range map[string]bool{
		"fetching":  true,
		"committed": true,
		"failed":    true,
		"":          false,
		"done":      false,
		"pending":   false,
		"Committed": false,
		"FAILED":    false,
		" failed":   false,
	} {
		if got := ValidReplaySetState(state); got != want {
			t.Errorf("ValidReplaySetState(%q) = %v, want %v", state, got, want)
		}
	}
}
