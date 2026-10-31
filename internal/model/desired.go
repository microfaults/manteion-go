package model

// Desired-state protocol v1 (docs/plans/2026-10-30-desired-state-protocol.md).
// TODO(desired-state): swap ReplaySetRef and the status shapes for the
// atropos-go wire aliases once the sibling SDK plan lands them.

// UnsupportedRule is one rule an instance skipped because its host cannot
// execute the fault category.
type UnsupportedRule struct {
	Rule     string `json:"rule"`
	Category string `json:"category"`
}

// ReplaySetStatus is the instance's replay-set progress for the artifact it
// was told to hold.
type ReplaySetStatus struct {
	ID    string `json:"id"`
	State string `json:"state"` // fetching | committed | failed
}

// ValidReplaySetState reports whether s is a state the status endpoint accepts.
func ValidReplaySetState(s string) bool {
	switch s {
	case "fetching", "committed", "failed":
		return true
	}
	return false
}

// ObservedStatus is what POST /api/v1/sdk/status persists. The report is a full
// snapshot of what the instance holds, so each field but AppliedGeneration
// replaces the stored one: empty Unsupported and Errors clear, and an absent
// (nil) ReplaySet means the instance holds none and clears the stored id/state.
// AppliedGeneration only raises the stored observed generation, whose
// authoritative writer is the poll.
type ObservedStatus struct {
	AppliedGeneration uint64
	Etag              string
	Unsupported       []UnsupportedRule
	Errors            []string
	ReplaySet         *ReplaySetStatus
}

// ReplaySetRef is the replay-set artifact a frozen service's instances must
// hold (RuleSync.replay_set). ID is "sha256:<hex>" of the artifact file bytes,
// ExperimentID/PhaseID the pair the set is installed under (the cache-box
// serves it only to the rule whose context matches), SHA256 the W5 digest
// over its entries, URL the artifact endpoint path.
type ReplaySetRef struct {
	ID           string `json:"id"`
	ExperimentID string `json:"experiment_id"`
	PhaseID      string `json:"phase_id"`
	KeyStrategy  string `json:"key_strategy"`
	Count        int64  `json:"count"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
	URL          string `json:"url"`
}
