package model

import "time"

// ReplayArtifact is one immutable replay-set artifact assigned to a frozen
// (phase, service): ID is "sha256:<hex>" of the file bytes (content-addressed:
// two phases replaying the same recording share the id and the file), the
// pair is the isolation phase the set is installed under.
type ReplayArtifact struct {
	ID           string    `json:"id"`
	ExperimentID string    `json:"experiment_id"`
	PhaseID      string    `json:"phase_id"`
	Service      string    `json:"service"`
	EntryCount   int64     `json:"count"`
	W5SHA256     string    `json:"sha256"`
	SizeBytes    int64     `json:"size_bytes"`
	Path         string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}
