package desired

import (
	"context"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// ArtifactURLPrefix is where manteion serves replay-set artifacts
// (GET /api/v1/cache/artifacts/{id}); a replay set's url is this prefix and
// the artifact id. It stays relative: the SDK resolves it against the manteion
// it polls and authenticates it there.
const ArtifactURLPrefix = "/api/v1/cache/artifacts/"

// FrozenPhaseSource finds the running phase that freezes a service.
type FrozenPhaseSource interface {
	// FrozenServiceForService returns that phase and its cache-box config for
	// the service, or nil, nil when the service is not frozen right now.
	FrozenServiceForService(ctx context.Context, service string) (*model.ExperimentPhase, *model.CacheBoxConfig, error)
}

// ArtifactSource finds the replay-set artifact assigned to a frozen
// (phase, service).
type ArtifactSource interface {
	// ForPhaseService returns nil, nil when none is assigned.
	ForPhaseService(ctx context.Context, phaseID, service string) (*model.ReplayArtifact, error)
}

// PhaseSources derives the frozen-phase inputs of the desired-state document
// from the database, with no state of its own: the freeze config from the
// running phase that freezes a service, the replay set from the artifact
// assigned to that phase and service. One value fills both of the
// Materializer's seams (Freeze and ReplaySets), so the two always name the same
// phase. Phases is required; Artifacts is required for the replay set.
type PhaseSources struct {
	Phases    FrozenPhaseSource
	Artifacts ArtifactSource
}

var (
	_ FreezeSource    = (*PhaseSources)(nil)
	_ ReplaySetSource = (*PhaseSources)(nil)
)

// FreezeConfigForService is the delay source the service's cache-box installs
// while a running phase freezes it (FreezeDelayRequest of that phase's config),
// nil when it is not frozen. It does not wait for the replay set: the freeze
// goes out with the phase, the artifact is assigned during phase entry.
func (s *PhaseSources) FreezeConfigForService(ctx context.Context, service string) (*atroposdk.DelayRequest, error) {
	p, fs, err := s.Phases.FrozenServiceForService(ctx, service)
	if err != nil || p == nil || fs == nil {
		return nil, err
	}
	req := FreezeDelayRequest(p, *fs)
	return &req, nil
}

// ReplaySetForService is the replay set the service's instances must hold: the
// artifact assigned to the frozen (isolation) phase and service, installed under
// that phase's (experiment, phase) pair, the one the replay rule's context
// carries, not the baseline's it was recorded in. It is nil when the service is
// not frozen, and while no artifact is assigned yet (phase entry builds it after
// the phase starts running): nothing is desired until the set exists, and load
// has not started.
func (s *PhaseSources) ReplaySetForService(ctx context.Context, service string) (*model.ReplaySetRef, error) {
	p, fs, err := s.Phases.FrozenServiceForService(ctx, service)
	if err != nil || p == nil || fs == nil {
		return nil, err
	}
	a, err := s.Artifacts.ForPhaseService(ctx, p.ID, service)
	if err != nil || a == nil {
		return nil, err
	}
	return &model.ReplaySetRef{
		ID:           a.ID,
		ExperimentID: p.ExperimentID,
		PhaseID:      p.ID,
		KeyStrategy:  model.ResolveKeyStrategy(fs.KeyStrategy),
		Count:        a.EntryCount,
		SHA256:       a.W5SHA256,
		SizeBytes:    a.SizeBytes,
		URL:          ArtifactURLPrefix + a.ID,
	}, nil
}
