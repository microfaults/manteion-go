package desired

import (
	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// FreezeDelayRequest builds the freeze config for one frozen service: the
// delay source its cache-box installs (the poll's freeze_cfg), made of its
// fitted synthetic-delay distribution plus the authoritative CacheBoxContext
// (§W1) scoping the freeze to (experiment_id, phase_id) with the service's key
// strategy. The context is provenance/forward-compat — replay itself is driven
// by the poll-synthesized replay rule (MANT-4).
func FreezeDelayRequest(p *model.ExperimentPhase, fs model.CacheBoxConfig) atroposdk.DelayRequest {
	delay := atroposdk.DelayRequest{}
	if fs.SyntheticDelay != nil {
		if fs.SyntheticDelay.FitMu != nil {
			delay.Mu = *fs.SyntheticDelay.FitMu
		}
		if fs.SyntheticDelay.FitSigma != nil {
			delay.Sigma = *fs.SyntheticDelay.FitSigma
		}
	}
	strat := model.ResolveKeyStrategy(fs.KeyStrategy)
	delay.Context = &atroposdk.CacheBoxContext{
		ExperimentID:    p.ExperimentID,
		PhaseID:         p.ID,
		KeyStrategy:     strat,
		StrategyVersion: model.KeyStrategyVersion(strat),
		KeyHeaders:      fs.KeyHeaders,
	}
	return delay
}
