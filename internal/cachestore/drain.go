package cachestore

import (
	"strings"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// RecordDrainReport stores an SDK's W3 drain report for the (experiment_id,
// phase_id, instance_id) it names, overwriting any prior report for that
// instance. Idempotent (last-wins), so an SDK retrying its drain report is
// safe.
//
// SINGLE-REPLICA: drain reports live in process memory; the drain gate that
// reads them (MANT-2) is correct only under one manteion replica.
func (s *Store) RecordDrainReport(r atroposdk.DrainReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pair := pairKey(r.ExperimentID, r.PhaseID)
	if s.drainReports[pair] == nil {
		s.drainReports[pair] = map[string]atroposdk.DrainReport{}
	}
	s.drainReports[pair][r.InstanceID] = r
}

// DrainReports returns a copy of every drain report received for (exp, phase),
// keyed by instance_id.
func (s *Store) DrainReports(experimentID, phaseID string) map[string]atroposdk.DrainReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.drainReports[pairKey(experimentID, phaseID)]
	out := make(map[string]atroposdk.DrainReport, len(src))
	for id, r := range src {
		out[id] = r
	}
	return out
}

// PushedInstances returns the (instance_id → service) of every instance that
// pushed at least one recorded entry for (exp, phase) — part of the drain gate's
// expected set (an instance that appeared mid-phase and pushed but is no longer
// registered still owes a drain report).
func (s *Store) PushedInstances(experimentID, phaseID string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for key, count := range s.received[pairKey(experimentID, phaseID)] {
		if count <= 0 {
			continue
		}
		service, instance, ok := strings.Cut(key, "\x00")
		if ok {
			out[instance] = service
		}
	}
	return out
}
