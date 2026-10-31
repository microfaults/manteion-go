package api

import (
	"context"
	"time"
)

// StartFaultConfigReaper completes long-running faults whose finite duration has
// elapsed. It only flips DB status and bumps the affected services' desired-state
// generations — there is no push to instances. The SDK drops an expired fault
// when the poll's active_faults list no longer includes it (ListActiveForService
// already excludes expired ones), with the SDK watchdog as a backstop.
func (s *Server) StartFaultConfigReaper(ctx context.Context, interval time.Duration) {
	s.logger.Info("starting fault config reaper", "interval", interval)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reapExpiredFaultConfigs(ctx)
			}
		}
	}()
}

func (s *Server) reapExpiredFaultConfigs(ctx context.Context) {
	expired, err := s.faultConfigs.ListExpired(ctx)
	if err != nil {
		s.logger.Error("fault reaper: list expired failed", "error", err)
		return
	}
	seen := make(map[string]struct{})
	var services []string
	for _, f := range expired {
		if err := s.faultConfigs.MarkCompleted(ctx, f.ID); err != nil {
			s.logger.Error("fault reaper: mark completed failed", "id", f.ID, "error", err)
			continue
		}
		if _, ok := seen[f.Service]; !ok {
			seen[f.Service] = struct{}{}
			services = append(services, f.Service)
		}
		s.logger.Info("fault reaper: completed expired fault", "id", f.ID, "service", f.Service)
	}
	// One bump per affected service once the batch is marked completed; the
	// service's polls and SSE subscribers wake and drop the expired faults.
	for _, svc := range services {
		s.bumpGeneration(ctx, svc)
	}
}
