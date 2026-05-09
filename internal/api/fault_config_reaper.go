package api

import (
	"context"
	"time"
)

// StartFaultConfigReaper starts a background goroutine that polls the database
// for expired fault configs (where duration > 0 and fired_at + duration <= now),
// marks them as completed, and fans out a clear request to all SDK instances.
func (s *Server) StartFaultConfigReaper(ctx context.Context, pollInterval time.Duration) {
	s.logger.Info("starting fault config reaper", "interval", pollInterval)

	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("fault config reaper stopped")
				return
			case <-ticker.C:
				s.reapExpiredFaultConfigs(ctx)
			}
		}
	}()
}

func (s *Server) reapExpiredFaultConfigs(ctx context.Context) {
	// 1. Fetch expired faults
	expired, err := s.faultConfigs.ListExpired(ctx)
	if err != nil {
		s.logger.Error("reaper: list expired failed", "error", err)
		return
	}

	for _, f := range expired {
		s.logger.Info("reaper: reaping expired fault config",
			"id", f.ID, "service", f.Service, "category", f.Category)

		// 2. Clear fault on target instances (best-effort fanout)
		// We use a short timeout context to prevent blocking the reaper loop.
		fanoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		res, err := s.controller.ClearFault(fanoutCtx, f.Service, f.Category)
		cancel()

		if err != nil {
			s.logger.Warn("reaper: clear fault fanout failed", "id", f.ID, "error", err)
		} else if len(res.Failed) > 0 {
			s.logger.Warn("reaper: clear fault fanout had failures", "id", f.ID, "failures", len(res.Failed))
		}

		// 3. Mark completed in DB
		if err := s.faultConfigs.MarkCompleted(ctx, f.ID); err != nil {
			s.logger.Error("reaper: mark completed failed", "id", f.ID, "error", err)
		} else {
			s.logger.Info("reaper: successfully reaped fault config", "id", f.ID)
		}
	}
}
