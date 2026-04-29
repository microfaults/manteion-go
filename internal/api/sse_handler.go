package api

import (
	"fmt"
	"net/http"
	"time"
)

// handleSSEEvents streams rule-change notifications to SDK clients via SSE.
// Clients connect with ?service=<name> and receive events until disconnected.
// A heartbeat comment is sent every 30s to prevent proxy timeouts.
func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query param required")
		return
	}

	rc := http.NewResponseController(w)

	// Clear the server-level write deadline — SSE connections are indefinitely
	// long-lived and the global WriteTimeout would kill them. We push the
	// deadline forward after each successful write instead.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	ch := s.broker.Subscribe(service)
	defer s.broker.Unsubscribe(service, ch)

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	flush := func() bool {
		if err := rc.Flush(); err != nil {
			return false
		}
		// Extend write deadline 90s past each successful flush so a slow
		// client that misses one heartbeat doesn't get killed immediately.
		// If the deadline-extension fails (rare; transport-specific), log so
		// the connection's premature timeout is diagnosable rather than silent.
		if err := rc.SetWriteDeadline(time.Now().Add(90 * time.Second)); err != nil {
			s.logger.Debug("sse: extend write deadline failed", "error", err, "service", service)
		}
		return true
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, event.Data)
			if !flush() {
				return
			}
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			if !flush() {
				return
			}
		}
	}
}
