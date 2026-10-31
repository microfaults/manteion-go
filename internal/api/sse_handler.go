package api

import (
	"fmt"
	"net/http"
	"time"
)

// handleSSEEvents streams desired-state change nudges to SDK clients via SSE.
// Clients connect with ?service=<name> and receive a rules_changed event, data
// {"generation":G}, on every generation change of that service, until they
// disconnect or the server begins shutting down. A heartbeat comment is sent
// every 30s to prevent proxy timeouts.
//
// @Summary      Rule-change nudges (SSE)
// @Description  Server-sent events for one service. Event type rules_changed, with data
// @Description  {"generation":G}, is emitted on every change of the service's generation:
// @Description  rule CRUD, fault-config fire/cancel/delete/expiry, kill switch, a capability
// @Description  change on re-register, and orchestrator phase transitions (enter, pause,
// @Description  drain, finish). A ": heartbeat" comment follows every 30 s. A nudge only:
// @Description  the long-poll (GET /sdk/rules) is primary; an SDK polls at once on
// @Description  rules_changed. The stream ends when the server shuts down.
// @Tags         sdk
// @Produce      text/event-stream
// @Param        service  query     string  true  "service name"
// @Success      200      {string}  string  "event stream: rules_changed events and heartbeat comments"
// @Failure      400      {object}  api.ErrorResponse  "service query param required"
// @Failure      500      {object}  api.ErrorResponse  "streaming not supported"
// @Router       /sdk/events [get]
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
	shutdown := s.shutdownContext()

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
		case <-shutdown.Done():
			return // the server is shutting down; the SDK reconnects after its backoff
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
