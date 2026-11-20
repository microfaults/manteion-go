package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// routeEvents is the sixth public route: one run's event stream.
const routeEvents = "GET /api/v1/demo/runs/{id}/events"

// heartbeatFrame is the comment a stream sends every Config.Heartbeat so that
// proxies keep the connection. An EventSource ignores comments: it is never an
// event.
var heartbeatFrame = []byte(": hb\n\n")

// errNotContract: an event that is not one of the contract's, or whose payload
// is not that event's contract type, or whose JSON would break the frame.
var errNotContract = errors.New("demo: not a contract event")

// handleEvents serves GET /runs/{id}/events, a text/event-stream of the run's
// events (see the contract): state first, then queue, phase and sample events
// as they happen, end last. Heartbeat comments keep the connection open.
//
// In order: a malformed run id is 404 not_found; a stream slot is taken (429
// too_many_streams with Retry-After if the client holds 3 or 50 are open); the
// hub is subscribed to; only then is the run's view read (404 not_found, 503
// unavailable). Subscribing first means an end published while the view is
// read is either in the view or in the channel.
//
// A view that has already ended is sent as state, then end, and the stream
// closes at once, without waiting on the channel: the hub replays an end only
// while it remembers the run (the last hubSealed ended runs of this process),
// and a client reconnecting to an older run, or after a restart, must not wait
// for the maximum age.
//
// The stream ends on the end event (sent, then nothing more), the client going
// away, the hub closing the channel (the demo shutting down), the maximum age
// (Config.StreamMaxAge), or a write that fails or does not complete within
// Config.StreamWriteTimeout. Every way out, a panic included, unsubscribes and
// releases the slot once.
//
// The server's own WriteTimeout does not apply: the stream clears the write
// deadline, and sets one around each frame instead.
func (d *Demo) handleEvents(w http.ResponseWriter, r *http.Request, client string) {
	runID := r.PathValue("id")
	if !validRunID(runID) {
		d.writeError(w, CodeNotFound, 0)
		return
	}
	release, ok, wait := d.lim.takeStream(client)
	if !ok {
		d.writeError(w, CodeTooManyStreams, wait)
		return
	}
	defer release()
	events, unsubscribe := d.hub.Subscribe(runID)
	defer unsubscribe()

	ctx, cancel := context.WithTimeout(r.Context(), d.cfg.routeTimeout())
	run, err := d.view(ctx, runID)
	cancel()
	if err != nil {
		d.fail(w, r, err, 0)
		return
	}
	state, err := encodeFrame(Event{Type: EventState, Data: run})
	if err != nil {
		d.log.Error("demo: a run's state frame did not encode", "run_id", runID, "error", err)
		d.writeError(w, CodeInternal, 0)
		return
	}
	s, err := d.startStream(w)
	if err != nil {
		d.log.Error("demo: the event stream cannot control its connection; refusing it", "run_id", runID, "error", err)
		d.writeError(w, CodeInternal, 0)
		return
	}
	defer s.close()

	if s.send(state) != nil {
		return
	}
	if runEnded(run.Status) {
		end, err := encodeFrame(Event{Type: EventEnd, Data: run})
		if err == nil {
			_ = s.send(end)
		}
		return
	}
	d.forward(r.Context(), s, runID, events)
}

// forward sends the run's events as they come, and heartbeats between them,
// until one of the stream's endings (see handleEvents).
func (d *Demo) forward(ctx context.Context, s *eventStream, runID string, events <-chan Event) {
	heartbeat := time.NewTicker(d.cfg.Heartbeat)
	defer heartbeat.Stop()
	maxAge := time.NewTimer(d.cfg.StreamMaxAge)
	defer maxAge.Stop()
	for {
		select {
		case <-ctx.Done():
			return // the client went away
		case <-maxAge.C:
			return
		case <-heartbeat.C:
			if s.send(heartbeatFrame) != nil {
				return
			}
		case ev, ok := <-events:
			if !ok {
				return // the hub closed: the demo is shutting down
			}
			frame, err := encodeFrame(ev)
			switch {
			case err != nil:
				d.log.Error("demo: an event did not encode; not sent", "run_id", runID, "event", ev.Type, "error", err)
			case frame != nil:
				if s.send(frame) != nil {
					return
				}
			}
			if ev.Type == EventEnd {
				return // nothing follows the end, not even when it did not encode
			}
		}
	}
}

// runEnded reports whether a run with status s has ended.
func runEnded(s RunStatus) bool {
	return s == RunCompleted || s == RunFailed || s == RunCancelled
}

// encodeFrame returns ev as one frame, "event: <type>\ndata: <json>\n\n". Only
// the contract's events encode, each with its own payload type (Run for state
// and end, QueueEvent, PhaseEvent, Sample), so nothing else can reach a client.
// JSON never holds a raw line break; a payload that did would be refused, not
// sent as two lines. A sample whose values are all null encodes to nil: it is
// not sent.
func encodeFrame(ev Event) ([]byte, error) {
	switch ev.Type {
	case EventState, EventEnd:
		_, ok := ev.Data.(Run)
		if !ok {
			return nil, errNotContract
		}
	case EventQueue:
		if _, ok := ev.Data.(QueueEvent); !ok {
			return nil, errNotContract
		}
	case EventPhase:
		if _, ok := ev.Data.(PhaseEvent); !ok {
			return nil, errNotContract
		}
	case EventSample:
		s, ok := ev.Data.(Sample)
		if !ok {
			return nil, errNotContract
		}
		if s.Empty() {
			return nil, nil
		}
	default:
		return nil, errNotContract
	}
	data, err := json.Marshal(ev.Data)
	if err != nil {
		return nil, err
	}
	if bytes.ContainsAny(data, "\r\n") {
		return nil, errNotContract
	}
	frame := make([]byte, 0, len("event: \ndata: \n\n")+len(ev.Type)+len(data))
	frame = append(frame, "event: "...)
	frame = append(frame, ev.Type...)
	frame = append(frame, "\ndata: "...)
	frame = append(frame, data...)
	frame = append(frame, "\n\n"...)
	return frame, nil
}

// ---- the connection ----

// eventStream writes one stream's frames.
type eventStream struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
}

// startStream clears the connection's write deadline (the server's
// WriteTimeout would cut the stream) and sends the stream's headers. An error
// means the connection's deadline cannot be controlled; nothing is written
// then, and the caller answers 500.
func (d *Demo) startStream(w http.ResponseWriter) (*eventStream, error) {
	// The controller goes to the writer under the route's recorder: the
	// recorder's Flush cannot report an error, its writer's can.
	rc := http.NewResponseController(unwrap(w))
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear the write deadline: %w", err)
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	return &eventStream{w: w, rc: rc, timeout: d.cfg.streamWriteTimeout()}, nil
}

// send writes and flushes one frame under its own deadline, then clears the
// deadline again, so that it never runs out while the stream waits for the
// next frame (on HTTP/2 an expired deadline resets the stream even then). Any
// error ends the stream.
func (s *eventStream) send(frame []byte) error {
	if err := s.rc.SetWriteDeadline(wallDeadline(s.timeout)); err != nil {
		return err
	}
	if _, err := s.w.Write(frame); err != nil {
		return err
	}
	if err := s.rc.Flush(); err != nil {
		return err
	}
	return s.rc.SetWriteDeadline(time.Time{})
}

// close bounds the write the server makes once the handler has returned (the
// end of the chunked body), so a client that stopped reading cannot hold the
// connection there either. The server clears the deadline after that write.
func (s *eventStream) close() {
	_ = s.rc.SetWriteDeadline(wallDeadline(s.timeout))
}

// wallDeadline is a socket deadline d from now. The network poller compares
// deadlines with the wall clock, so this is the one place the package reads
// time.Now rather than Deps.Clock.
func wallDeadline(d time.Duration) time.Time { return time.Now().Add(d) }

// streamWriteTimeout is Config.StreamWriteTimeout, or its default when unset.
func (c Config) streamWriteTimeout() time.Duration {
	if c.StreamWriteTimeout > 0 {
		return c.StreamWriteTimeout
	}
	return DefaultConfig().StreamWriteTimeout
}
