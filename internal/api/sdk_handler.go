package api

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleRegister registers (or re-registers) an SDK instance and answers with
// its materialized desired state (desired-state protocol v1 §2 Register).
//
// @Summary      Register SDK instance
// @Description  Registers an atropos-go SDK process (in-process embed or atroposd sidecar)
// @Description  with its host kind and fault capabilities. The 201 response carries
// @Description  status="registered" plus the instance's materialized desired state —
// @Description  the same RuleSync shape a 200 poll serves (version = generation, etag,
// @Description  rules, active_faults, freeze_cfg, replay_set), already filtered by the
// @Description  advertised capabilities — so the SDK converges before its first poll.
// @Description  Registering with capabilities different from the stored ones bumps the
// @Description  service generation. If the desired state cannot be materialized the
// @Description  registration still succeeds with no document (version 0, empty etag,
// @Description  rules and active_faults null), which the SDK applies as nothing.
// @Tags         sdk
// @Accept       json
// @Produce      json
// @Param        instance  body      model.SDKInstance         true  "SDK instance metadata (id, service, address, poll_interval_ms, routes, host, capabilities)"
// @Success      201       {object}  api.RegisterResponseWire
// @Failure      400       {object}  api.ErrorResponse  "invalid JSON or registration error"
// @Router       /sdk/register [post]
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var inst model.SDKInstance
	if err := readJSON(r, &inst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	ctx := r.Context()
	prev, err := s.registry.Get(ctx, inst.ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.logger.Warn("register: read previous registration failed", "id", inst.ID, "error", err)
	}
	if err := s.registry.Register(ctx, &inst); err != nil {
		s.logger.Error("sdk register failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("sdk registered", "id", inst.ID, "service", inst.Service, "host", inst.Host, "capabilities", inst.Capabilities)

	// A changed capability set changes this instance's document; bump so its
	// long-poll (and anyone else's) re-materializes. A fresh registration
	// changes nobody's document: the response below already carries it.
	if prev != nil && !sameCapabilities(prev.Capabilities, inst.Capabilities) {
		s.bumpGeneration(ctx, inst.Service)
	}

	resp := RegisterResponseWire{Status: "registered"}
	doc, err := s.materializer.Materialize(ctx, inst.Service, &inst)
	if err != nil {
		// Never fail registration on a data error: the SDK would refuse to
		// start. Answer with no document instead — version 0, an empty etag,
		// null rules and active_faults, no freeze_cfg or replay_set — which
		// the SDK applies as nothing, so an instance re-registering while its
		// polls fail keeps what it has armed ([] would clear it). The poll
		// reports the error as a 500 until the data is fixed.
		s.logger.Error("register: materialize desired state failed", "service", inst.Service, "error", err)
		writeJSON(w, http.StatusCreated, resp)
		return
	}
	resp.RuleSyncWire = ruleSyncFrom(doc)
	writeJSON(w, http.StatusCreated, resp)
}

// sameCapabilities reports whether two advertised capability lists name the
// same set: order and duplicates do not matter, and nil equals [] (both are
// the wire's "absent = all"). Strings compare verbatim, so an unknown one the
// filter ignores still counts as a change.
func sameCapabilities(a, b []string) bool {
	set := func(caps []string) map[string]bool {
		m := make(map[string]bool, len(caps))
		for _, c := range caps {
			m[c] = true
		}
		return m
	}
	sa, sb := set(a), set(b)
	if len(sa) != len(sb) {
		return false
	}
	for c := range sa {
		if !sb[c] {
			return false
		}
	}
	return true
}

// handleDeregister removes an SDK instance.
//
// @Summary      Deregister SDK instance
// @Tags         sdk
// @Param        id   path  string  true  "Instance ID"
// @Success      204  "instance deregistered"
// @Failure      404  {object}  api.ErrorResponse  "instance not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/register/{id} [delete]
func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.sdk.Deregister(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "instance not found")
			return
		}
		s.logger.Error("sdk deregister failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to deregister")
		return
	}

	s.logger.Info("sdk deregistered", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// handleListInstances returns all registered SDK instances.
//
// @Summary      List SDK instances
// @Description  Returns all currently registered SDK instances. Status is computed
// @Description  (alive/stale/dead) from last-poll-at.
// @Tags         sdk
// @Produce      json
// @Success      200  {array}   model.SDKInstance
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/instances [get]
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	instances, err := s.sdk.List(r.Context())
	if err != nil {
		s.logger.Error("list instances failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list instances")
		return
	}
	if instances == nil {
		instances = []*model.SDKInstance{}
	}
	writeJSON(w, http.StatusOK, instances)
}

// maxPollWait caps the long-poll hold (contract §2: wait ≤ 60 s).
const maxPollWait = 60 * time.Second

// handlePollRules serves an instance's desired state (desired-state protocol v1).
//
// @Summary      Poll for desired state (long-poll)
// @Description  Records the instance's observed generation (version) and poll time,
// @Description  materializes DesiredState(service, instance) and compares its ETag with
// @Description  If-None-Match. With wait>0 (seconds, max 60) and a matching ETag the
// @Description  request is held until the service's generation changes or wait elapses,
// @Description  then re-materialized. 200 carries the RuleSync body (version = generation,
// @Description  etag, rules, active_faults, freeze_cfg, replay_set) with ETag and
// @Description  X-Atropos-Generation headers; 304 carries the same headers and no body —
// @Description  the instance adopts X-Atropos-Generation without re-applying.
// @Tags         sdk
// @Produce      json
// @Param        service        query   string  true   "service name"
// @Param        instance_id    query   string  false  "SDK instance ID; selects the capability filter and records observed state"
// @Param        version        query   integer false  "generation the instance currently holds (recorded as observed_generation; at most 9223372036854775807)"
// @Param        wait           query   integer false  "long-poll hold in seconds (0-60, default 0)"
// @Param        If-None-Match  header  string  false  "ETag of the last document the instance applied or confirmed"
// @Success      200  {object}  api.RuleSyncWire  "desired state; headers ETag, X-Atropos-Generation"
// @Success      304  "unchanged; headers ETag, X-Atropos-Generation"
// @Failure      400  {object}  api.ErrorResponse  "service query parameter required, or version out of range"
// @Failure      500  {object}  api.ErrorResponse  "desired state could not be materialized"
// @Router       /sdk/rules [get]
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	service := q.Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query parameter required")
		return
	}
	// Absent or garbage reads as 0. observed_generation is a BIGINT, so a
	// larger value (ParseUint answers MaxUint64 for one that overflows) would
	// fail the observed-state write: refuse it before touching the store.
	observed, _ := strconv.ParseUint(q.Get("version"), 10, 64)
	if observed > math.MaxInt64 {
		writeError(w, http.StatusBadRequest, "version out of range")
		return
	}
	wait := parseWait(q.Get("wait"))
	instanceID := q.Get("instance_id")
	ctx := r.Context()

	// Observed state first (contract §2): the generation the instance holds and
	// its poll time, before any 304 decision. Best-effort — a purged row must
	// not break polling.
	if instanceID != "" {
		if err := s.registry.RecordPoll(ctx, instanceID, observed); err != nil {
			s.logger.Warn("record poll failed", "instance_id", instanceID, "error", err)
		}
	}
	inst, err := s.pollInstance(ctx, instanceID)
	if err != nil {
		s.pollFailed(ctx, w, "read instance", service, err)
		return
	}
	doc, err := s.materializer.Materialize(ctx, service, inst)
	if err != nil {
		// Never fall back to a partial document: a 500 makes the SDK keep its
		// stale state and log degraded, its designed failure mode.
		s.pollFailed(ctx, w, "materialize desired state", service, err)
		return
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if wait > 0 && etagMatches(ifNoneMatch, doc.ETag) {
		// Hold. The server's WriteTimeout (30s in cmd/manteion) would kill a
		// longer hold, so push the deadline past it, as the SSE handler does.
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(wait + 15*time.Second)); err != nil {
			s.logger.Debug("poll: extend write deadline failed", "error", err) // httptest.ResponseRecorder: unsupported
		}
		if _, err := s.generations.Wait(ctx, service, doc.Generation, wait); err != nil {
			s.pollFailed(ctx, w, "wait for a generation change", service, err)
			return
		}
		// Re-read the instance: it may have re-registered with other
		// capabilities while parked (the change that bumped).
		if inst, err = s.pollInstance(ctx, instanceID); err != nil {
			s.pollFailed(ctx, w, "re-read instance after the hold", service, err)
			return
		}
		if doc, err = s.materializer.Materialize(ctx, service, inst); err != nil {
			s.pollFailed(ctx, w, "re-materialize after the hold", service, err)
			return
		}
	}

	w.Header().Set("ETag", `"`+doc.ETag+`"`)
	w.Header().Set("X-Atropos-Generation", strconv.FormatUint(doc.Generation, 10))
	if etagMatches(ifNoneMatch, doc.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, ruleSyncFrom(doc))
}

// pollInstance reads the polling instance's registration, which selects its
// capability filter. No instance_id, or one whose row is gone (a replaced or
// purged pod still polling), means no registration: the unfiltered document.
func (s *Server) pollInstance(ctx context.Context, id string) (*model.SDKInstance, error) {
	if id == "" {
		return nil, nil
	}
	inst, err := s.registry.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return inst, err
}

// pollFailed answers a poll whose step failed: with nothing when the client
// has gone, since a failure is then likely its cancellation (Wait returns
// ctx.Err(), and SpecResolver reports a cancelled read as a missing spec);
// otherwise with a 500.
func (s *Server) pollFailed(ctx context.Context, w http.ResponseWriter, step, service string, err error) {
	if ctx.Err() != nil {
		return
	}
	s.logger.Error("poll: "+step+" failed", "service", service, "error", err)
	writeError(w, http.StatusInternalServerError, "failed to materialize desired state")
}

// parseWait parses the wait query param: seconds, clamped to [0, maxPollWait]
// before conversion (a huge value must not overflow time.Duration); absent,
// negative or unparseable → 0.
func parseWait(s string) time.Duration {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}
	if n >= int(maxPollWait/time.Second) {
		return maxPollWait
	}
	return time.Duration(n) * time.Second
}

// etagMatches reports whether the If-None-Match header names etag: weak
// validators (W/) and quotes are stripped, a comma-separated list is matched
// element-wise, anything else is a mismatch.
func etagMatches(header, etag string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(part), "W/"))
		if strings.Trim(part, `"`) == etag && etag != "" {
			return true
		}
	}
	return false
}

// handleInit is the startup readiness check for SDK initialization.
// Returns 200 when manteion is ready to serve rules.
//
// @Summary      SDK init readiness
// @Description  Returns 200 with status="ready" when manteion is ready to
// @Description  serve rules. Used by atropos-go SDKs as a startup gate.
// @Tags         sdk
// @Produce      json
// @Success      200  {object}  map[string]string  "{status: ready}"
// @Router       /sdk/init [get]
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	if err := s.dbPing.PingContext(r.Context()); err != nil {
		s.logger.Warn("sdk/init: db unreachable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "database unreachable",
		})
		return
	}
	if _, err := s.rulever.Version(r.Context()); err != nil {
		s.logger.Warn("sdk/init: rule store not initialized", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "rule store not initialized",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
