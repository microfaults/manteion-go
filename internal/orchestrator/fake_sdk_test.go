package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// fakeSDK is an httptest SDK control server for the orchestrator tests. It
// serves the one route manteion still calls on an instance at a phase exit,
// GET /cachebox/fidelity (the verdict pull and the drain fallback); phase entry
// and exit reach instances through their polls, which the tests simulate on
// the database (registerSDK, ackGenerations, ackAssigned).
type fakeSDK struct {
	server *httptest.Server
	// fidelity, when set, is served at GET /cachebox/fidelity; nil ⇒ 503 (used
	// to simulate a missing-telemetry instance).
	fidelity *atroposdk.FidelitySnapshot
	// onFidelity, when set, runs in the fidelity handler before it answers;
	// genAtPull is where a test's onFidelity records the generation it saw.
	onFidelity func()
	genAtPull  atomic.Uint64
}

func newFakeSDK(t *testing.T) *fakeSDK {
	t.Helper()
	f := &fakeSDK{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /cachebox/fidelity", func(w http.ResponseWriter, _ *http.Request) {
		if f.onFidelity != nil {
			f.onFidelity()
		}
		if f.fidelity == nil {
			http.Error(w, "no snapshot", http.StatusServiceUnavailable)
			return
		}
		writeTestJSON(w, http.StatusOK, *f.fidelity)
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// lookSpy is the registry the convergence barrier reads (through the
// atrocontrol controller): every look at service is counted and, once its
// read has returned, announced on looked, without ever blocking. Anything
// started on looked can change only what the later looks read.
type lookSpy struct {
	atrocontrol.InstanceResolver
	service string
	looked  chan struct{}
	looks   atomic.Int64
}

func (s *lookSpy) ForService(ctx context.Context, service string) ([]*model.SDKInstance, error) {
	insts, err := s.InstanceResolver.ForService(ctx, service)
	if service == s.service {
		s.looks.Add(1)
		select {
		case s.looked <- struct{}{}:
		default:
		}
	}
	return insts, err
}

// spyOnLooks points o's controller at a lookSpy for service. A simulated
// instance started on its looked channel acks only once the barrier's first
// look has read it lagging, so a barrier that waited looks at least twice, and
// an entry that returned without waiting leaves the instance behind, where its
// row shows it.
func spyOnLooks(o *Orchestrator, service string) *lookSpy {
	spy := &lookSpy{InstanceResolver: &atrocontrol.RepoResolver{Repo: testSDKRepo}, service: service, looked: make(chan struct{}, 1)}
	o.controller = newTestController(spy, o.logger)
	return spy
}

func writeTestJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// registerSDK registers one live instance of service at addr, deregisters it
// on cleanup, and returns its id. Register stamps last_poll_at=now(), so the
// instance is "alive".
func registerSDK(t *testing.T, service, addr string) string {
	t.Helper()
	inst := &model.SDKInstance{ID: id.New("sdk"), Service: service, Version: "test", Address: addr}
	if err := testSDKRepo.Register(context.Background(), inst); err != nil {
		t.Fatalf("register sdk: %v", err)
	}
	t.Cleanup(func() { _ = testSDKRepo.Deregister(context.Background(), inst.ID) })
	return inst.ID
}

// ackGenerations simulates a converging instance: every 20ms it records the
// service's current generation as observed and, when artifact != "", reports
// the replay set committed under it. Stops at test cleanup.
func ackGenerations(t *testing.T, o *Orchestrator, instanceID, service, artifact string) {
	t.Helper()
	simulateInstance(t, o, nil, instanceID, service, committedSet(artifact))
}

// committedSet is the status report of an instance holding artifact
// committed; "" reports no replay set.
func committedSet(artifact string) func(context.Context) *model.ObservedStatus {
	return func(context.Context) *model.ObservedStatus {
		if artifact == "" {
			return nil
		}
		return &model.ObservedStatus{ReplaySet: &model.ReplaySetStatus{ID: artifact, State: "committed"}}
	}
}

// ackAssigned is ackGenerations for a replay set that is not built yet: every
// 20ms it records the service's current generation as observed and, once a
// set is assigned to (phaseID, service), reports that set in state, with errs
// as the instance's errors. Stops at test cleanup.
func ackAssigned(t *testing.T, o *Orchestrator, instanceID, phaseID, service, state string, errs ...string) {
	t.Helper()
	simulateInstance(t, o, nil, instanceID, service, assignedSet(o, phaseID, service, state, errs...))
}

// assignedSet is the status report of an instance holding, in state, the set
// assigned to (phaseID, service) once there is one, with errs as its errors.
func assignedSet(o *Orchestrator, phaseID, service, state string, errs ...string) func(context.Context) *model.ObservedStatus {
	return func(ctx context.Context) *model.ObservedStatus {
		a, err := o.artifacts.ForPhaseService(ctx, phaseID, service)
		if err != nil || a == nil {
			return nil
		}
		return &model.ObservedStatus{Errors: errs, ReplaySet: &model.ReplaySetStatus{ID: a.ID, State: state}}
	}
}

// simulateInstance runs the loop behind ackGenerations and ackAssigned on a
// goroutine that makes no testing.T calls: every 20ms it records the
// service's current generation as the instance's observed one (a poll) and,
// when report returns a status, stores it applied at that generation (a status
// report). With start non-nil the loop begins only once start delivers (a
// lookSpy's first look). It stops when the test is cleaned up.
func simulateInstance(t *testing.T, o *Orchestrator, start <-chan struct{}, instanceID, service string, report func(context.Context) *model.ObservedStatus) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if start != nil {
			select {
			case <-ctx.Done():
				return
			case <-start:
			}
		}
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				gen, err := o.generations.Get(ctx, service)
				if err != nil {
					continue
				}
				_ = testSDKRepo.RecordPoll(ctx, instanceID, gen)
				if st := report(ctx); st != nil {
					st.AppliedGeneration = gen
					_ = testSDKRepo.RecordStatus(ctx, instanceID, *st)
				}
			}
		}
	}()
}
