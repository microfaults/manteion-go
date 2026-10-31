package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// bumpService bumps svc's generation and returns it: a convergence target.
func bumpService(t *testing.T, o *Orchestrator, svc string) uint64 {
	t.Helper()
	g, err := o.generations.Bump(context.Background(), svc)
	if err != nil {
		t.Fatalf("bump %s: %v", svc, err)
	}
	return g
}

// markSuspect backdates the instance's last poll by 30 s: past twice its 10 s
// poll interval and short of five times, so the liveness classification reads
// it as stale (suspect: missed polls, not dead yet) for the next 20 s.
func markSuspect(t *testing.T, instanceID string) {
	t.Helper()
	if _, err := testDB.ExecContext(context.Background(),
		`UPDATE sdk_instances SET last_poll_at = now() - interval '30 seconds' WHERE id = $1`, instanceID); err != nil {
		t.Fatalf("mark %s suspect: %v", instanceID, err)
	}
}

// registerHost registers one live instance of service whose host advertises
// caps, and deregisters it on cleanup.
func registerHost(t *testing.T, service string, caps []string) string {
	t.Helper()
	inst := &model.SDKInstance{ID: id.New("sdk"), Service: service, Version: "test", Address: "http://host.invalid", Capabilities: caps}
	if err := testSDKRepo.Register(context.Background(), inst); err != nil {
		t.Fatalf("register sdk: %v", err)
	}
	t.Cleanup(func() { _ = testSDKRepo.Deregister(context.Background(), inst.ID) })
	return inst.ID
}

// The barrier compares observed_generation with >=, never ==: an instance
// that already adopted a later generation (another bump landed) has converged
// too.
func TestWaitConverged_AliveInstancesAtOrPastTheTarget(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	svc := id.New("svc-conv")
	target := bumpService(t, o, svc)
	acking := registerSDK(t, svc, "http://acking.invalid")
	ahead := registerSDK(t, svc, "http://ahead.invalid")
	ackGenerations(t, o, acking, svc, "")
	if err := testSDKRepo.RecordPoll(ctx, ahead, target+2); err != nil {
		t.Fatal(err)
	}
	if err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 5*time.Second, nil); err != nil {
		t.Fatalf("waitConverged = %v, want nil", err)
	}
}

// The barrier waits on alive instances only. A suspect one (missed polls, not
// dead yet: a crashed pod that never deregistered) is not waited on, but it is
// named in the log and, when the wait times out, in the error next to the
// lagging ones.
func TestWaitConverged_SuspectInstanceIsNamedNotWaitedOn(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	logs := &logBuffer{}
	o.logger = slog.New(slog.NewTextHandler(logs, nil))
	svc := id.New("svc-suspect")
	target := bumpService(t, o, svc)
	acking := registerSDK(t, svc, "http://acking.invalid")
	ackGenerations(t, o, acking, svc, "")
	suspect := registerSDK(t, svc, "http://suspect.invalid")
	markSuspect(t, suspect)

	if err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 5*time.Second, nil); err != nil {
		t.Fatalf("waitConverged = %v, want nil: a suspect instance is not waited on", err)
	}
	if got := logs.String(); !strings.Contains(got, suspect) || !strings.Contains(got, "not waited on") {
		t.Errorf("logs = %q, want the suspect instance %s named as not waited on", got, suspect)
	}

	// An alive instance that never polls the target is waited on until the
	// timeout, and the timeout error names it and the suspect one.
	lagging := registerSDK(t, svc, "http://lagging.invalid")
	err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 300*time.Millisecond, nil)
	if err == nil {
		t.Fatal("waitConverged = nil, want a timeout naming the lagging instance")
	}
	for _, want := range []string{lagging + "(" + svc + "): observed=0", "suspect", suspect} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error = %q, want it to contain %q", err, want)
		}
	}
	// The timeout is typed: the API answers it 504 with both lists.
	var nc *ErrNotConverged
	if !errors.As(err, &nc) || nc.Timeout != 300*time.Millisecond ||
		len(nc.Lagging) != 1 || !strings.HasPrefix(nc.Lagging[0], lagging+"(") ||
		len(nc.Suspect) != 1 || !strings.HasPrefix(nc.Suspect[0], suspect+"(") {
		t.Errorf("timeout error = %#v, want an *ErrNotConverged listing %s as lagging and %s as suspect", err, lagging, suspect)
	}
	if strings.Contains(err.Error(), acking) {
		t.Errorf("timeout error = %q names the converged instance %s", err, acking)
	}
}

// Whatever is already known ends the wait at once instead of burning the
// timeout: a frozen service without a generation target (its bump failed:
// never "nothing to wait for") or without an alive instance to hold the replay
// set (INV-4), an alive host that cannot hold one, and an instance reporting
// the assigned set failed.
func TestWaitConverged_FatalAtOnce(t *testing.T) {
	const art = "sha256:aaaa"
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, o *Orchestrator, svc string, target uint64) (targets map[string]uint64, wants []string)
	}{
		{"frozen, bump failed", func(t *testing.T, o *Orchestrator, svc string, target uint64) (map[string]uint64, []string) {
			registerSDK(t, svc, "http://alive.invalid")
			return map[string]uint64{}, []string{svc, "no generation target"}
		}},
		{"frozen, no instance", func(t *testing.T, o *Orchestrator, svc string, target uint64) (map[string]uint64, []string) {
			return map[string]uint64{svc: target}, []string{svc, "no live instances"}
		}},
		{"frozen, only a suspect instance", func(t *testing.T, o *Orchestrator, svc string, target uint64) (map[string]uint64, []string) {
			suspect := registerSDK(t, svc, "http://suspect.invalid")
			markSuspect(t, suspect)
			return map[string]uint64{svc: target}, []string{svc, "no live instances", suspect}
		}},
		{"frozen, host without a cache-box", func(t *testing.T, o *Orchestrator, svc string, target uint64) (map[string]uint64, []string) {
			host := registerHost(t, svc, []string{"inline", "network"})
			return map[string]uint64{svc: target}, []string{host, "cachebox"}
		}},
		{"replay set failed", func(t *testing.T, o *Orchestrator, svc string, target uint64) (map[string]uint64, []string) {
			inst := registerSDK(t, svc, "http://failed.invalid")
			ctx := context.Background()
			if err := testSDKRepo.RecordPoll(ctx, inst, target); err != nil {
				t.Fatal(err)
			}
			if err := testSDKRepo.RecordStatus(ctx, inst, model.ObservedStatus{AppliedGeneration: target,
				Errors: []string{"fetch: 416"}, ReplaySet: &model.ReplaySetStatus{ID: art, State: "failed"}}); err != nil {
				t.Fatal(err)
			}
			return map[string]uint64{svc: target}, []string{inst, art, "failed", "fetch: 416"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			o := newOrch(t, "")
			svc := id.New("svc-fatal")
			targets, wants := tc.setup(t, o, svc, bumpService(t, o, svc))
			const timeout = 5 * time.Second
			start := time.Now()
			err := o.waitConverged(ctx, targets, map[string]string{svc: art}, timeout, nil)
			if err == nil {
				t.Fatal("waitConverged = nil, want a fatal error")
			}
			if elapsed := time.Since(start); elapsed > timeout/2 {
				t.Errorf("took %v: a known outcome must end the wait before the %v timeout", elapsed, timeout)
			}
			for _, want := range wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}

// A frozen service converges only when the instance holds the assigned set
// committed. Anything else is lagging, not fatal: a fetch in flight, or a
// failure reported for another set (the SDK retries a failed pull on its own,
// and a set it failed earlier is not the one desired now).
func TestWaitConverged_ReplaySetMustBeTheAssignedOneCommitted(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	svc := id.New("svc-replay")
	const art = "sha256:bbbb"
	target := bumpService(t, o, svc)
	inst := registerSDK(t, svc, "http://replay.invalid")
	if err := testSDKRepo.RecordPoll(ctx, inst, target); err != nil {
		t.Fatal(err)
	}
	report := func(id, state string) {
		t.Helper()
		if err := testSDKRepo.RecordStatus(ctx, inst, model.ObservedStatus{AppliedGeneration: target,
			ReplaySet: &model.ReplaySetStatus{ID: id, State: state}}); err != nil {
			t.Fatal(err)
		}
	}
	targets, artifacts := map[string]uint64{svc: target}, map[string]string{svc: art}

	for _, tc := range []struct{ id, state, want string }{
		{"", "", "replay_set=none target=" + art + "/committed"},
		{"sha256:older", "failed", "replay_set=sha256:older/failed target=" + art + "/committed"},
		{art, "fetching", "replay_set=" + art + "/fetching target=" + art + "/committed"},
	} {
		if tc.id == "" {
			if err := testSDKRepo.RecordStatus(ctx, inst, model.ObservedStatus{AppliedGeneration: target}); err != nil {
				t.Fatal(err)
			}
		} else {
			report(tc.id, tc.state)
		}
		err := o.waitConverged(ctx, targets, artifacts, 200*time.Millisecond, nil)
		if err == nil || !strings.Contains(err.Error(), "still lagging") || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("replay set %q/%q: err = %v, want a lagging timeout with %q", tc.id, tc.state, err, tc.want)
		}
	}
	report(art, "committed")
	if err := o.waitConverged(ctx, targets, artifacts, 5*time.Second, nil); err != nil {
		t.Fatalf("committed: waitConverged = %v, want nil", err)
	}
}

// seedBaseline completes a baseline with n recorded entries for service.
func seedBaseline(t *testing.T, o *Orchestrator, expID, service string, n int) *model.ExperimentPhase {
	t.Helper()
	return seedBaselineAt(t, o, expID, service, 0, n)
}

// seedBaselineAt is seedBaseline at position pos.
func seedBaselineAt(t *testing.T, o *Orchestrator, expID, service string, pos, n int) *model.ExperimentPhase {
	t.Helper()
	base := mkCompletedBaseline(t, expID, pos)
	entries := make([]atroposdk.CacheBoxWireEntry, n)
	for i := range entries {
		entries[i] = atroposdk.CacheBoxWireEntry{Key: "k" + string(rune('a'+i)), StatusCode: 200, Body: []byte("v")}
	}
	if err := o.cacheStore.Append(expID, base.ID, service, entries); err != nil {
		t.Fatal(err)
	}
	return base
}

// mkRunningRulePhase creates a running phase that freezes nothing and attaches
// one enabled inline-latency rule targeting service (with its fault spec).
func mkRunningRulePhase(t *testing.T, expID, service string, pos int) *model.ExperimentPhase {
	t.Helper()
	p := mkPendingRulePhase(t, expID, service, pos)
	if err := testExpRepo.UpdatePhaseStatus(context.Background(), p.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}
	p.Status = "running"
	return p
}

// mkPendingRulePhase is mkRunningRulePhase left pending, for StartPhase.
func mkPendingRulePhase(t *testing.T, expID, service string, pos int) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	tag := id.New("rulephase")
	specID := "spec-" + tag
	spec := &model.FaultSpec{
		ID: specID, Name: "rulephase-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), specID) })
	rule := &model.Rule{
		ID: "rule-" + tag, Name: "rule-" + tag, Service: service, Enabled: true, Priority: 10,
		Mode: "background", StartPolicy: "always_start",
		Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		Match:     model.MatchCriteria{InjectionPoint: "ingress"},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: expID, Name: fmt.Sprintf("rules-%d", pos), Position: pos, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := testExpRepo.AttachPhaseRules(ctx, p.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach phase rule: %v", err)
	}
	return p
}

// waitGeneration polls (no testing.T calls, safe on any goroutine) until svc's
// generation passes after, for at most 5s, and reports whether it did.
func waitGeneration(o *Orchestrator, svc string, after uint64) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if g, err := o.generations.Get(context.Background(), svc); err == nil && g > after {
			return true
		}
	}
	return false
}

func TestEnterPhase_ConvergesWhenInstancesAck(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false
	o.WithConvergeTimeout(5 * time.Second) // returns once the instance acks
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 2)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	sdk := newFakeSDK(t)
	instID := registerSDK(t, "frontend", sdk.server.URL)
	// The artifact id is only known once built; commit whatever is assigned,
	// but only once the barrier has looked at the instance.
	spy := spyOnLooks(o, "frontend")
	simulateInstance(t, o, spy.looked, instID, "frontend", assignedSet(o, iso.ID, "frontend", "committed"))

	if err := o.enterPhase(ctx, iso, true); err != nil {
		t.Fatalf("enterPhase: %v", err)
	}
	// The instance acks only after the barrier's first look has read the
	// fleet, so a barrier that waited looked again; one that returned on its
	// first look did not wait, however quickly the instance acked after it.
	if n := spy.looks.Load(); n < 2 {
		t.Fatalf("the barrier looked %d time(s), want at least 2: the entry did not wait for its fleet", n)
	}
	a, err := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend")
	if err != nil || a == nil || a.EntryCount != 2 || a.ExperimentID != exp.ID || !strings.HasPrefix(a.ID, "sha256:") {
		t.Fatalf("artifact = %+v, %v", a, err)
	}
	if got := getPhase(t, iso.ID).Status; got != "running" {
		t.Fatalf("phase status = %q, want running", got)
	}
	// The entry returned only once the fleet held what it bumped for.
	target, _ := o.generations.Get(ctx, "frontend")
	inst, err := testSDKRepo.Get(ctx, instID)
	if err != nil {
		t.Fatal(err)
	}
	if inst.ObservedGeneration < target || inst.ReplaySetID != a.ID || inst.ReplaySetState != "committed" {
		t.Fatalf("instance observed=%d replay_set=%q/%q, want observed >= %d holding %s committed: the entry did not wait for its fleet",
			inst.ObservedGeneration, inst.ReplaySetID, inst.ReplaySetState, target, a.ID)
	}
}

func TestEnterPhase_ReplaySetNotCommittedAborts(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	ackGenerations(t, o, instID, "frontend", "") // generation yes, replay set never

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "converge gate") || !strings.Contains(err.Error(), instID) || !strings.Contains(err.Error(), "replay_set") {
		t.Fatalf("enterPhase err = %v, want a converge-gate abort naming %s and its replay_set state", err, instID)
	}
	if got := getPhase(t, iso.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, instID) {
		t.Fatalf("phase = %s / %q, want failed with the lagging instance", got.Status, got.FailureReason)
	}
}

func TestEnterPhase_ReplaySetFailedAbortsEarly(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithConvergeTimeout(5 * time.Second)
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	ackAssigned(t, o, instID, iso.ID, "frontend", "failed", "fetch: 416")

	start := time.Now()
	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "failed") || !strings.Contains(err.Error(), "fetch: 416") {
		t.Fatalf("enterPhase err = %v, want an early abort carrying the instance's error", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatalf("took %v: a failed replay set must abort before the converge timeout", time.Since(start))
	}
}

func TestEnterPhase_FrozenServiceWithoutInstancesAborts(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	seedBaseline(t, o, exp.ID, "frontend", 1)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "no live instances") {
		t.Fatalf("enterPhase err = %v, want 'no live instances' (INV-4)", err)
	}
}

func TestEnterPhase_LaggingInstanceFailsWithList(t *testing.T) {
	// A fault-rule phase (no freeze): a registered instance that never polls
	// the new generation fails the phase, named with observed/target.
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	ph := mkRunningRulePhase(t, exp.ID, "frontend", 0)
	instID := registerSDK(t, "frontend", newFakeSDK(t).server.URL)
	err := o.enterPhase(ctx, ph, true)
	if err == nil || !strings.Contains(err.Error(), instID) || !strings.Contains(err.Error(), "observed=") {
		t.Fatalf("enterPhase err = %v, want the lagging instance with observed/target", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, "converge gate") {
		t.Fatalf("phase = %s / %q, want failed at the converge gate", got.Status, got.FailureReason)
	}
}

// The converge gate can hold its caller for the whole timeout. A caller that
// goes away meanwhile (an operator's client hanging up on a start) must still
// leave the phase failed with its reason, not wedged 'running' with no load
// driver and no poller: the failure is finalized on a context the caller
// cannot cancel.
func TestEnterPhase_CallerGoneStillFailsThePhase(t *testing.T) {
	o := newOrch(t, "")
	o.WithConvergeTimeout(time.Second)
	exp := mkExp(t)
	svc := id.New("svc-gone")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	registerSDK(t, svc, "http://never-acks.invalid")
	g0 := generation(t, o, svc)

	ctx, hangUp := context.WithCancel(context.Background())
	defer hangUp()
	go func() {
		if waitGeneration(o, svc, g0) { // the entry bumped: it is waiting now
			hangUp()
		}
	}()
	err := o.enterPhase(ctx, ph, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("enterPhase err = %v, want the converge gate cut short by the caller", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, "converge gate") {
		t.Fatalf("phase = %s / %q, want failed at the converge gate", got.Status, got.FailureReason)
	}
}

// A pause (or stop) that lands while the entry waits for the fleet owns the
// phase from then on: once the wait ends, the entry must see the phase left
// 'running' and start no load for it.
func TestEnterPhase_PausedWhileConvergingStartsNoLoad(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	o.WithConvergeTimeout(5 * time.Second)
	exp := mkExp(t)
	svc := id.New("svc-paused")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1}) // a run would drive it
	inst := registerSDK(t, svc, "http://acks-late.invalid")
	g0 := generation(t, o, svc)

	paused := make(chan error, 1)
	go func() {
		if !waitGeneration(o, svc, g0) {
			paused <- errors.New("the entry never bumped")
			return
		}
		err := o.PausePhase(ctx, ph.ID)
		if g, gerr := o.generations.Get(ctx, svc); err == nil && gerr == nil {
			err = testSDKRepo.RecordPoll(ctx, inst, g) // the instance converges after the pause
		}
		paused <- err
	}()
	err := o.enterPhase(ctx, ph, true)
	if perr := <-paused; perr != nil {
		t.Fatalf("pause while converging: %v", perr)
	}
	var state *ErrInvalidState
	if !errors.As(err, &state) || state.Current != "paused" {
		t.Fatalf("enterPhase err = %v, want an invalid-state refusal naming the paused phase", err)
	}
	if got := getPhase(t, ph.ID).Status; got != "paused" {
		t.Fatalf("phase status = %q, want paused (the pause owns it)", got)
	}
	if n := fz.runCount(); n != 0 {
		t.Fatalf("zeus started %d run(s) for a phase that left running", n)
	}
}

// bumpFailingStore is a generation store whose bumps fail: reads still work.
type bumpFailingStore struct{ desired.Store }

func (bumpFailingStore) Bump(context.Context, *sql.Tx, string) (uint64, error) {
	return 0, errors.New("generation store down")
}

// Ruling 2: a bump that fails is a convergence failure, never "nothing to wait
// for": the phase fails at the converge gate instead of starting on instances
// that were never told about it.
func TestEnterPhase_FailedBumpFailsThePhase(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.generations = desired.NewGenerations(bumpFailingStore{store.NewGenerationRepo(testDB)}, desired.NewBroker())
	exp := mkExp(t)
	svc := id.New("svc-nobump")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	err := o.enterPhase(ctx, ph, true)
	if err == nil || !strings.Contains(err.Error(), "converge gate") || !strings.Contains(err.Error(), svc) {
		t.Fatalf("enterPhase err = %v, want a converge-gate failure naming %s", err, svc)
	}
	if got := getPhase(t, ph.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, "generation store down") {
		t.Fatalf("phase = %s / %q, want failed with the bump error", got.Status, got.FailureReason)
	}
}

func TestFinishPhase_VerdictBeforeBump(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	mkCompletedBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	g0, _ := o.generations.Get(ctx, "frontend")

	sdk := newFakeSDK(t)
	sdk.fidelity = cleanSnapshot()
	// The fake records the service generation at the moment of the fidelity pull.
	sdk.onFidelity = func() { g, _ := o.generations.Get(context.Background(), "frontend"); sdk.genAtPull.Store(g) }
	registerSDK(t, "frontend", sdk.server.URL)

	if !o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("finishPhase returned false")
	}
	if got := sdk.genAtPull.Load(); got != g0 {
		t.Fatalf("fidelity pulled at generation %d, want %d (before the thaw bump)", got, g0)
	}
	if g1, _ := o.generations.Get(ctx, "frontend"); g1 <= g0 {
		t.Fatalf("phase exit did not bump: %d → %d", g0, g1)
	}
	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil || v.Verdict != "VALID" {
		t.Fatalf("verdict = %+v, %v", v, err)
	}
}

// Pause thaws under desired state (a paused phase leaves every service's
// document), so a resume is an entry: bump and wait with the convergence
// timeout. A resume whose fleet does not converge fails and leaves the phase
// paused, through the phase resume and the experiment resume alike (which
// reports that failure, not "no paused phase"); once the fleet converges, the
// resume goes through.
func TestResume_UnconvergedFleetLeavesThePhasePaused(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false
	exp := mkExp(t)
	if err := testExpRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatal(err)
	}
	svc := id.New("svc-resume")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	if err := o.PausePhase(ctx, ph.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	inst := registerSDK(t, svc, "http://late.invalid") // alive, not acking yet

	g0 := generation(t, o, svc)
	err := o.StartPhase(ctx, ph.ID)
	if err == nil || !strings.Contains(err.Error(), "converge gate") || !strings.Contains(err.Error(), inst) {
		t.Fatalf("resume err = %v, want a converge-gate failure naming %s", err, inst)
	}
	if got := getPhase(t, ph.ID); got.Status != "paused" || got.FailureReason != "" {
		t.Fatalf("phase = %s / %q after an unconverged resume, want paused with no failure reason", got.Status, got.FailureReason)
	}
	// The resume bumped; the return to paused took the phase out of the
	// document again, and bumped once more to say so.
	if g := generation(t, o, svc); g != g0+2 {
		t.Fatalf("generation %d → %d, want +2 (the resume, then back to paused)", g0, g)
	}

	err = o.ResumeExperiment(ctx, exp.ID)
	var state *ErrInvalidState
	if err == nil || errors.As(err, &state) || !strings.Contains(err.Error(), "converge gate") {
		t.Fatalf("ResumeExperiment err = %v, want the phase's converge-gate failure, not an invalid-state refusal", err)
	}
	if got := getPhase(t, ph.ID).Status; got != "paused" {
		t.Fatalf("phase status = %q, want still paused", got)
	}

	ackGenerations(t, o, inst, svc, "")
	o.WithConvergeTimeout(5 * time.Second) // returns once the instance acks
	if err := o.ResumeExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("ResumeExperiment with a converging fleet: %v", err)
	}
	if got := getPhase(t, ph.ID).Status; got != "running" {
		t.Fatalf("phase status = %q, want running", got)
	}
}

// The verdict's snapshots are pulled before the terminal transition, while the
// phase still runs: from the CAS on, the phase is out of its frozen services'
// documents, and an instance whose held poll re-materializes in that window
// (its wait elapsing) would drop the replay set before the pull. Only the CAS
// winner records the verdict.
func TestFinishPhase_VerdictPulledWhileThePhaseRuns(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	mkCompletedBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)

	sdk := newFakeSDK(t)
	sdk.fidelity = cleanSnapshot()
	statusAtPull := make(chan string, 1)
	sdk.onFidelity = func() {
		p, err := testExpRepo.GetPhase(context.Background(), iso.ID)
		if err == nil {
			select {
			case statusAtPull <- p.Status:
			default:
			}
		}
	}
	registerSDK(t, "frontend", sdk.server.URL)

	if !o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("finishPhase returned false")
	}
	select {
	case got := <-statusAtPull:
		if got != "running" {
			t.Fatalf("fidelity pulled with the phase %q, want running (before the terminal transition)", got)
		}
	default:
		t.Fatal("the verdict never pulled the instance's fidelity")
	}
	if v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID); err != nil || v == nil || v.Verdict != "VALID" {
		t.Fatalf("verdict = %+v, %v", v, err)
	}

	// A finish that cannot win (the phase is already completed) pulls nothing
	// and records nothing.
	if o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("second finishPhase returned true")
	}
	select {
	case got := <-statusAtPull:
		t.Fatalf("a losing finish pulled fidelity (phase %q)", got)
	default:
	}
}

// receive waits, bounded, for the result of an entry running on another
// goroutine.
func receive(t *testing.T, what string, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never returned", what)
		return nil
	}
}

// The pause→resume race: the phase-level pause and resume take no experiment
// lock, so while a first entry waits for its fleet an operator can pause the
// phase and resume it, starting a second entry. The first entry's claim ended
// at the pause: when the fleet converges only the second entry, which holds
// the phase's current claim, starts load, and the first one leaves the phase
// alone (no second run, no failure, no return to paused).
func TestEnterPhase_PauseAndResumeDuringTheWait_OnlyTheNewEntryStartsLoad(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	o.WithConvergeTimeout(5 * time.Second)
	exp := mkExp(t)
	svc := id.New("svc-reclaim")
	ph := mkPendingRulePhase(t, exp.ID, svc, 0)
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1}) // a run drives it
	t.Cleanup(func() { o.cancelPoller(ph.ID) })
	inst := registerSDK(t, svc, "http://acks-late.invalid")
	g0 := generation(t, o, svc)

	first := make(chan error, 1)
	go func() { first <- o.StartPhase(ctx, ph.ID) }()
	if !waitGeneration(o, svc, g0) {
		t.Fatal("the first entry never bumped")
	}
	if err := o.PausePhase(ctx, ph.ID); err != nil {
		t.Fatalf("pause during the wait: %v", err)
	}
	g1 := generation(t, o, svc)
	second := make(chan error, 1)
	go func() { second <- o.StartPhase(ctx, ph.ID) }()
	if !waitGeneration(o, svc, g1) {
		t.Fatal("the resume never bumped")
	}
	// The fleet converges on every bump so far, which satisfies both waits.
	if err := testSDKRepo.RecordPoll(ctx, inst, generation(t, o, svc)); err != nil {
		t.Fatal(err)
	}

	var state *ErrInvalidState
	if err := receive(t, "the first entry", first); !errors.As(err, &state) {
		t.Fatalf("first entry = %v, want an invalid-state refusal: the resume took the phase", err)
	}
	if err := receive(t, "the second entry", second); err != nil {
		t.Fatalf("second entry = %v, want nil", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "running" || got.FailureReason != "" {
		t.Fatalf("phase = %s / %q, want running with no failure (the second entry's)", got.Status, got.FailureReason)
	}
	if n := fz.runCount(); n != 1 {
		t.Fatalf("zeus started %d run(s), want 1: only the entry holding the phase's claim starts load", n)
	}
}

// A stop that lands while an entry waits for its fleet ends the wait at the
// barrier's next look: the phase is no longer running, so the entry gives up
// at once with a refusal naming the status, instead of sitting out the whole
// timeout before its failure turns out to be a no-op. It starts no load and
// leaves the phase as the stop left it.
func TestEnterPhase_StoppedWhileConvergingEndsTheWait(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	o.WithConvergeTimeout(5 * time.Second)
	exp := mkExp(t)
	svc := id.New("svc-stopped")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1}) // a run would drive it
	inst := registerSDK(t, svc, "http://acks-late.invalid")
	g0 := generation(t, o, svc)

	entry := make(chan error, 1)
	go func() { entry <- o.enterPhase(ctx, ph, true) }()
	if !waitGeneration(o, svc, g0) {
		t.Fatal("the entry never bumped")
	}
	stopped := make(chan error, 1)
	stopAt := time.Now()
	go func() { stopped <- o.StopPhase(ctx, ph.ID, "skipped") }()

	var err error
	select {
	case err = <-entry:
	case <-time.After(4 * time.Second):
		t.Fatal("the entry sat out its wait after the phase was stopped")
	}
	if elapsed := time.Since(stopAt); elapsed > 2*time.Second {
		t.Errorf("the entry gave up %v after the stop, want within a few of the barrier's looks", elapsed)
	}
	var state *ErrInvalidState
	if !errors.As(err, &state) || state.Current != "skipped" {
		t.Fatalf("entry = %v, want an invalid-state refusal naming the skipped phase", err)
	}

	// The stop's own exit wait ends once the instance observes its bump.
	if err := testSDKRepo.RecordPoll(ctx, inst, generation(t, o, svc)); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, "the stop", stopped); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "skipped" || got.FailureReason != "" {
		t.Fatalf("phase = %s / %q, want skipped with no failure (the stop's)", got.Status, got.FailureReason)
	}
	if n := fz.runCount(); n != 0 {
		t.Fatalf("zeus started %d run(s) for a stopped phase", n)
	}
}

// A pause that lands while an entry calls zeus, after its post-gate check,
// ends the entry's claim but finds no load recorded to stop. The entry then
// stops the load it started itself and records no poller, so nothing is left
// running on the paused phase. Landing inside the run start, the pause is
// found by the run's handle write: the entry records nothing and starts no
// attack. Landing inside the attack start, after both handles were recorded,
// it is found when the entry records its poller.
func TestEnterPhase_PausedWhileStartingLoad_StopsTheLoadItStarted(t *testing.T) {
	for _, tc := range []struct {
		name               string
		hook               func(fz *fakeZeus, fn func())
		wantRuns, wantAtks int
	}{
		{"during its run start", (*fakeZeus).onRunStart, 1, 0},
		{"during its attack start", (*fakeZeus).onAttackStart, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fz := newFakeZeus(t)
			o := newOrch(t, fz.srv.URL)
			_, phases := mkExperiment(t, 1)
			ph := phases[0]
			attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 30, TargetURL: "http://target.invalid/"}) // a run and an attack
			t.Cleanup(func() { o.cancelPoller(ph.ID) })
			paused := make(chan error, 1)
			var once sync.Once
			tc.hook(fz, func() { once.Do(func() { paused <- o.PausePhase(ctx, ph.ID) }) })

			err := o.StartPhase(ctx, ph.ID)
			var state *ErrInvalidState
			if !errors.As(err, &state) || state.Current != "paused" {
				t.Fatalf("StartPhase = %v, want an invalid-state refusal naming the paused phase", err)
			}
			if err := receive(t, "the pause", paused); err != nil {
				t.Fatalf("pause during the entry's zeus calls: %v", err)
			}
			runs, attacks := fz.driverStatuses()
			if len(runs) != tc.wantRuns || len(attacks) != tc.wantAtks {
				t.Fatalf("zeus knows runs %v and attacks %v, want %d run(s) and %d attack(s) started", runs, attacks, tc.wantRuns, tc.wantAtks)
			}
			for kind, drivers := range map[string]map[string]string{"run": runs, "attack": attacks} {
				for id, st := range drivers {
					if st != "stopped" {
						t.Errorf("%s %s is %s, want stopped: the entry left its load running on a paused phase", kind, id, st)
					}
				}
			}
			if pollerRunning(o, ph.ID) {
				t.Error("a poller survives on the paused phase")
			}
			if got := getPhase(t, ph.ID); got.Status != "paused" || got.FailureReason != "" {
				t.Errorf("phase = %s / %q, want paused with no failure (the pause's)", got.Status, got.FailureReason)
			}
		})
	}
}

// A pause and a resume that land while an entry is inside its run start hand
// the phase to a second entry, which starts its own run and attack, records
// their handles on the phase's workflow row and records its poller, all
// before the first entry's run start returns. The first entry's claim has
// ended, so it writes no handle over the second entry's: it starts nothing
// further and stops the run it started. The row names the second entry's run
// and attack, both still running, and the second entry's poller supervises
// them. (Written over, the row would name the first entry's stopped drivers,
// and the second entry's poller would complete the phase while its load ran
// on, recorded nowhere.)
func TestEnterPhase_PausedAndResumedWhileStartingLoad_TheResumeKeepsItsHandles(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	_, phases := mkExperiment(t, 1)
	ph := phases[0]
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 30, TargetURL: "http://target.invalid/"}) // a run and an attack
	t.Cleanup(func() { o.cancelPoller(ph.ID) })

	type resumeResult struct {
		paused, resumed error
		runID, attackID string // the drivers zeus knows once the resume returned: the resume's own
	}
	resumeDone := make(chan resumeResult, 1)
	var fired atomic.Bool
	fz.onRunStart(func() {
		if !fired.CompareAndSwap(false, true) {
			return // the resume's own run start
		}
		var r resumeResult
		r.paused = o.PausePhase(ctx, ph.ID)
		r.resumed = o.StartPhase(ctx, ph.ID)
		runs, attacks := fz.driverStatuses()
		for id := range runs {
			r.runID = id
		}
		for id := range attacks {
			r.attackID = id
		}
		resumeDone <- r
	})

	err := o.StartPhase(ctx, ph.ID)
	var resume resumeResult
	select {
	case resume = <-resumeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the run-start hook never paused and resumed the phase")
	}
	if resume.paused != nil || resume.resumed != nil {
		t.Fatalf("pause = %v, resume = %v during the first entry's run start, want both nil", resume.paused, resume.resumed)
	}
	var state *ErrInvalidState
	if !errors.As(err, &state) || state.Current != "running" {
		t.Errorf("first entry = %v, want an invalid-state refusal naming the running phase (the resume holds it)", err)
	}
	pws, lerr := testExpRepo.ListPhaseWorkflows(ctx, ph.ID)
	if lerr != nil || len(pws) != 1 {
		t.Fatalf("phase workflows = %+v, %v; want one row", pws, lerr)
	}
	if pws[0].ZeusRunID != resume.runID || pws[0].ZeusAttackID != resume.attackID {
		t.Errorf("row names run %q and attack %q, want the resume's run %q and attack %q",
			pws[0].ZeusRunID, pws[0].ZeusAttackID, resume.runID, resume.attackID)
	}
	runs, attacks := fz.driverStatuses()
	if len(runs) != 2 || len(attacks) != 1 {
		t.Errorf("zeus knows runs %v and attacks %v, want two runs and one attack: the first entry starts nothing after its claim ended", runs, attacks)
	}
	for id, st := range runs {
		if want := map[bool]string{true: "running", false: "stopped"}[id == resume.runID]; st != want {
			t.Errorf("run %s is %s, want %s", id, st, want)
		}
	}
	if st := attacks[resume.attackID]; st != "running" {
		t.Errorf("the resume's attack %s is %q, want running", resume.attackID, st)
	}
	if !pollerRunning(o, ph.ID) {
		t.Error("no poller supervises the resume's load")
	}
	if got := getPhase(t, ph.ID); got.Status != "running" || got.FailureReason != "" {
		t.Errorf("phase = %s / %q, want running with no failure (the resume's)", got.Status, got.FailureReason)
	}
}

// pollerRunning reports whether the orchestrator holds a poller for phaseID.
func pollerRunning(o *Orchestrator, phaseID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.running[phaseID]
	return ok
}

// A zero poll interval must not turn the barrier into a busy loop: the option
// falls back to the default, and so does the loop itself for an orchestrator
// whose field was never set.
func TestWaitConverged_ZeroPollIntervalDoesNotSpin(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithConvergePollInterval(0)
	if o.convergePollInterval != defaultConvergePollInterval {
		t.Fatalf("WithConvergePollInterval(0) set %v, want the default %v", o.convergePollInterval, defaultConvergePollInterval)
	}
	svc := id.New("svc-spin")
	target := bumpService(t, o, svc)
	registerSDK(t, svc, "http://never-acks.invalid")
	for _, interval := range []time.Duration{o.convergePollInterval, 0} {
		o.convergePollInterval = interval // 0: a field the option never set
		spy := spyOnLooks(o, svc)
		if err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 300*time.Millisecond, nil); err == nil {
			t.Fatal("waitConverged = nil, want the lagging instance's timeout")
		}
		if n := spy.looks.Load(); n > 3 {
			t.Fatalf("interval %v: the barrier looked %d times in 300ms, want a few at the %v default", interval, n, defaultConvergePollInterval)
		}
	}
}

// A lagging line carries the errors the instance last reported, which is
// usually why it lags (a rule it could not decode, a replay-set pull that
// keeps failing).
func TestWaitConverged_LaggingLinesCarryTheInstanceErrors(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	svc := id.New("svc-errs")
	const art = "sha256:cccc"
	target := bumpService(t, o, svc)
	behind := registerSDK(t, svc, "http://behind.invalid")
	if err := testSDKRepo.RecordStatus(ctx, behind, model.ObservedStatus{AppliedGeneration: target - 1,
		Errors: []string{"rule r1: decode: unknown fault_type"}}); err != nil {
		t.Fatal(err)
	}
	fetching := registerSDK(t, svc, "http://fetching.invalid")
	if err := testSDKRepo.RecordStatus(ctx, fetching, model.ObservedStatus{AppliedGeneration: target,
		Errors: []string{"replay set: chunk 3: 503"}, ReplaySet: &model.ReplaySetStatus{ID: art, State: "fetching"}}); err != nil {
		t.Fatal(err)
	}
	err := o.waitConverged(ctx, map[string]uint64{svc: target}, map[string]string{svc: art}, 200*time.Millisecond, nil)
	if err == nil {
		t.Fatal("waitConverged = nil, want both instances lagging")
	}
	for _, want := range []string{
		behind + "(" + svc + "): observed=", "rule r1: decode: unknown fault_type",
		fetching + "(" + svc + "): replay_set=" + art + "/fetching", "replay set: chunk 3: 503",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
}

// The non-fatal waits say which barrier they belong to: a recording phase's
// completion waits after its drain bump and again after its exit bump, and a
// straggler is logged for each.
func TestAwaitConvergedNonFatal_NamesTheBarrier(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	logs := &logBuffer{}
	o.logger = slog.New(slog.NewTextHandler(logs, nil))
	exp := mkExp(t)
	svc := id.New("svc-barrier")
	base := mkRunningBaseline(t, exp.ID, 0)
	mkRunningIsolation(t, exp.ID, svc, 1) // the baseline records svc
	registerSDK(t, svc, "http://never-acks.invalid")

	if !o.finishPhase(ctx, base.ID, "completed", "running") {
		t.Fatal("finishPhase returned false")
	}
	for _, want := range []string{"barrier=drain", "barrier=exit"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs = %q, want a non-fatal timeout logged with %s", logs.String(), want)
		}
	}
}
