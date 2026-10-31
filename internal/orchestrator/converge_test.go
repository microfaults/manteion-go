package orchestrator

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
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

// The barrier compares observed_generation with >=: an instance that already
// adopted a later generation has converged too (it can also lag one poll,
// which is why the comparison is never ==).
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
	if err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 5*time.Second); err != nil {
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

	if err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 5*time.Second); err != nil {
		t.Fatalf("waitConverged = %v, want nil: a suspect instance is not waited on", err)
	}
	if got := logs.String(); !strings.Contains(got, suspect) || !strings.Contains(got, "not waited on") {
		t.Errorf("logs = %q, want the suspect instance %s named as not waited on", got, suspect)
	}

	// An alive instance that never polls the target is waited on until the
	// timeout, and the timeout error names it and the suspect one.
	lagging := registerSDK(t, svc, "http://lagging.invalid")
	err := o.waitConverged(ctx, map[string]uint64{svc: target}, nil, 300*time.Millisecond)
	if err == nil {
		t.Fatal("waitConverged = nil, want a timeout naming the lagging instance")
	}
	for _, want := range []string{lagging + "(" + svc + "): observed=0", "suspect", suspect} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error = %q, want it to contain %q", err, want)
		}
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
			err := o.waitConverged(ctx, targets, map[string]string{svc: art}, timeout)
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
		err := o.waitConverged(ctx, targets, artifacts, 200*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "still lagging") || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("replay set %q/%q: err = %v, want a lagging timeout with %q", tc.id, tc.state, err, tc.want)
		}
	}
	report(art, "committed")
	if err := o.waitConverged(ctx, targets, artifacts, 5*time.Second); err != nil {
		t.Fatalf("committed: waitConverged = %v, want nil", err)
	}
}
