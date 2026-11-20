package demo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// The log lines that mark a run's rule: "demo: rule switched on" and "demo:
// rule switched off" (with its reason) at Info, once per write that changed
// the rule; a failed switch-off at Warn, the first and then at most one a
// minute; a switch-off that found the rule already off or gone at Debug.

// ruleRecords returns the records of c whose message is msg and whose rule_id
// is ruleID.
func ruleRecords(c *captureHandler, msg, ruleID string) []slog.Record {
	var out []slog.Record
	for _, r := range c.all() {
		if v, ok := attr(r, "rule_id"); ok && r.Message == msg && v.String() == ruleID {
			out = append(out, r)
		}
	}
	return out
}

// requireOneRecord checks that recs is one record at level whose attributes
// include want, and returns it.
func requireOneRecord(t *testing.T, what string, recs []slog.Record, level slog.Level, want map[string]string) {
	t.Helper()
	if len(recs) != 1 {
		t.Errorf("%s: %d lines; want 1", what, len(recs))
		return
	}
	r := recs[0]
	if r.Level != level {
		t.Errorf("%s: logged at %s; want %s", what, r.Level, level)
	}
	for k, v := range want {
		if got, ok := attr(r, k); !ok || got.String() != v {
			t.Errorf("%s: %s = %q (present %v); want %q", what, k, got.String(), ok, v)
		}
	}
}

// capturedHarness is a worker harness whose log is captured, every level.
func capturedHarness(t *testing.T, tweak func(*Config)) (*harness, *captureHandler) {
	x := newHarness(t, tweak)
	c := &captureHandler{}
	x.rebuild(func(d *Deps) { d.Logger = slog.New(c) })
	return x, c
}

// TestRuleLog_CompletedRun: a run that completes logs its rule switched on
// once and switched off once, at Info, with the run and rule ids, the off with
// reason completed; nothing about the switch at Warn.
func TestRuleLog_CompletedRun(t *testing.T) {
	t.Parallel()
	x, c := capturedHarness(t, noCap)
	x.f.autopilot = true
	r, ch := x.enqueue("a")
	x.start()
	awaitEnd(t, ch)
	x.stop()
	rule := x.f.ruleIDOfRun(r.RunID)
	ids := map[string]string{"run_id": r.RunID, "rule_id": rule}
	requireOneRecord(t, "switched on", ruleRecords(c, "demo: rule switched on", rule), slog.LevelInfo, ids)
	ids["reason"] = offCompleted
	requireOneRecord(t, "switched off", ruleRecords(c, "demo: rule switched off", rule), slog.LevelInfo, ids)
	for _, rec := range c.all() {
		if rec.Level >= slog.LevelWarn {
			t.Errorf("a line at %s: %s", rec.Level, rec.Message)
		}
	}
}

// TestRuleLog_ShutdownMidRun: a shutdown while the run's rule is on logs the
// switch-off once at Info with reason shutdown. Shutdown's own switch-off
// races the ending's: both write, and the second is a Debug line, not a
// second Info "off".
func TestRuleLog_ShutdownMidRun(t *testing.T) {
	t.Parallel()
	x, c := capturedHarness(t, noCap)
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	rule := x.f.ruleIDOfRun(r.RunID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := x.w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitClosed(t, x.w.done, "Run to return")
	x.stop()
	if n := len(x.f.callList("rules.SetEnabled " + rule + " false")); n != 2 {
		t.Fatalf("%d switch-off writes; want 2, Shutdown's and the ending's", n)
	}
	requireOneRecord(t, "switched off", ruleRecords(c, "demo: rule switched off", rule), slog.LevelInfo,
		map[string]string{"run_id": r.RunID, "rule_id": rule, "reason": offShutdown})
	requireOneRecord(t, "the second switch-off", ruleRecords(c, "demo: rule already off", rule), slog.LevelDebug,
		map[string]string{"run_id": r.RunID, "rule_id": rule})
}

// TestRuleLog_FailingSwitchOff: a switch-off that fails five times logs one
// Warn line, the first failure's, while the clock stands still, then its
// success once at Info with the attempt that landed. When a minute passes
// between failures, one more Warn line follows, and no more.
func TestRuleLog_FailingSwitchOff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		minuteAt int   // the failed attempt before which a minute passes; 0 for none
		warnedAt []int // the attempts logged at Warn
	}{
		{"clock still", 0, []int{1}},
		{"a minute passes", 4, []int{1, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, c := capturedHarness(t, noCap)
			x.f.autopilot = true
			for i := 1; i <= 5; i++ {
				x.f.fail(fmt.Sprintf("rules.SetEnabled#%d", i), errLeaky)
			}
			if tc.minuteAt > 0 {
				x.f.onCall(fmt.Sprintf("rules.SetEnabled#%d", tc.minuteAt), func() { x.f.clock.Advance(offWarnEvery) })
			}
			r, ch := x.enqueue("a")
			x.start()
			awaitEnd(t, ch)
			rule := x.f.ruleIDOfRun(r.RunID)
			waitFor(t, "the switch-off to land", func() bool { return len(x.f.callList("rules.SetEnabled "+rule+" false")) == 6 })
			x.stop()

			var warned []int
			for _, rec := range c.all() {
				if rec.Level != slog.LevelWarn {
					continue
				}
				if v, ok := attr(rec, "rule_id"); !ok || v.String() != rule {
					t.Errorf("a Warn line without the rule's id: %s", rec.Message)
					continue
				}
				a, ok := attr(rec, "attempt")
				if !ok { // the worker holding the next run says so once
					if !strings.Contains(rec.Message, "the next run waits") {
						t.Errorf("Warn line %q without an attempt", rec.Message)
					}
					continue
				}
				warned = append(warned, int(a.Int64()))
				if !strings.Contains(rec.Message, "switching the run's rule off failed") {
					t.Errorf("Warn line %q", rec.Message)
				}
			}
			if fmt.Sprint(warned) != fmt.Sprint(tc.warnedAt) {
				t.Errorf("Warn lines for attempts %v; want %v", warned, tc.warnedAt)
			}
			requireOneRecord(t, "switched off", ruleRecords(c, "demo: rule switched off", rule), slog.LevelInfo,
				map[string]string{"run_id": r.RunID, "reason": offCompleted, "attempt": "6"})
		})
	}
}

// TestRuleLog_StartUpAndRetention: the start-up sweep logs each rule it finds
// on and switches off at Info with reason start-up and the run's id;
// retention logs a rule it switches off at Info with reason retention, and
// one it found already off at Debug only.
func TestRuleLog_StartUpAndRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	c := &captureHandler{}
	d := f.deps()
	d.Logger = slog.New(c)

	left := seed(t, f, "running", true)
	if err := Sweep(ctx, d, 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	requireOneRecord(t, "start-up", ruleRecords(c, "demo: rule switched off", left.RuleID), slog.LevelInfo,
		map[string]string{"run_id": left.runID, "reason": offStartUp})

	on := seed(t, f, "completed", true)
	off := seed(t, f, "completed", false)
	seed(t, f, "completed", false)
	seed(t, f, "completed", false)
	if err := Retain(ctx, d, 2); err != nil { // left, on and off go
		t.Fatalf("Retain: %v", err)
	}
	requireOneRecord(t, "retention, rule on", ruleRecords(c, "demo: rule switched off", on.RuleID), slog.LevelInfo,
		map[string]string{"run_id": on.runID, "reason": offRetention})
	if n := len(ruleRecords(c, "demo: rule switched off", off.RuleID)); n != 0 {
		t.Errorf("retention logged a rule already off at Info (%d lines)", n)
	}
	requireOneRecord(t, "retention, rule already off", ruleRecords(c, "demo: rule already off", off.RuleID), slog.LevelDebug,
		map[string]string{"run_id": off.runID, "reason": offRetention})
}

// TestRuleSwitch_FailuresMarkedWithTheirCount: of two switch-offs failing at
// once (the watchdog's and the ending's), exactly one is attempt 1 and due the
// first Warn line, whichever of their callers logs first; the other is attempt
// 2 and not due while the clock stands still.
func TestRuleSwitch_FailuresMarkedWithTheirCount(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	s := seed(t, f, "planned", false)
	sw := newRuleSwitch(s.runID, failRuleOff{fakeRules{f}, s.RuleID}, slog.New(slog.DiscardHandler), f.clock)
	sw.attach(context.Background(), s.RuleID)
	errs := make(chan error, 2)
	for _, why := range []string{offTimedOut, offCancelled} {
		go func() { errs <- sw.off(context.Background(), why) }()
	}
	got := map[int]bool{}
	for i := 0; i < 2; i++ {
		var fail *offFailure
		if err := <-errs; !errors.As(err, &fail) || !errors.Is(err, ErrRuleState) {
			t.Fatalf("switch-off: %v; want an offFailure wrapping the rule-state failure", err)
		}
		got[fail.attempt] = fail.warn
	}
	if len(got) != 2 || !got[1] || got[2] {
		t.Errorf("attempt -> due a Warn line: %v; want 1 -> true, 2 -> false", got)
	}
}
