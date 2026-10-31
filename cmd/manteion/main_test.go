package main

import (
	"strings"
	"testing"
	"time"
)

// MANTEION_CONVERGE_TIMEOUT bounds the convergence barrier; zero or a
// negative value would fail every phase entry at its first look, so startup
// refuses it, and refuses a value that is not a duration rather than silently
// running on the default.
func TestPositiveDurationEnv(t *testing.T) {
	const key = "MANTEION_TEST_POSITIVE_DURATION"
	for _, tc := range []struct {
		val     string
		want    time.Duration
		wantErr string
	}{
		{val: "", want: 30 * time.Second}, // unset: the fallback
		{val: "45s", want: 45 * time.Second},
		{val: "2m", want: 2 * time.Minute},
		{val: "0s", wantErr: "must be positive"},
		{val: "-5s", wantErr: "must be positive"},
		{val: "soon", wantErr: "not a duration"},
	} {
		t.Setenv(key, tc.val)
		got, err := positiveDurationEnv(key, 30*time.Second)
		switch {
		case tc.wantErr != "":
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), key) {
				t.Errorf("%q: err = %v, want one naming %s and saying %q", tc.val, err, key, tc.wantErr)
			}
		case err != nil || got != tc.want:
			t.Errorf("%q: = %v, %v; want %v", tc.val, got, err, tc.want)
		}
	}
}

// Both barrier bounds are read through positiveDurationEnv, capped at
// maxBarrierTimeout. The drain bound sizes the HTTP write timeout next to the
// converge bound, and a negative one would shrink that deadline (far enough
// below zero, to none at all); zero would end every drain before an instance
// could report; and a huge value of either would overflow the deadline to a
// negative duration. Startup refuses all of them, naming the key.
func TestBarrierTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		converge, drain         string
		wantConverge, wantDrain time.Duration
		wantErr                 string
	}{
		{name: "unset: the defaults", wantConverge: 30 * time.Second, wantDrain: 30 * time.Second},
		{name: "both set", converge: "45s", drain: "2m", wantConverge: 45 * time.Second, wantDrain: 2 * time.Minute},
		{name: "converge zero", converge: "0s", wantErr: "MANTEION_CONVERGE_TIMEOUT"},
		{name: "drain negative", drain: "-90s", wantErr: "MANTEION_DRAIN_TIMEOUT=\"-90s\" must be positive"},
		{name: "drain zero", drain: "0s", wantErr: "MANTEION_DRAIN_TIMEOUT=\"0s\" must be positive"},
		{name: "drain not a duration", drain: "soon", wantErr: "MANTEION_DRAIN_TIMEOUT=\"soon\" is not a duration"},
		{name: "both at the bound", converge: "1h", drain: "60m", wantConverge: time.Hour, wantDrain: time.Hour},
		{name: "converge above the bound", converge: "61m", wantErr: "MANTEION_CONVERGE_TIMEOUT=\"61m\" must be at most 1h0m0s"},
		{name: "drain near the largest duration", drain: "2562047h", wantErr: "MANTEION_DRAIN_TIMEOUT=\"2562047h\" must be at most 1h0m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MANTEION_CONVERGE_TIMEOUT", tc.converge)
			t.Setenv("MANTEION_DRAIN_TIMEOUT", tc.drain)
			converge, drain, err := barrierTimeouts()
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("err = %v, want one containing %q", err, tc.wantErr)
				}
			case err != nil || converge != tc.wantConverge || drain != tc.wantDrain:
				t.Errorf("= %v, %v, %v; want %v, %v", converge, drain, err, tc.wantConverge, tc.wantDrain)
			}
		})
	}
}

// The HTTP server's write deadline must outlast what a stop can wait out
// before it answers: the drain barrier's convergence wait and report gate,
// then the exit bump's convergence wait. Otherwise a stop that succeeded has
// its 202 cut off.
func TestHTTPWriteTimeoutOutlastsAStop(t *testing.T) {
	for _, tc := range []struct{ converge, drain time.Duration }{
		{30 * time.Second, 30 * time.Second},
		{2 * time.Minute, 30 * time.Second},
		{10 * time.Second, 5 * time.Minute},
		{maxBarrierTimeout, maxBarrierTimeout}, // the largest values startup accepts
	} {
		got := httpWriteTimeout(tc.converge, tc.drain)
		if stop := tc.converge + tc.drain + tc.converge; got <= stop {
			t.Errorf("httpWriteTimeout(%v, %v) = %v, want more than a stop's %v of waits", tc.converge, tc.drain, got, stop)
		}
		if floor := 2*tc.converge + 30*time.Second; got < floor {
			t.Errorf("httpWriteTimeout(%v, %v) = %v, want at least 2 × the converge timeout + 30s (%v)", tc.converge, tc.drain, got, floor)
		}
	}
}
