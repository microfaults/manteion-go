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

// The HTTP server's write deadline must outlast what a stop can wait out
// before it answers: the drain barrier's convergence wait and report gate,
// then the exit bump's convergence wait. Otherwise a stop that succeeded has
// its 202 cut off.
func TestHTTPWriteTimeoutOutlastsAStop(t *testing.T) {
	for _, tc := range []struct{ converge, drain time.Duration }{
		{30 * time.Second, 30 * time.Second},
		{2 * time.Minute, 30 * time.Second},
		{10 * time.Second, 5 * time.Minute},
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
