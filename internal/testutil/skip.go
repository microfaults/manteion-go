// Package testutil provides shared helpers for integration and end-to-end tests.
package testutil

import (
	"os"
	"testing"
)

// RequireIntegration skips the test unless MANTEION_INTEGRATION=1.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("MANTEION_INTEGRATION") != "1" {
		t.Skip("set MANTEION_INTEGRATION=1 to run integration tests")
	}
}

// RequireE2E skips the test unless MANTEION_E2E=1.
func RequireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("MANTEION_E2E") != "1" {
		t.Skip("set MANTEION_E2E=1 to run end-to-end tests")
	}
}
