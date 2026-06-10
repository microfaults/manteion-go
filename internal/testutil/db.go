package testutil

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"manteion-go/internal/db"
)

const defaultDSN = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"

// TestDB opens a real Postgres connection, runs migrations, and registers a
// cleanup function that truncates all application tables (preserving schema).
// Skips the test if MANTEION_INTEGRATION is not set.
func TestDB(t *testing.T) *sql.DB {
	t.Helper()
	RequireIntegration(t)

	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = defaultDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("testutil.TestDB: %v", err)
	}

	t.Cleanup(func() {
		truncateAll(conn)
		_ = db.Close(conn)
	})

	return conn
}

func truncateAll(conn *sql.DB) {
	// Epoch-2 live tables in FK-safe (children-first) order. rule_version
	// is the singleton counter and stays seeded.
	tables := []string{
		"trace_anchors",
		"experiment_results",
		"phase_workflow_results",
		"phase_service_latency",
		"phase_service_cache",
		"phase_rules",
		"phase_workflows",
		"attack_results",
		"attacks",
		"fault_configs",
		"experiment_phases",
		"experiments",
		"workflows",
		"policy_rules",
		"sdk_instances",
		"rules",
		"fault_composition_members",
		"fault_compositions",
		"fault_specs",
	}
	for _, tbl := range tables {
		_, _ = conn.Exec("DELETE FROM " + tbl)
	}
}
