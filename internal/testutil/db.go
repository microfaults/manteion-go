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
	tables := []string{
		"trace_anchors",
		"contribution_results",
		"service_run_results",
		"workflow_run_results",
		"attacks",
		"attack_results",
		"experiment_runs",
		"experiments",
		"policy_rules",
		"sdk_instances",
		"rules",
		"fault_compositions",
		"fault_specs",
		"workloads",
		"flows",
		"personas",
	}
	for _, tbl := range tables {
		_, _ = conn.Exec("DELETE FROM " + tbl)
	}
}
