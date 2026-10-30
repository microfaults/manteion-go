package db_test

import (
	"testing"

	"manteion-go/internal/testutil"
)

// TestMigration12_DesiredStateColumns pins migration 12: the observed-state
// columns on sdk_instances and the per-service generation table.
func TestMigration12_DesiredStateColumns(t *testing.T) {
	conn := testutil.TestDB(t)
	for _, col := range []string{"host_kind", "capabilities", "observed_generation", "observed_etag",
		"observed_unsupported", "observed_errors", "replay_set_id", "replay_set_state", "observed_at"} {
		var n int
		if err := conn.QueryRow(`SELECT count(*) FROM information_schema.columns
			WHERE table_name = 'sdk_instances' AND column_name = $1`, col).Scan(&n); err != nil || n != 1 {
			t.Errorf("sdk_instances.%s: present=%d err=%v, want 1", col, n, err)
		}
	}
	var gen uint64
	if err := conn.QueryRow(`INSERT INTO service_generations (service) VALUES ('m12-probe') RETURNING generation`).Scan(&gen); err != nil {
		t.Fatalf("service_generations insert: %v", err)
	}
	if gen != 1 {
		t.Errorf("default generation = %d, want 1", gen)
	}
}
