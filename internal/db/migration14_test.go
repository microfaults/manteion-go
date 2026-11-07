package db_test

import (
	"testing"

	"manteion-go/internal/testutil"
)

// TestMigration14_AppliedGeneration pins migration 14: the generation an
// instance's last status report was applied at, a BIGINT that defaults to 0.
func TestMigration14_AppliedGeneration(t *testing.T) {
	conn := testutil.TestDB(t)
	var dataType, nullable, def string
	if err := conn.QueryRow(`SELECT data_type, is_nullable, coalesce(column_default, '') FROM information_schema.columns
		WHERE table_name = 'sdk_instances' AND column_name = 'applied_generation'`).Scan(&dataType, &nullable, &def); err != nil {
		t.Fatalf("sdk_instances.applied_generation: %v", err)
	}
	if dataType != "bigint" || nullable != "NO" || def != "0" {
		t.Errorf("applied_generation = %s nullable=%s default=%q, want bigint NOT NULL DEFAULT 0", dataType, nullable, def)
	}
}
