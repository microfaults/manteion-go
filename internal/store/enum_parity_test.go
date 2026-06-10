package store

import (
	"slices"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/testutil"
)

// TestEnumParity is the drift-killer for the mixed enum strategy: every
// native Postgres enum type in the epoch-2 schema must carry exactly the
// labels (in order) that model.EnumValues declares, and vice versa. A label
// added on one side without the other fails here before it can ship.
func TestEnumParity(t *testing.T) {
	conn := testutil.TestDB(t)

	for typname, want := range model.EnumValues {
		rows, err := conn.Query(`
			SELECT e.enumlabel
			FROM pg_enum e
			JOIN pg_type t ON t.oid = e.enumtypid
			WHERE t.typname = $1
			ORDER BY e.enumsortorder`, typname)
		if err != nil {
			t.Fatalf("query pg_enum for %s: %v", typname, err)
		}
		var got []string
		for rows.Next() {
			var label string
			if err := rows.Scan(&label); err != nil {
				t.Fatalf("scan label: %v", err)
			}
			got = append(got, label)
		}
		rows.Close()

		if len(got) == 0 {
			t.Errorf("enum type %s missing from database (schema/model drift)", typname)
			continue
		}
		if !slices.Equal(got, want) {
			t.Errorf("enum %s labels drifted:\n  db   = %v\n  go   = %v", typname, got, want)
		}
	}

	// Reverse direction: no enum type in the DB that the model doesn't know.
	rows, err := conn.Query(`
		SELECT DISTINCT t.typname
		FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid`)
	if err != nil {
		t.Fatalf("list enum types: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var typname string
		if err := rows.Scan(&typname); err != nil {
			t.Fatalf("scan typname: %v", err)
		}
		if _, ok := model.EnumValues[typname]; !ok {
			t.Errorf("database enum type %s has no model.EnumValues entry", typname)
		}
	}
}
