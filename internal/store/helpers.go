// Package store provides PostgreSQL-backed repositories for all manteion
// domain types. Each repository operates on *sql.DB and converts between
// SQL rows and model.* types.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned when a Get/Delete finds no matching row.
var ErrNotFound = errors.New("store: not found")

// affectedOrNotFound returns ErrNotFound when res affected zero rows,
// or wraps any RowsAffected error. The canonical post-Exec check for
// UPDATE/DELETE by primary key. pgx never returns a negative count
// (the int64 return is just sql/driver-interface compliance), so the
// only failure shapes are (a) the driver itself errored, or (b) the
// row didn't exist.
func affectedOrNotFound(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// execTx runs fn inside a database transaction. It commits on success
// and rolls back on error or panic.
func execTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// jsonbMarshal converts a Go value to []byte for JSONB insertion.
// Returns []byte("null") for nil values.
func jsonbMarshal(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	return json.Marshal(v)
}

// jsonbScan unmarshals JSONB bytes into a Go value.
// No-ops on nil data.
func jsonbScan(data []byte, v any) error {
	if data == nil {
		return nil
	}
	return json.Unmarshal(data, v)
}

// nullString converts a string to sql.NullString (empty → NULL).
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// fromNullString converts sql.NullString back to string.
func fromNullString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// nullInt converts an int to sql.NullInt32 (0 → NULL).
func nullInt(n int) sql.NullInt32 {
	if n == 0 {
		return sql.NullInt32{}
	}
	return sql.NullInt32{Int32: int32(n), Valid: true}
}

// nullFloat returns nil for zero (so the driver writes NULL), or the value.
// Used for optional float columns like phase_workflows.rate_rps.
func nullFloat(v float64) any {
	if v == 0 {
		return nil
	}
	return v
}

// nullTimeToPtr converts sql.NullTime into *time.Time for model fields
// that are optional timestamps.
func nullTimeToPtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	tt := t.Time
	return &tt
}
