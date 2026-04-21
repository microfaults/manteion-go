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
)

// ErrNotFound is returned when a Get/Delete finds no matching row.
var ErrNotFound = errors.New("store: not found")

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
