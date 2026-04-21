// Package db provides PostgreSQL connection management and schema migrations
// for manteion-go. It uses pgx/v5 via the database/sql interface.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Open connects to PostgreSQL at the given DSN, configures the connection pool,
// and runs any pending schema migrations.
//
// DSN format: postgres://user:password@host:port/dbname?sslmode=disable
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}

	// Connection pool settings.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Verify the connection is live.
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}

	// Run schema migrations.
	if err := Migrate(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("db: migrate: %w", err)
	}

	slog.Info("database connected and migrated")
	return db, nil
}

// Close drains the connection pool and closes the database.
func Close(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}
