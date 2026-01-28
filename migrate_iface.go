package dbinitiator

import "context"

// Migrator is an interface for database migration.
type Migrator interface {
	// MigrateUpSchema applies all up migrations for the database schema.
	MigrateUpSchema(ctx context.Context, sourceURL string) error

	// MigrateUpData applies all up migrations for the database data.
	MigrateUpData(ctx context.Context, sourceURL string) error

	// MigrateDropSchema drops the database schema.
	MigrateDropSchema(ctx context.Context) error
}
