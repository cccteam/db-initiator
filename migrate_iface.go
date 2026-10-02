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

	// SchemaVersion reads the schema migrations table.
	SchemaVersion(ctx context.Context) (Version, error)

	// DataVersion reads the data migrations table.
	DataVersion(ctx context.Context) (Version, error)

	// ForceSchema sets the schema migrations table to a version, clean; -1 means no version.
	ForceSchema(ctx context.Context, version int) error

	// ForceData sets the data migrations table to a version, clean; -1 means no version.
	ForceData(ctx context.Context, version int) error
}
