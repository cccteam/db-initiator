package dbinitiator

import (
	"context"

	"github.com/cccteam/db-initiator/internal/runner"
	"github.com/go-playground/errors/v5"
)

// postgresMigrationsTable is the one version table a PostgreSQL database keeps.
const postgresMigrationsTable = "schema_migrations"

// errPostgresDataMigrations is what the data-migration methods return on PostgreSQL, which
// keeps one version table and no data migrations.
var errPostgresDataMigrations = errors.New("data migrations are not supported on PostgreSQL")

// PostgresMigrator handles connecting to an existing PostgreSQL database and running
// migrations. Each file runs inside one transaction: a file that fails rolls back whole and
// the version stays where it was.
type PostgresMigrator struct {
	connStr string
}

var _ Migrator = (*PostgresMigrator)(nil)

// NewPostgresMigrator returns a new [PostgresMigrator].
// It does not attempt to create the database or schema.
//
// sslMode sets the sslmode query parameter.
// Pass an empty string to use the default, which is [SSLModeRequire].
// Use [SSLModeDisable] only for local test containers.
func NewPostgresMigrator(username, password, host, port, database string, sslMode SSLMode) *PostgresMigrator {
	return &PostgresMigrator{
		connStr: PostgresConnStr(username, password, host, port, database, sslMode),
	}
}

// MigrateUpSchema applies every pending up migration from the sourceURL. A database
// already at the last version is a normal result, not an error.
func (p *PostgresMigrator) MigrateUpSchema(ctx context.Context, sourceURL string) error {
	src, err := runner.Open(sourceURL)
	if err != nil {
		return errors.Wrap(err, "runner.Open()")
	}

	return p.run(ctx, func(r *runner.Postgres) error {
		if err := r.Up(ctx, src); err != nil {
			return errors.Wrapf(err, "runner.Postgres.Up(): %s", sourceURL)
		}

		return nil
	})
}

// MigrateUpData is not supported on PostgreSQL.
func (p *PostgresMigrator) MigrateUpData(_ context.Context, _ string) error {
	return errPostgresDataMigrations
}

// MigrateDropSchema is not supported on PostgreSQL.
func (p *PostgresMigrator) MigrateDropSchema(_ context.Context) error {
	return errors.New("dropping the schema is not supported on PostgreSQL")
}

// SchemaVersion reads the migrations table.
func (p *PostgresMigrator) SchemaVersion(ctx context.Context) (Version, error) {
	var v Version
	err := p.run(ctx, func(r *runner.Postgres) error {
		var err error
		if v, err = r.Version(ctx); err != nil {
			return errors.Wrap(err, "runner.Postgres.Version()")
		}

		return nil
	})
	if err != nil {
		return Version{}, err
	}

	return v, nil
}

// DataVersion is not supported on PostgreSQL.
func (p *PostgresMigrator) DataVersion(_ context.Context) (Version, error) {
	return Version{}, errPostgresDataMigrations
}

// ForceSchema sets the migrations table to a version, clean; -1 leaves the database with
// no version.
func (p *PostgresMigrator) ForceSchema(ctx context.Context, version int) error {
	return p.run(ctx, func(r *runner.Postgres) error {
		if err := r.Force(ctx, version); err != nil {
			return errors.Wrap(err, "runner.Postgres.Force()")
		}

		return nil
	})
}

// ForceData is not supported on PostgreSQL.
func (p *PostgresMigrator) ForceData(_ context.Context, _ int) error {
	return errPostgresDataMigrations
}

// run opens a connection pool for one call and closes it after.
func (p *PostgresMigrator) run(ctx context.Context, fn func(r *runner.Postgres) error) error {
	pool, err := openDB(ctx, p.connStr)
	if err != nil {
		return errors.Wrapf(err, "connecting to %s", p.connStr)
	}
	defer pool.Close()

	return fn(runner.NewPostgres(pool, postgresMigrationsTable))
}
