package dbinitiator

import (
	"context"

	"github.com/cccteam/db-initiator/internal/runner"
	"github.com/go-playground/errors/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDatabase represents a postgres database created and ready for migrations
type PostgresDatabase struct {
	*pgxpool.Pool
	dbName  string
	schema  string
	connStr string
}

// NewPostgresDatabase creates a new database and schema, then connects to it.
func NewPostgresDatabase(ctx context.Context, username, password, host, port, databaseToCreate, schemaToCreate string, sslMode SSLMode) (*PostgresDatabase, error) {
	// a. Construct connection string for a default database (e.g., "postgres")
	defaultDBConnStr := PostgresConnStr(username, password, host, port, "postgres", sslMode)

	// b. Open a temporary admin connection to this default database
	adminPool, err := openDB(ctx, defaultDBConnStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to default database 'postgres' as user %s", username)
	}
	defer adminPool.Close()

	// c. Using adminPool, execute CREATE DATABASE
	createDBSQL := "CREATE DATABASE " + pgx.Identifier{databaseToCreate}.Sanitize() + " WITH OWNER " + pgx.Identifier{username}.Sanitize()
	if _, err := adminPool.Exec(ctx, createDBSQL); err != nil {
		return nil, errors.Wrapf(err, "failed to execute CREATE DATABASE %s WITH OWNER %s", databaseToCreate, username)
	}

	// e. Construct the connection string for the newly created database
	targetDBConnStr := PostgresConnStr(username, password, host, port, databaseToCreate, sslMode)

	// f. Open the main connection pool to this target database
	mainPool, err := openDB(ctx, targetDBConnStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to newly created database %s as user %s", databaseToCreate, username)
	}

	// g. Using mainPool, execute CREATE SCHEMA
	createSchemaSQL := "CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{schemaToCreate}.Sanitize()
	if _, err := mainPool.Exec(ctx, createSchemaSQL); err != nil {
		mainPool.Close()

		return nil, errors.Wrapf(err, "failed to create schema %s in database %s", schemaToCreate, databaseToCreate)
	}

	return &PostgresDatabase{
		Pool:    mainPool,
		dbName:  databaseToCreate,
		schema:  schemaToCreate,
		connStr: targetDBConnStr,
	}, nil
}

// Schema returns the default schema
func (db *PostgresDatabase) Schema() string {
	return db.schema
}

// MigrateUp applies every up migration of every sourceURL, in the order given. Each source
// is applied from its own first version: the migrations table is reset before each one, so
// a test can layer an application's schema and then its fixtures, each numbered from 1.
func (db *PostgresDatabase) MigrateUp(sourceURL ...string) error {
	ctx := context.Background()
	r := db.runner()

	for _, source := range sourceURL {
		src, err := runner.Open(source)
		if err != nil {
			return errors.Wrap(err, "runner.Open()")
		}

		if err := r.Force(ctx, -1); err != nil {
			return errors.Wrapf(err, "runner.Postgres.Force(): %s", source)
		}

		if err := r.Up(ctx, src); err != nil {
			return errors.Wrapf(err, "runner.Postgres.Up(): %s", source)
		}
	}

	return nil
}

// MigrateDown reverts every version of the sourceURL, from the database's current version
// down to no version.
func (db *PostgresDatabase) MigrateDown(sourceURL string) error {
	src, err := runner.Open(sourceURL)
	if err != nil {
		return errors.Wrap(err, "runner.Open()")
	}

	if err := db.runner().Down(context.Background(), src); err != nil {
		return errors.Wrapf(err, "runner.Postgres.Down(): %s", sourceURL)
	}

	return nil
}

// runner returns the migration runner on the migrations table.
func (db *PostgresDatabase) runner() *runner.Postgres {
	return runner.NewPostgres(db.Pool, postgresMigrationsTable)
}

// Close closes the database connection
func (db *PostgresDatabase) Close() {
	db.Pool.Close()
}
