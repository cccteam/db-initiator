package dbinitiator

import (
	"context"

	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
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
func NewPostgresDatabase(ctx context.Context, username, password, host, port, databaseToCreate, schemaToCreate string) (*PostgresDatabase, error) {
	// a. Construct connection string for a default database (e.g., "postgres")
	defaultDBConnStr := PostgresConnStr(username, password, host, port, "postgres")

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
	targetDBConnStr := PostgresConnStr(username, password, host, port, databaseToCreate)

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

// MigrateUp will migrate all the way up, applying all up migrations from all sourceURL's
func (db *PostgresDatabase) MigrateUp(sourceURL ...string) error {
	for _, source := range sourceURL {
		m, err := migrate.New(source, db.connStr)
		if err != nil {
			return errors.Wrapf(err, "migrate.New(): fileURL=%s and connectionURL=%s", source, db.connStr)
		}

		if _, _, err := m.Version(); err == nil {
			if err := m.Force(-1); err != nil {
				return errors.Wrapf(err, "migrate.Migrate.Force(): %s", source)
			}
		}

		if err := m.Up(); err != nil {
			return errors.Wrapf(err, "migrate.Migrate.Up(): %s", source)
		}

		if err, dbErr := m.Close(); err != nil {
			return errors.Wrapf(err, "migrate.Migrate.Close(): source error: %s", source)
		} else if dbErr != nil {
			return errors.Wrapf(dbErr, "migrate.Migrate.Close(): database error: %s", source)
		}
	}

	return nil
}

// MigrateDown will migrate all the way down
func (db *PostgresDatabase) MigrateDown(sourceURL string) error {
	m, err := migrate.New(sourceURL, db.connStr)
	if err != nil {
		return errors.Wrapf(err, "failed to create new migrate with fileURL=%s and connectionURL=%s", sourceURL, db.connStr)
	}

	if err := m.Down(); err != nil {
		return errors.Wrap(err, "migrate.Migrate.Down()")
	}

	if err, dbErr := m.Close(); err != nil {
		return errors.Wrap(err, "migrate.Migrate.Close(): source error")
	} else if dbErr != nil {
		return errors.Wrap(dbErr, "migrate.Migrate.Close(): database error")
	}

	return nil
}

// Close closes the database connection
func (db *PostgresDatabase) Close() {
	db.Pool.Close()
}
