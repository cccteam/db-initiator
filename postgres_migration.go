package dbinitiator

import (
	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
)

// PostgresMigrator implements the MigrationService interface for PostgreSQL.
type PostgresMigrator struct {
	connStr string
}

// ConnectToPostgres connects to an existing postgres database using structured parameters.
// It does not attempt to create the database or schema.
// It returns a [PostgresMigrator] which can be used to run migrations.
func ConnectToPostgres(username, password, host, port, database string) (*PostgresMigrator, error) {
	connStr := PostgresConnStr(username, password, host, port, database)

	return &PostgresMigrator{
		connStr: connStr,
	}, nil
}

// MigrateUp will migrate all the way up, applying all up migrations from the sourceURL
func (p *PostgresMigrator) MigrateUp(sourceURL string) error {
	m, err := migrate.New(sourceURL, p.connStr)
	if err != nil {
		return errors.Wrapf(err, "migrate.New(): fileURL=%s and connectionURL=%s", sourceURL, p.connStr)
	}
	m.Log = new(logger)

	if err := m.Up(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Up(): %s", sourceURL)
	}

	if err, dbErr := m.Close(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Close(): source error: %s", sourceURL)
	} else if dbErr != nil {
		return errors.Wrapf(dbErr, "migrate.Migrate.Close(): database error: %s", sourceURL)
	}

	return nil
}
