package dbinitiator

import (
	"context"

	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
)

type PostgresMigrator struct {
	connStr string
}

var _ Migrator = (*PostgresMigrator)(nil)

// NewPostgresMigrator returns a new [PostgresMigrator].
// It does not attempt to create the database or schema.
func NewPostgresMigrator(username, password, host, port, database string) *PostgresMigrator {
	connStr := PostgresConnStr(username, password, host, port, database)

	return &PostgresMigrator{
		connStr: connStr,
	}
}

// MigrateUp will migrate all the way up, applying all up migrations from the sourceURL
func (p *PostgresMigrator) MigrateUpSchema(_ context.Context, sourceURL string) error {
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

// FIXME(zredinger): implement this method
func (p *PostgresMigrator) MigrateUpData(_ context.Context, sourceURL string) error {
	return errors.New("Not implemented")
}

// FIXME(zredinger): implement this method
func (p *PostgresMigrator) MigrateDropSchema(_ context.Context) error {
	return errors.New("Not implemented")
}
