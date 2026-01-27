// package spannermigrate handles connecting to a spanner database and running migrations
package spannermigrate

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cccteam/logger"
	"github.com/go-playground/errors/v5"
	"github.com/zredinger-ccc/migrate/v4"
	spannerDriver "github.com/zredinger-ccc/migrate/v4/database/spanner"
	_ "github.com/zredinger-ccc/migrate/v4/source/file" // up/down script file source driver for the migrate package
	"google.golang.org/api/option"
)

type Client struct {
	connectionString      string
	dataMigrationsTable   string
	schemaMigrationsTable string
	admin                 *spannerDB.DatabaseAdminClient
	client                *spanner.Client
}

// Connect connects to an existing spanner database and returns a Client
func Connect(ctx context.Context, projectID, instanceID, dbName string, opts ...option.ClientOption) (*Client, error) {
	dbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	client, err := spanner.NewClient(ctx, dbStr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "spanner.NewClient()")
	}

	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		client.Close()

		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	return &Client{
		dataMigrationsTable:   "DataMigrations",
		schemaMigrationsTable: "SchemaMigrations",
		connectionString:      dbStr,
		admin:                 adminClient,
		client:                client,
	}, nil
}

// WithSchemaMigrationsTable allows setting the schema migration table to be used
func (c *Client) WithSchemaMigrationsTable(table string) *Client {
	c.schemaMigrationsTable = table

	return c
}

// WithDataMigrationsTable allows setting the data migration table to be used
func (c *Client) WithDataMigrationsTable(table string) *Client {
	c.dataMigrationsTable = table

	return c
}

// MigrateUpSchema will migrate all the way up, applying all up migrations from the sourceURL
func (c *Client) MigrateUpSchema(ctx context.Context, sourceURL string) error {
	logger.FromCtx(ctx).Infof("Applying schema migrations from %s", sourceURL)
	m, err := c.newMigrate(c.schemaMigrationsTable, sourceURL)
	if err != nil {
		return errors.Wrap(err, "Client.newMigrate()")
	}

	if err := m.Up(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Up(): %s", sourceURL)
	}

	return nil
}

// MigrateUpData will apply all data migrations from the sourceURL
func (c *Client) MigrateUpData(ctx context.Context, sourceURL string) error {
	logger.FromCtx(ctx).Infof("Applying data migrations from %s", sourceURL)
	if err := c.migrateUp(c.dataMigrationsTable, sourceURL); err != nil {
		return errors.Wrap(err, "Client.migrateUp()")
	}

	return nil
}

// MigrateDropSchema drops all objects in the schema
func (c *Client) MigrateDropSchema(ctx context.Context, sourceURL string) error {
	m, err := c.newMigrate(c.schemaMigrationsTable, sourceURL)
	if err != nil {
		return errors.Wrap(err, "Client.newMigrate()")
	}
	defer func() {
		srcErr, dbErr := m.Close()
		if srcErr != nil {
			logger.FromCtx(ctx).Errorf("migrate.Migrate.Close() error: source error: %v: %s", srcErr, sourceURL)
		}
		if dbErr != nil {
			logger.FromCtx(ctx).Errorf("migrate.Migrate.Close() error: database error: %v: %s", dbErr, sourceURL)
		}
	}()

	if err := m.Drop(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Drop(): %s", sourceURL)
	}

	if err, dbErr := m.Close(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Close(): source error: %s", sourceURL)
	} else if dbErr != nil {
		return errors.Wrapf(dbErr, "migrate.Migrate.Close(): database error: %s", sourceURL)
	}

	return nil
}

func (c *Client) Close(ctx context.Context) {
	if err := c.admin.Close(); err != nil {
		logger.FromCtx(ctx).Errorf("failed to close admin client: %v", err)
	}
	c.client.Close()
}

func (c *Client) migrateUp(migrationsTable, sourceURL string) error {
	m, err := c.newMigrate(migrationsTable, sourceURL)
	if err != nil {
		return errors.Wrap(err, "Client.newMigrate()")
	}

	if err := m.Up(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Up(): %s", sourceURL)
	}

	return nil
}

// newMigrate creates a new migrate instance
func (c *Client) newMigrate(migrationsTable, sourceURL string) (*migrate.Migrate, error) {
	conf := &spannerDriver.Config{DatabaseName: c.connectionString, CleanStatements: true, MigrationsTable: migrationsTable}
	spannerInstance, err := spannerDriver.WithInstance(
		spannerDriver.NewDB(*c.admin, *c.client),
		conf,
	)
	if err != nil {
		return nil, errors.Wrap(err, "spannerDriver.WithInstance()")
	}

	m, err := migrate.NewWithDatabaseInstance(sourceURL, "spanner", spannerInstance)
	if err != nil {
		return nil, errors.Wrapf(err, "migrate.NewWithDatabaseInstance(): fileURL=%s, db=%s", sourceURL, c.connectionString)
	}

	return m, nil
}
