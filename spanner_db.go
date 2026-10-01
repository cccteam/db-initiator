package dbinitiator

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instanceadm "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/cccteam/db-initiator/internal/runner"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/option"
)

// SpannerDB represents a database created and ready for migrations
type SpannerDB struct {
	dbStr      string
	admin      *spannerDB.DatabaseAdminClient
	closeAdmin bool
	*spanner.Client
}

// NewSpannerDatabase will create a spanner database
func NewSpannerDatabase(ctx context.Context, projectID, instanceID, dbName string, opts ...option.ClientOption) (*SpannerDB, error) {
	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	db, err := newSpannerDatabase(ctx, adminClient, projectID, instanceID, dbName, opts...)
	if err != nil {
		if closeErr := adminClient.Close(); closeErr != nil {
			return nil, errors.Wrap(errors.Join(err, closeErr), "spannerDB.DatabaseAdminClient.Close()")
		}

		return nil, err
	}

	db.closeAdmin = true

	return db, nil
}

// newSpannerDatabase creates the database, then the client on it. The client
// comes second on purpose: a spanner.Client starts creating its session as soon
// as it is built, and a session requested before the database exists is
// answered "Database not found"; a query that arrives while that request is
// still in flight fails with it, which a consumer's CI met on its first query
// of a just-created database.
func newSpannerDatabase(ctx context.Context, adminClient *spannerDB.DatabaseAdminClient, projectID, instanceID, dbName string, opts ...option.ClientOption) (*SpannerDB, error) {
	dbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	op, err := adminClient.CreateDatabase(ctx,
		&databasepb.CreateDatabaseRequest{
			Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
		},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "database.DatabaseAdminClient.CreateDatabase()")
	}

	if _, err := op.Wait(ctx); err != nil {
		return nil, errors.Wrapf(err, "database.CreateDatabaseOperation.Wait()")
	}

	client, err := spanner.NewClientWithConfig(ctx, dbStr, spanner.ClientConfig{DisableNativeMetrics: true}, opts...)
	if err != nil {
		if dropErr := adminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbStr}); dropErr != nil {
			return nil, errors.Wrap(errors.Join(err, dropErr), "spanner.NewClientWithConfig(), and the database it was for could not be dropped")
		}

		return nil, errors.Wrapf(err, "spanner.NewClientWithConfig()")
	}

	return &SpannerDB{
		dbStr:  dbStr,
		admin:  adminClient,
		Client: client,
	}, nil
}

// MigrateUp applies every up migration of every sourceURL, in the order given. Each source
// is applied from its own first version: the schema migrations table is reset before each
// one, so a test can layer an application's schema and then its fixtures, each numbered
// from 1.
func (db *SpannerDB) MigrateUp(sourceURL ...string) error {
	ctx := context.Background()
	r := db.runner()

	for _, source := range sourceURL {
		src, err := runner.Open(source)
		if err != nil {
			return errors.Wrap(err, "runner.Open()")
		}

		if err := r.Force(ctx, -1); err != nil {
			return errors.Wrapf(err, "runner.Spanner.Force(): %s", source)
		}

		if err := r.Up(ctx, src); err != nil {
			return errors.Wrapf(err, "runner.Spanner.Up(): %s", source)
		}
	}

	return nil
}

// MigrateDown reverts every version of the sourceURL, from the database's current version
// down to no version.
func (db *SpannerDB) MigrateDown(sourceURL string) error {
	src, err := runner.Open(sourceURL)
	if err != nil {
		return errors.Wrap(err, "runner.Open()")
	}

	if err := db.runner().Down(context.Background(), src); err != nil {
		return errors.Wrapf(err, "runner.Spanner.Down(): %s", sourceURL)
	}

	return nil
}

// runner returns the migration runner on the default schema migrations table.
func (db *SpannerDB) runner() *runner.Spanner {
	return runner.NewSpanner(db.admin, db.Client, db.dbStr, defaultSchemaMigrationsTable)
}

func (db *SpannerDB) DropDatabase(ctx context.Context) error {
	if err := db.admin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: db.dbStr}); err != nil {
		return errors.Wrap(err, "database.DatabaseAdminClient.DropDatabase()")
	}

	return nil
}

// Close cleans up open resources
func (db *SpannerDB) Close() error {
	db.Client.Close()

	if db.closeAdmin {
		if err := db.admin.Close(); err != nil {
			return errors.Wrap(err, "database.DatabaseAdminClient.Close()")
		}
	}

	return nil
}

// NewSpannerInstance creates a spanner instance. This is intended for use with a spanner emulator.
func NewSpannerInstance(ctx context.Context, projectID, instanceID string, opts ...option.ClientOption) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return errors.Wrap(err, "instanceadmin.NewInstanceAdminClient()")
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx,
		&instanceadm.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", projectID),
			InstanceId: instanceID,
			Instance: &instanceadm.Instance{
				DisplayName: instanceID,
			},
		},
	)
	if err != nil {
		return errors.Wrapf(err, "instanceadmin.InstanceAdminClient.CreateInstance()")
	}

	i, err := op.Wait(ctx)
	if err != nil {
		return errors.Wrapf(err, "instanceadmin.CreateInstanceOperation.Wait()")
	}
	if i.State != instanceadm.Instance_READY {
		return errors.Newf("instanceadmin.CreateInstanceOperation.Wait(): State = %v", i.State)
	}

	return nil
}
