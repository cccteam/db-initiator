package dbinitiator

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	ccclogger "github.com/cccteam/logger"
	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	spannerDriver "github.com/golang-migrate/migrate/v4/database/spanner"
	_ "github.com/golang-migrate/migrate/v4/source/file" // up/down script file source driver for the migrate package
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// SpannerMigrator handles connecting to an existing spanner database and running migrations
type SpannerMigrator struct {
	connectionString      string
	dataMigrationsTable   string
	schemaMigrationsTable string
	databaseName          string
	admin                 *spannerDB.DatabaseAdminClient
	client                *spanner.Client
}

// ConnectToSpanner connects to an existing spanner database and returns a [SpannerMigrator]
//
// Uses the following tables by default to store migration versions:
//   - Data Migrations table: "DataMigrations"
//   - Schema Migrations table: "SchemaMigrations"
func ConnectToSpanner(ctx context.Context, projectID, instanceID, dbName string, opts ...option.ClientOption) (*SpannerMigrator, error) {
	dbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	client, err := spanner.NewClient(ctx, dbStr, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "spanner.NewClient()")
	}

	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		client.Close()

		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	return &SpannerMigrator{
		dataMigrationsTable:   "DataMigrations",
		schemaMigrationsTable: "SchemaMigrations",
		connectionString:      dbStr,
		databaseName:          dbName,
		admin:                 adminClient,
		client:                client,
	}, nil
}

// WithSchemaMigrationsTable allows setting the schema migration table to be used
func (s *SpannerMigrator) WithSchemaMigrationsTable(table string) *SpannerMigrator {
	s.schemaMigrationsTable = table

	return s
}

// WithDataMigrationsTable allows setting the data migration table to be used
func (s *SpannerMigrator) WithDataMigrationsTable(table string) *SpannerMigrator {
	s.dataMigrationsTable = table

	return s
}

// MigrateUpSchema will migrate all the way up, applying all up migrations from the sourceURL
//
// Use for DDL migrations
func (s *SpannerMigrator) MigrateUpSchema(ctx context.Context, sourceURL string) error {
	ccclogger.FromCtx(ctx).Infof("Applying schema migrations from %s", sourceURL)
	if err := s.migrateUp(s.schemaMigrationsTable, sourceURL); err != nil {
		return errors.Wrap(err, "SpannerMigrator.migrateUp()")
	}

	return nil
}

// MigrateUpData will apply all data migrations from the sourceURL
//
// Use for DML migrations
func (s *SpannerMigrator) MigrateUpData(ctx context.Context, sourceURL string) error {
	ccclogger.FromCtx(ctx).Infof("Applying data migrations from %s", sourceURL)
	if err := s.migrateUp(s.dataMigrationsTable, sourceURL); err != nil {
		return errors.Wrap(err, "SpannerMigrator.migrateUp()")
	}

	return nil
}

// MigrateDropSchema drops all objects in the schema
func (s *SpannerMigrator) MigrateDropSchema(ctx context.Context, sourceURL string) error {
	if err := s.drop(ctx); err != nil {
		return errors.Wrapf(err, "SpannerMigrator.drop(): %s", sourceURL)
	}

	return nil
}

// Close closes the SpannerMigrator connections
func (s *SpannerMigrator) Close() error {
	s.client.Close()

	if err := s.admin.Close(); err != nil {
		return errors.Wrap(err, "database.DatabaseAdminClient.Close()")
	}

	return nil
}

func (s *SpannerMigrator) migrateUp(migrationsTable, sourceURL string) error {
	m, err := s.newMigrate(migrationsTable, sourceURL)
	if err != nil {
		return errors.Wrap(err, "SpannerMigrator.newMigrate()")
	}

	if err := m.Up(); err != nil {
		return errors.Wrapf(err, "migrate.Migrate.Up(): %s", sourceURL)
	}

	return nil
}

// newMigrate creates a new migrate instance
func (s *SpannerMigrator) newMigrate(migrationsTable, sourceURL string) (*migrate.Migrate, error) {
	conf := &spannerDriver.Config{DatabaseName: s.connectionString, CleanStatements: true, MigrationsTable: migrationsTable}
	spannerInstance, err := spannerDriver.WithInstance(
		spannerDriver.NewDB(*s.admin, *s.client),
		conf,
	)
	if err != nil {
		return nil, errors.Wrap(err, "spannerDriver.WithInstance()")
	}

	m, err := migrate.NewWithDatabaseInstance(sourceURL, "spanner", spannerInstance)
	if err != nil {
		return nil, errors.Wrapf(err, "migrate.NewWithDatabaseInstance(): fileURL=%s, db=%s", sourceURL, s.connectionString)
	}
	m.Log = new(logger)

	return m, nil
}

// drop creates statements to drop the indexes and tables accordingly.
// Drop happens in the following order:
//  1. Drop views
//  2. Drop FK constraints
//  3. Drop Indexes
//  4. Drop tables
func (s *SpannerMigrator) drop(ctx context.Context) error {
	stmts := make([]string, 0, 10)
	viewDropStatements, err := s.viewDropStatements(ctx)
	if err != nil {
		return err
	}
	stmts = append(stmts, viewDropStatements...)

	foreignKeyDropStatements, err := s.foreignKeyDropStatements(ctx)
	if err != nil {
		return err
	}
	stmts = append(stmts, foreignKeyDropStatements...)

	indexDropStatements, err := s.indexDropStatements(ctx)
	if err != nil {
		return err
	}
	stmts = append(stmts, indexDropStatements...)

	tableDropStatements, err := s.tableDropStatements(ctx)
	if err != nil {
		return err
	}
	stmts = append(stmts, tableDropStatements...)

	if len(stmts) > 0 {
		op, err := s.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   s.connectionString,
			Statements: stmts,
		})
		if err != nil {
			return errors.Wrap(err, "SpannerMigrator.admin.UpdateDatabaseDdl()")
		}
		if err := op.Wait(ctx); err != nil {
			return errors.Wrap(err, "SpannerMigrator.admin.UpdateDatabaseDdl().Wait()")
		}
	}

	return nil
}

func (s *SpannerMigrator) viewDropStatements(ctx context.Context) ([]string, error) {
	query := `
		SELECT CONCAT('DROP VIEW ` + "`" + `', TABLE_NAME, '` + "`" + `') AS ddl
		FROM information_schema.tables
		WHERE NOT TABLE_SCHEMA IN('INFORMATION_SCHEMA', 'SPANNER_SYS')
		  AND TABLE_TYPE = 'VIEW'
		ORDER BY TABLE_NAME`

	iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	var stmts []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "spanner.RowIterator.Next()")
		}

		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, errors.Wrap(err, "spanner.Row.Columns()")
		}
		stmts = append(stmts, stmt)
	}

	return stmts, nil
}

func (s *SpannerMigrator) foreignKeyDropStatements(ctx context.Context) ([]string, error) {
	query := `
		SELECT CONCAT(
			'ALTER TABLE ',
			CASE
				WHEN tc.table_schema = '' THEN CONCAT('` + "`" + `', tc.table_name, '` + "`" + `')
				ELSE CONCAT('` + "`" + `', tc.table_schema, '` + "`.`" + `', tc.table_name, '` + "`" + `')
			END,
			' DROP CONSTRAINT ` + "`" + `', tc.constraint_name, '` + "`" + `'
		) AS ddl
		FROM information_schema.table_constraints tc
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND NOT CONSTRAINT_SCHEMA IN('INFORMATION_SCHEMA', 'SPANNER_SYS')
		ORDER BY tc.table_schema, tc.table_name, tc.constraint_name`

	iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	var stmts []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "spanner.RowIterator.Next()")
		}

		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, errors.Wrap(err, "spanner.Row.Columns()")
		}
		stmts = append(stmts, stmt)
	}

	return stmts, nil
}

func (s *SpannerMigrator) indexDropStatements(ctx context.Context) ([]string, error) {
	query := `
		SELECT CONCAT('DROP INDEX IF EXISTS ` + "`" + `', idx.index_name, '` + "`" + `') AS ddl
		FROM information_schema.indexes idx
		WHERE idx.index_type = 'INDEX'
			AND NOT TABLE_SCHEMA IN('INFORMATION_SCHEMA', 'SPANNER_SYS')
		ORDER BY idx.table_schema, idx.table_name, idx.index_name`

	iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	var stmts []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "spanner.RowIterator.Next()")
		}

		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, &database.Error{OrigErr: err}
		}
		stmts = append(stmts, stmt)
	}

	return stmts, nil
}

func (s *SpannerMigrator) tableDropStatements(ctx context.Context) ([]string, error) {
	query := `
		WITH t AS (
			SELECT table_name, parent_table_name
			FROM information_schema.tables
			WHERE NOT TABLE_SCHEMA IN('INFORMATION_SCHEMA', 'SPANNER_SYS')
			  AND table_type = 'BASE TABLE'
		),
		d AS (
			SELECT
				c.table_name,
				CAST(p1.table_name IS NOT NULL AS INT64) +
				CAST(p2.table_name IS NOT NULL AS INT64) +
				CAST(p3.table_name IS NOT NULL AS INT64) +
				CAST(p4.table_name IS NOT NULL AS INT64) +
				CAST(p5.table_name IS NOT NULL AS INT64) +
				CAST(p6.table_name IS NOT NULL AS INT64) +
				CAST(p7.table_name IS NOT NULL AS INT64) AS depth
			FROM t c
			LEFT JOIN t p1 ON c.parent_table_name = p1.table_name
			LEFT JOIN t p2 ON p1.parent_table_name = p2.table_name
			LEFT JOIN t p3 ON p2.parent_table_name = p3.table_name
			LEFT JOIN t p4 ON p3.parent_table_name = p4.table_name
			LEFT JOIN t p5 ON p4.parent_table_name = p5.table_name
			LEFT JOIN t p6 ON p5.parent_table_name = p6.table_name
			LEFT JOIN t p7 ON p6.parent_table_name = p7.table_name
		)
		SELECT CONCAT('DROP TABLE ` + "`" + `', table_name, '` + "`" + `') AS ddl
		FROM d
		ORDER BY depth DESC, table_name`

	iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	var stmts []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "spanner.RowIterator.Next()")
		}

		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, errors.Wrap(err, "spanner.Row.Columns()")
		}
		stmts = append(stmts, stmt)
	}

	return stmts, nil
}
