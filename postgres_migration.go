package dbinitiator

import (
	"context"
	"fmt"
	"strings"

	"github.com/cccteam/db-initiator/internal/runner"
	ccclogger "github.com/cccteam/logger"
	"github.com/go-playground/errors/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The version tables a [PostgresMigrator] uses unless told otherwise, and the one a
// [PostgresDatabase] uses.
const (
	defaultPostgresSchemaMigrationsTable = "schema_migrations"
	defaultPostgresDataMigrationsTable   = "data_migrations"
)

// PostgresMigrator handles connecting to an existing PostgreSQL database and running
// migrations. Each file runs inside one transaction: a file that fails rolls back whole and
// the version stays where it was.
type PostgresMigrator struct {
	connStr               string
	database              string
	dataMigrationsTable   string
	schemaMigrationsTable string
}

var _ Migrator = (*PostgresMigrator)(nil)

// NewPostgresMigrator returns a new [PostgresMigrator].
// It does not attempt to create the database or schema.
//
// Uses the following tables by default to store migration versions:
//   - Data Migrations table: "data_migrations"
//   - Schema Migrations table: "schema_migrations"
//
// sslMode sets the sslmode query parameter.
// Pass an empty string to use the default, which is [SSLModeRequire].
// Use [SSLModeDisable] only for local test containers.
func NewPostgresMigrator(username, password, host, port, database string, sslMode SSLMode) *PostgresMigrator {
	return &PostgresMigrator{
		connStr:               PostgresConnStr(username, password, host, port, database, sslMode),
		database:              database,
		dataMigrationsTable:   defaultPostgresDataMigrationsTable,
		schemaMigrationsTable: defaultPostgresSchemaMigrationsTable,
	}
}

// WithSchemaMigrationsTable allows setting the schema migration table to be used
func (p *PostgresMigrator) WithSchemaMigrationsTable(table string) *PostgresMigrator {
	p.schemaMigrationsTable = table

	return p
}

// WithDataMigrationsTable allows setting the data migration table to be used
func (p *PostgresMigrator) WithDataMigrationsTable(table string) *PostgresMigrator {
	p.dataMigrationsTable = table

	return p
}

// MigrateUpSchema applies every pending up migration from the sourceURL, recording the
// versions in the schema migrations table. A database already at the last version is a
// normal result, not an error. A file that fails rolls back whole and the version stays
// where it was.
//
// Use for DDL migrations
func (p *PostgresMigrator) MigrateUpSchema(ctx context.Context, sourceURL string) error {
	ccclogger.FromCtx(ctx).Infof("Applying schema migrations from %s", sourceURL)
	if err := p.migrateUp(ctx, p.schemaMigrationsTable, sourceURL); err != nil {
		return errors.Wrap(err, "PostgresMigrator.migrateUp()")
	}

	return nil
}

// MigrateUpData applies every pending up migration from the sourceURL, recording the
// versions in the data migrations table. It behaves as [PostgresMigrator.MigrateUpSchema].
//
// Use for DML migrations
func (p *PostgresMigrator) MigrateUpData(ctx context.Context, sourceURL string) error {
	ccclogger.FromCtx(ctx).Infof("Applying data migrations from %s", sourceURL)
	if err := p.migrateUp(ctx, p.dataMigrationsTable, sourceURL); err != nil {
		return errors.Wrap(err, "PostgresMigrator.migrateUp()")
	}

	return nil
}

// SchemaVersion reads the schema migrations table.
func (p *PostgresMigrator) SchemaVersion(ctx context.Context) (Version, error) {
	return p.version(ctx, p.schemaMigrationsTable)
}

// DataVersion reads the data migrations table.
func (p *PostgresMigrator) DataVersion(ctx context.Context) (Version, error) {
	return p.version(ctx, p.dataMigrationsTable)
}

// ForceSchema sets the schema migrations table to a version, clean; -1 leaves the database
// with no version. For when the runner cannot tell what state the database is in.
func (p *PostgresMigrator) ForceSchema(ctx context.Context, version int) error {
	return p.force(ctx, p.schemaMigrationsTable, version)
}

// ForceData sets the data migrations table to a version, as [PostgresMigrator.ForceSchema]
// does for the schema.
func (p *PostgresMigrator) ForceData(ctx context.Context, version int) error {
	return p.force(ctx, p.dataMigrationsTable, version)
}

// MigrateDropSchema drops all objects in the schema: everything the migrations created, the
// version tables included, so the migrations apply again from the start. It covers every
// schema of the database but PostgreSQL's own, skips the objects an installed extension
// owns, and leaves the schemas themselves in place, so a migration that creates a schema
// should say IF NOT EXISTS. The drops run in one transaction: one that is refused leaves
// the database as it was.
//
// This happens in the following order:
//  1. Drop views
//  2. Drop materialized views
//  3. Drop tables, and with them their indexes, constraints, triggers and owned sequences
//  4. Drop sequences
//  5. Drop routines: functions, procedures and aggregates
//  6. Drop types: enums, domains, ranges and composite types
func (p *PostgresMigrator) MigrateDropSchema(ctx context.Context) error {
	pool, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "pgxpool.Pool.Begin()")
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var dropped []string
	for _, step := range postgresDropSteps() {
		n, err := step.run(ctx, tx)
		if err != nil {
			return err
		}
		if n > 0 {
			dropped = append(dropped, step.count(n))
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "pgx.Tx.Commit()")
	}

	log := ccclogger.FromCtx(ctx)
	if len(dropped) == 0 {
		log.Info("No database objects found to drop")
	} else {
		log.Infof("Dropped %s", strings.Join(dropped, ", "))
	}

	return nil
}

// migrateUp applies the pending migrations of the sourceURL, recording the versions in
// migrationsTable.
func (p *PostgresMigrator) migrateUp(ctx context.Context, migrationsTable, sourceURL string) error {
	src, err := runner.Open(sourceURL)
	if err != nil {
		return errors.Wrap(err, "runner.Open()")
	}

	return p.run(ctx, migrationsTable, func(r *runner.Postgres) error {
		if err := r.Up(ctx, src); err != nil {
			return errors.Wrapf(err, "runner.Postgres.Up(): %s", sourceURL)
		}

		return nil
	})
}

// version reads migrationsTable.
func (p *PostgresMigrator) version(ctx context.Context, migrationsTable string) (Version, error) {
	var v Version
	err := p.run(ctx, migrationsTable, func(r *runner.Postgres) error {
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

// force sets migrationsTable to a version, clean.
func (p *PostgresMigrator) force(ctx context.Context, migrationsTable string, version int) error {
	return p.run(ctx, migrationsTable, func(r *runner.Postgres) error {
		if err := r.Force(ctx, version); err != nil {
			return errors.Wrap(err, "runner.Postgres.Force()")
		}

		return nil
	})
}

// run opens a connection pool for one call, hands fn a runner keeping its versions in
// migrationsTable, and closes the pool after.
func (p *PostgresMigrator) run(ctx context.Context, migrationsTable string, fn func(r *runner.Postgres) error) error {
	pool, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	return fn(runner.NewPostgres(pool, migrationsTable))
}

// connect opens a connection pool to the database.
func (p *PostgresMigrator) connect(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := openDB(ctx, p.connStr)
	if err != nil {
		return nil, errors.Wrapf(err, "connecting to database %s", p.database)
	}

	return pool, nil
}

// postgresDropStep is one kind of object [PostgresMigrator.MigrateDropSchema] drops: the
// keyword of its DROP statement and the query listing the objects of the kind, each
// schema-qualified and quoted as DROP wants it named.
type postgresDropStep struct {
	keyword string
	query   string
}

// postgresDropSteps lists the kinds of object [PostgresMigrator.MigrateDropSchema] drops, in
// order.
func postgresDropSteps() []postgresDropStep {
	return []postgresDropStep{
		{keyword: "VIEW", query: postgresRelationsQuery("'v'")},
		{keyword: "MATERIALIZED VIEW", query: postgresRelationsQuery("'m'")},
		{keyword: "TABLE", query: postgresRelationsQuery("'r', 'p'")},
		{keyword: "SEQUENCE", query: postgresRelationsQuery("'S'")},
		{keyword: "ROUTINE", query: postgresRoutinesQuery},
		{keyword: "TYPE", query: postgresTypesQuery},
	}
}

// run drops every object the step's query lists, in one statement, and reports how many.
func (s postgresDropStep) run(ctx context.Context, tx pgx.Tx) (int, error) {
	names, err := s.list(ctx, tx)
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 0, nil
	}

	if _, err := tx.Exec(ctx, "DROP "+s.keyword+" IF EXISTS "+strings.Join(names, ", ")+" CASCADE"); err != nil {
		return 0, errors.Wrapf(err, "dropping %s", s.count(len(names)))
	}

	return len(names), nil
}

// list runs the step's query.
func (s postgresDropStep) list(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, s.query)
	if err != nil {
		return nil, errors.Wrapf(err, "listing the %ss", s.noun())
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Wrap(err, "pgx.Rows.Scan()")
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "listing the %ss", s.noun())
	}

	return names, nil
}

// noun names one of the step's objects in a message.
func (s postgresDropStep) noun() string {
	return strings.ToLower(s.keyword)
}

// count phrases a number of the step's objects: "1 table", "3 tables".
func (s postgresDropStep) count(n int) string {
	if n == 1 {
		return "1 " + s.noun()
	}

	return fmt.Sprintf("%d %ss", n, s.noun())
}

// postgresUserSchemas is the condition, on a pg_namespace row n, that keeps every schema but
// PostgreSQL's own: information_schema and the pg_ prefixed ones, a prefix no one else may
// use.
const postgresUserSchemas = `n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'`

// The queries listing the routines and the types of the user schemas, skipping what an
// extension owns; a routine is named with the arguments that identify it.
const (
	postgresRoutinesQuery = `
		SELECT format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid))
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE ` + postgresUserSchemas + `
		  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
		ORDER BY 1`

	postgresTypesQuery = `
		SELECT format('%I.%I', n.nspname, t.typname)
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		LEFT JOIN pg_class c ON c.oid = t.typrelid
		WHERE ` + postgresUserSchemas + `
		  AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c'))
		  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e')
		ORDER BY 1`
)

// postgresRelationsQuery lists the relations of the given pg_class kinds in the user
// schemas, skipping what an extension owns.
func postgresRelationsQuery(relkinds string) string {
	return `
		SELECT format('%I.%I', n.nspname, c.relname)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN (` + relkinds + `)
		  AND ` + postgresUserSchemas + `
		  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
		ORDER BY 1`
}
