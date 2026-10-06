package dbinitiator

import (
	"context"
	"testing"

	"github.com/go-playground/errors/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgAssertion is a query returning one boolean a test expects true.
type pgAssertion struct {
	name  string
	query string
}

// pgAssert runs the assertions and reports those that return false.
func pgAssert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, stage string, assertions []pgAssertion) {
	t.Helper()

	for _, a := range assertions {
		result, err := pgAssertionQuery(ctx, pool, a.query)
		if err != nil {
			t.Fatalf("%s %q failed to execute: %v", stage, a.name, err)
		}
		if !result {
			t.Errorf("%s %q returned false", stage, a.name)
		}
	}
}

// pgAssertionQuery executes a SQL query that returns a single boolean value
func pgAssertionQuery(ctx context.Context, pool *pgxpool.Pool, query string) (bool, error) {
	var result bool
	if err := pool.QueryRow(ctx, query).Scan(&result); err != nil {
		return false, errors.Wrap(err, "pgxpool.Pool.QueryRow().Scan()")
	}

	return result, nil
}

// The post-drop assertions: nothing of the migrations' remains in the user schemas, while
// the container's btree_gist extension and the schemas themselves stay.
func pgDroppedAssertions() []pgAssertion {
	return []pgAssertion{
		{
			name:  "No relations should exist in the user schemas after drop",
			query: `SELECT NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%')`,
		},
		{
			name:  "No routines but the extensions' should exist after drop",
			query: `SELECT NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'))`,
		},
		{
			name:  "No types but the extensions' should exist after drop",
			query: `SELECT NOT EXISTS(SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_class c ON c.oid = t.typrelid WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%' AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c')) AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e'))`,
		},
		{
			name:  "schema_migrations table should not exist after drop",
			query: `SELECT to_regclass('schema_migrations') IS NULL`,
		},
		{
			name:  "btree_gist extension should still be installed after drop",
			query: `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'btree_gist')`,
		},
		{
			name:  "btree_gist extension should keep its routines after drop",
			query: `SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_depend d ON d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e' JOIN pg_extension e ON e.oid = d.refobjid WHERE e.extname = 'btree_gist')`,
		},
	}
}

func TestNewPostgresMigrator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sslMode SSLMode
	}{
		{name: "the default ssl mode"},
		{name: "ssl disabled", sslMode: SSLModeDisable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewPostgresMigrator("u", "p", "localhost", "5432", "db", tt.sslMode)

			if want := PostgresConnStr("u", "p", "localhost", "5432", "db", tt.sslMode); p.connStr != want {
				t.Errorf("connStr = %q, want %q", p.connStr, want)
			}
			if p.database != "db" {
				t.Errorf("database = %q, want %q", p.database, "db")
			}
			if p.schemaMigrationsTable != "schema_migrations" {
				t.Errorf("schemaMigrationsTable = %q, want %q", p.schemaMigrationsTable, "schema_migrations")
			}
			if p.dataMigrationsTable != "data_migrations" {
				t.Errorf("dataMigrationsTable = %q, want %q", p.dataMigrationsTable, "data_migrations")
			}
		})
	}
}

func TestPostgresMigrator_MethodChaining(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		schemaMigrationsTable string
		dataMigrationsTable   string
	}{
		{
			name:                  "chain both methods",
			schemaMigrationsTable: "custom_schema",
			dataMigrationsTable:   "custom_data",
		},
		{
			name:                  "chain with empty values",
			schemaMigrationsTable: "",
			dataMigrationsTable:   "",
		},
		{
			name:                  "chain with complex table names",
			schemaMigrationsTable: "schema_migrations_table_v2",
			dataMigrationsTable:   "data_migrations_table_v2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewPostgresMigrator("u", "p", "localhost", "5432", "db", SSLModeDisable)

			result := p.WithSchemaMigrationsTable(tt.schemaMigrationsTable).WithDataMigrationsTable(tt.dataMigrationsTable)

			if result.schemaMigrationsTable != tt.schemaMigrationsTable {
				t.Errorf("schemaMigrationsTable = %v, want %v", result.schemaMigrationsTable, tt.schemaMigrationsTable)
			}
			if result.dataMigrationsTable != tt.dataMigrationsTable {
				t.Errorf("dataMigrationsTable = %v, want %v", result.dataMigrationsTable, tt.dataMigrationsTable)
			}
			if result != p {
				t.Error("Method chaining should return the same instance")
			}
		})
	}
}

func TestPostgresMigrator_MigrateUpSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		sourceURL string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []pgAssertion
		postAssertions []pgAssertion
	}{
		{
			name: "successful schema migration",
			args: args{
				sourceURL: "file://testdata/postgres/migrations",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "test table should not exist before migration",
					query: `SELECT to_regclass('test') IS NULL`,
				},
			},
			postAssertions: []pgAssertion{
				{
					name:  "test table should exist after migration",
					query: `SELECT to_regclass('test') IS NOT NULL`,
				},
				{
					name:  "schema_migrations table should exist after migration",
					query: `SELECT to_regclass('schema_migrations') IS NOT NULL`,
				},
				{
					name:  "data_migrations table should not exist after a schema migration",
					query: `SELECT to_regclass('data_migrations') IS NULL`,
				},
			},
		},
		{
			name: "schema migration with views, routines, types, a sequence, partitions and a second schema",
			args: args{
				sourceURL: "file://testdata/postgres/migrations_full",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "products table should not exist before migration",
					query: `SELECT to_regclass('products') IS NULL`,
				},
			},
			postAssertions: []pgAssertion{
				{
					name:  "products table should exist after migration",
					query: `SELECT to_regclass('products') IS NOT NULL`,
				},
				{
					name:  "categories table should exist after migration",
					query: `SELECT to_regclass('categories') IS NOT NULL`,
				},
				{
					name:  "orders table should exist after migration",
					query: `SELECT to_regclass('orders') IS NOT NULL`,
				},
				{
					name:  "products_search index should exist after migration",
					query: `SELECT to_regclass('products_search') IS NOT NULL`,
				},
				{
					name:  "fk_orders_products foreign key should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname = 'fk_orders_products' AND contype = 'f')`,
				},
				{
					name:  "order_summary view should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'order_summary')`,
				},
				{
					name:  "product_counts materialized view should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = 'product_counts')`,
				},
				{
					name:  "product_status enum should exist after migration",
					query: `SELECT to_regtype('product_status') IS NOT NULL`,
				},
				{
					name:  "money_amount domain should exist after migration",
					query: `SELECT to_regtype('money_amount') IS NOT NULL`,
				},
				{
					name:  "address composite type should exist after migration",
					query: `SELECT to_regtype('address') IS NOT NULL`,
				},
				{
					name:  "set_order_date function should exist after migration",
					query: `SELECT to_regproc('set_order_date') IS NOT NULL`,
				},
				{
					name:  "archive_orders procedure should exist after migration",
					query: `SELECT to_regproc('archive_orders') IS NOT NULL`,
				},
				{
					name:  "order_numbers sequence should exist after migration",
					query: `SELECT to_regclass('order_numbers') IS NOT NULL`,
				},
				{
					name:  "events_2026_01 partition should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('events_2026_01') AND inhparent = to_regclass('events'))`,
				},
				{
					name:  "reporting.daily_sales table should exist after migration",
					query: `SELECT to_regclass('reporting.daily_sales') IS NOT NULL`,
				},
			},
		},
		{
			name: "connects and migrates",
			args: args{
				sourceURL: "file://testdata/postgres/migrations_connect_test",
			},
			wantErr: false,
			postAssertions: []pgAssertion{
				{
					name:  "testconnecttable should exist after migration",
					query: `SELECT to_regclass('testconnecttable') IS NOT NULL`,
				},
			},
		},
		{
			name: "migration source does not exist",
			args: args{
				sourceURL: "file://testdata/postgres/nonexistent",
			},
			wantErr: true,
		},
		{
			name: "migration with syntax error",
			args: args{
				sourceURL: "file://testdata/postgres/migration_error",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, migrator := testPostgresMigrator(ctx, t, container, tt.name)

			pgAssert(ctx, t, db.Pool, "Pre-assertion", tt.preAssertions)

			if err := migrator.MigrateUpSchema(ctx, tt.args.sourceURL); (err != nil) != tt.wantErr {
				t.Errorf("PostgresMigrator.MigrateUpSchema() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run post-migration assertions only if migration succeeded
			if !tt.wantErr {
				pgAssert(ctx, t, db.Pool, "Post-assertion", tt.postAssertions)
			}
		})
	}
}

func TestPostgresMigrator_MigrateUpData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		schemaSourceURL string
		dataSourceURL   string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []pgAssertion
		postAssertions []pgAssertion
	}{
		{
			name: "successful data migration",
			args: args{
				schemaSourceURL: "file://testdata/postgres/migrations",
				dataSourceURL:   "file://testdata/postgres/datamigrations",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "test table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM test) = 0`,
				},
			},
			postAssertions: []pgAssertion{
				{
					name:  "test table should have 1 row after data migration",
					query: `SELECT (SELECT COUNT(*) FROM test) = 1`,
				},
				{
					name:  "data_migrations table should exist after migration",
					query: `SELECT to_regclass('data_migrations') IS NOT NULL`,
				},
				{
					name:  "data_migrations should be at version 1 after migration",
					query: `SELECT (SELECT version FROM data_migrations) = 1`,
				},
				{
					name:  "schema_migrations should stay at version 1 after the data migration",
					query: `SELECT (SELECT version FROM schema_migrations) = 1`,
				},
			},
		},
		{
			name: "data migration with a materialized view and full-text search",
			args: args{
				schemaSourceURL: "file://testdata/postgres/migrations_full",
				dataSourceURL:   "file://testdata/postgres/datamigrations_full",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "products table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM products) = 0`,
				},
				{
					name:  "categories table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM categories) = 0`,
				},
			},
			postAssertions: []pgAssertion{
				{
					name:  "products table should have 3 rows after data migration",
					query: `SELECT (SELECT COUNT(*) FROM products) = 3`,
				},
				{
					name:  "categories table should have 3 rows after data migration",
					query: `SELECT (SELECT COUNT(*) FROM categories) = 3`,
				},
				{
					name:  "product_counts materialized view should count 3 categories after the refresh",
					query: `SELECT (SELECT COUNT(*) FROM product_counts) = 3`,
				},
				{
					name:  "full-text search should find the laptop after data migration",
					query: `SELECT EXISTS(SELECT 1 FROM products WHERE to_tsvector('english', name || ' ' || coalesce(description, '')) @@ to_tsquery('english', 'laptop'))`,
				},
			},
		},
		{
			name: "nonexistent data source",
			args: args{
				schemaSourceURL: "file://testdata/postgres/migrations",
				dataSourceURL:   "file://testdata/postgres/nonexistent",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, migrator := testPostgresMigrator(ctx, t, container, tt.name)

			// First apply schema migrations to set up the tables
			if err := migrator.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
				t.Fatalf("PostgresMigrator.MigrateUpSchema() error = %v", err)
			}

			pgAssert(ctx, t, db.Pool, "Pre-assertion", tt.preAssertions)

			if err := migrator.MigrateUpData(ctx, tt.args.dataSourceURL); (err != nil) != tt.wantErr {
				t.Errorf("PostgresMigrator.MigrateUpData() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run post-migration assertions only if we don't expect an error
			if !tt.wantErr {
				pgAssert(ctx, t, db.Pool, "Post-assertion", tt.postAssertions)
			}
		})
	}
}

// TestPostgresMigrator_TableNames keeps the versions in the tables the migrator is told to
// use, and in the defaults otherwise.
func TestPostgresMigrator_TableNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name        string
		schemaTable string
		dataTable   string
		wantSchema  string
		wantData    string
	}{
		{name: "the default tables", wantSchema: "schema_migrations", wantData: "data_migrations"},
		{name: "tables of the caller's naming", schemaTable: "versions", dataTable: "seeds", wantSchema: "versions", wantData: "seeds"},
		{name: "mixed case tables are quoted", schemaTable: "SchemaMigrations", dataTable: "DataMigrations", wantSchema: `"SchemaMigrations"`, wantData: `"DataMigrations"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, migrator := testPostgresMigrator(ctx, t, container, tt.name)
			if tt.schemaTable != "" {
				migrator = migrator.WithSchemaMigrationsTable(tt.schemaTable)
			}
			if tt.dataTable != "" {
				migrator = migrator.WithDataMigrationsTable(tt.dataTable)
			}

			if err := migrator.MigrateUpSchema(ctx, "file://testdata/postgres/migrations"); err != nil {
				t.Fatalf("PostgresMigrator.MigrateUpSchema() error = %v", err)
			}
			if err := migrator.MigrateUpData(ctx, "file://testdata/postgres/datamigrations"); err != nil {
				t.Fatalf("PostgresMigrator.MigrateUpData() error = %v", err)
			}

			for _, table := range []string{tt.wantSchema, tt.wantData} {
				pgAssertClean(ctx, t, db.Pool, table, 1)
			}
			for _, table := range []string{"schema_migrations", "data_migrations"} {
				want := table == tt.wantSchema || table == tt.wantData
				if got := pgTableExists(ctx, t, db.Pool, table); got != want {
					t.Errorf("table %s exists = %v, want %v", table, got, want)
				}
			}
			if n := pgCount(ctx, t, db.Pool, "test"); n != 1 {
				t.Errorf("test has %d rows, want the data migration's 1", n)
			}
		})
	}
}

func TestPostgresMigrator_MigrateDropSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		schemaSourceURL string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []pgAssertion
		postAssertions []pgAssertion
	}{
		{
			name: "successful drop schema",
			args: args{
				schemaSourceURL: "file://testdata/postgres/migrations",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "test table should exist before drop",
					query: `SELECT to_regclass('test') IS NOT NULL`,
				},
				{
					name:  "test_id_seq sequence should exist before drop",
					query: `SELECT to_regclass('test_id_seq') IS NOT NULL`,
				},
			},
			postAssertions: append([]pgAssertion{
				{
					name:  "test table should not exist after drop",
					query: `SELECT to_regclass('test') IS NULL`,
				},
			}, pgDroppedAssertions()...),
		},
		{
			name: "drop schema with views, routines, types, a sequence, partitions and a second schema",
			args: args{
				schemaSourceURL: "file://testdata/postgres/migrations_full",
			},
			wantErr: false,
			preAssertions: []pgAssertion{
				{
					name:  "products table should exist before drop",
					query: `SELECT to_regclass('products') IS NOT NULL`,
				},
				{
					name:  "order_summary view should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'order_summary')`,
				},
				{
					name:  "product_status enum should exist before drop",
					query: `SELECT to_regtype('product_status') IS NOT NULL`,
				},
				{
					name:  "set_order_date function should exist before drop",
					query: `SELECT to_regproc('set_order_date') IS NOT NULL`,
				},
				{
					name:  "reporting.daily_sales table should exist before drop",
					query: `SELECT to_regclass('reporting.daily_sales') IS NOT NULL`,
				},
			},
			postAssertions: append([]pgAssertion{
				{
					name:  "products table should not exist after drop",
					query: `SELECT to_regclass('products') IS NULL`,
				},
				{
					name:  "order_summary view should not exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'order_summary')`,
				},
				{
					name:  "product_counts materialized view should not exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = 'product_counts')`,
				},
				{
					name:  "product_status enum should not exist after drop",
					query: `SELECT to_regtype('product_status') IS NULL`,
				},
				{
					name:  "money_amount domain should not exist after drop",
					query: `SELECT to_regtype('money_amount') IS NULL`,
				},
				{
					name:  "set_order_date function should not exist after drop",
					query: `SELECT to_regproc('set_order_date') IS NULL`,
				},
				{
					name:  "order_numbers sequence should not exist after drop",
					query: `SELECT to_regclass('order_numbers') IS NULL`,
				},
				{
					name:  "reporting.daily_sales table should not exist after drop",
					query: `SELECT to_regclass('reporting.daily_sales') IS NULL`,
				},
				{
					name:  "reporting schema should stay after drop",
					query: `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'reporting')`,
				},
			}, pgDroppedAssertions()...),
		},
		{
			name: "drop schema on empty database",
			args: args{
				schemaSourceURL: "",
			},
			wantErr:        false,
			postAssertions: pgDroppedAssertions(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, migrator := testPostgresMigrator(ctx, t, container, tt.name)

			// Apply schema migrations if provided
			if tt.args.schemaSourceURL != "" {
				if err := migrator.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
					t.Fatalf("PostgresMigrator.MigrateUpSchema() error = %v", err)
				}
			}

			pgAssert(ctx, t, db.Pool, "Pre-assertion", tt.preAssertions)

			if err := migrator.MigrateDropSchema(ctx); (err != nil) != tt.wantErr {
				t.Errorf("PostgresMigrator.MigrateDropSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			pgAssert(ctx, t, db.Pool, "Post-assertion", tt.postAssertions)

			// The migrations apply again from the start.
			if tt.args.schemaSourceURL == "" {
				return
			}
			if err := migrator.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
				t.Fatalf("PostgresMigrator.MigrateUpSchema() after the drop error = %v", err)
			}
			pgAssertClean(ctx, t, db.Pool, "schema_migrations", 1)
		})
	}
}
