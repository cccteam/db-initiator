package dbinitiator

import (
	"context"
	"crypto/rand"
	"math/big"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/iterator"
)

func TestSpannerMigrator_MethodChaining(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		schemaMigrationsTable string
		dataMigrationsTable   string
	}{
		{
			name:                  "chain both methods",
			schemaMigrationsTable: "CustomSchema",
			dataMigrationsTable:   "CustomData",
		},
		{
			name:                  "chain with empty values",
			schemaMigrationsTable: "",
			dataMigrationsTable:   "",
		},
		{
			name:                  "chain with complex table names",
			schemaMigrationsTable: "Schema_Migrations_Table_v2",
			dataMigrationsTable:   "Data_Migrations_Table_v2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &SpannerMigrator{
				schemaMigrationsTable: "SchemaMigrations",
				dataMigrationsTable:   "DataMigrations",
			}

			result := s.WithSchemaMigrationsTable(tt.schemaMigrationsTable).WithDataMigrationsTable(tt.dataMigrationsTable)

			if result.schemaMigrationsTable != tt.schemaMigrationsTable {
				t.Errorf("schemaMigrationsTable = %v, want %v", result.schemaMigrationsTable, tt.schemaMigrationsTable)
			}
			if result.dataMigrationsTable != tt.dataMigrationsTable {
				t.Errorf("dataMigrationsTable = %v, want %v", result.dataMigrationsTable, tt.dataMigrationsTable)
			}
			if result != s {
				t.Error("Method chaining should return the same instance")
			}
		})
	}
}

func TestSpannerMigrator_MigrateUpSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		sourceURL string
	}
	type assertion struct {
		name  string
		query string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []assertion
		postAssertions []assertion
	}{
		{
			name: "successful schema migration",
			args: args{
				sourceURL: "file://testdata/spanner/migrations",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Users table should not exist before migration",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Users' AND table_schema = '')`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "Users table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Users' AND table_schema = '')`,
				},
				{
					name:  "Users_Username index should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.indexes WHERE index_name = 'Users_Username' AND table_schema = '')`,
				},
				{
					name:  "SchemaMigrations table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'SchemaMigrations' AND table_schema = '')`,
				},
			},
		},
		{
			name: "schema migration with search indexes, foreign keys, and views",
			args: args{
				sourceURL: "file://testdata/spanner/migrations_full",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Products table should not exist before migration",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Products' AND table_schema = '')`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "Products table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Products' AND table_schema = '')`,
				},
				{
					name:  "Categories table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Categories' AND table_schema = '')`,
				},
				{
					name:  "Orders table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Orders' AND table_schema = '')`,
				},
				{
					name:  "ProductsSearchIndex search index should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.indexes WHERE index_name = 'ProductsSearchIndex' AND index_type IN ('INDEX', 'SEARCH'))`,
				},
				{
					name:  "Categories_Name index should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.indexes WHERE index_name = 'Categories_Name' AND table_schema = '')`,
				},
				{
					name:  "FK_Orders_Products foreign key should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'FK_Orders_Products' AND constraint_type = 'FOREIGN KEY')`,
				},
				{
					name:  "OrderSummary view should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'OrderSummary' AND table_type = 'VIEW')`,
				},
			},
		},
		{
			name: "migration source does not exist",
			args: args{
				sourceURL: "file://testdata/spanner/nonexistent",
			},
			wantErr: true,
		},
		{
			name: "migration with syntax error",
			args: args{
				sourceURL: "file://testdata/spanner/migration_error",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dbName := genDBName()
			db, err := container.CreateDatabase(ctx, dbName)
			if err != nil {
				t.Fatalf("SpannerContainer.CreateDatabase() error = %v", err)
			}
			defer func() {
				if err := db.DropDatabase(context.Background()); err != nil {
					t.Errorf("DB.DropDatabase() err=%s", err)
				}
				if err := db.Close(); err != nil {
					t.Errorf("DB.Close() err=%s", err)
				}
			}()

			svc, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("NewSpannerMigrator() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrator.Close() err=%s", err)
				}
			}()

			// Run pre-migration assertions
			for _, a := range tt.preAssertions {
				if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
					t.Fatalf("Pre-assertion %q failed to execute: %v", a.name, err)
				} else if !result {
					t.Errorf("Pre-assertion %q returned false", a.name)
				}
			}

			if err := svc.MigrateUpSchema(ctx, tt.args.sourceURL); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrator.MigrateUpSchema() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run post-migration assertions only if migration succeeded
			if !tt.wantErr {
				for _, a := range tt.postAssertions {
					if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
						t.Fatalf("Post-assertion %q failed to execute: %v", a.name, err)
					} else if !result {
						t.Errorf("Post-assertion %q returned false", a.name)
					}
				}
			}
		})
	}
}

func TestSpannerMigrator_MigrateUpData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		schemaSourceURL string
		dataSourceURL   string
	}
	type assertion struct {
		name  string
		query string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []assertion
		postAssertions []assertion
	}{
		{
			name: "successful data migration",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations2",
				dataSourceURL:   "file://testdata/spanner/datamigrations",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Users2 table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM Users2) = 0`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "Users2 table should have 1 row after data migration",
					query: `SELECT (SELECT COUNT(*) FROM Users2) = 1`,
				},
				{
					name:  "DataMigrations table should exist after migration",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'DataMigrations' AND table_schema = '')`,
				},
			},
		},
		{
			name: "data migration with search indexes and foreign keys",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations_full",
				dataSourceURL:   "file://testdata/spanner/datamigrations_full",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Products table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM Products) = 0`,
				},
				{
					name:  "Categories table should be empty before data migration",
					query: `SELECT (SELECT COUNT(*) FROM Categories) = 0`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "Products table should have 3 rows after data migration",
					query: `SELECT (SELECT COUNT(*) FROM Products) = 3`,
				},
				{
					name:  "Categories table should have 3 rows after data migration",
					query: `SELECT (SELECT COUNT(*) FROM Categories) = 3`,
				},
				{
					name:  "Search index should be queryable after data migration",
					query: `SELECT EXISTS(SELECT 1 FROM Products WHERE SEARCH(Name_Tokens, 'laptop'))`,
				},
			},
		},
		{
			name: "nonexistent data source",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations",
				dataSourceURL:   "file://testdata/spanner/nonexistent",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dbName := genDBName()

			db, err := container.CreateDatabase(ctx, dbName)
			if err != nil {
				t.Fatalf("SpannerContainer.CreateDatabase() error = %v", err)
			}
			defer func() {
				if err := db.DropDatabase(context.Background()); err != nil {
					t.Errorf("DB.DropDatabase() err=%s", err)
				}
				if err := db.Close(); err != nil {
					t.Errorf("DB.Close() err=%s", err)
				}
			}()

			svc, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("NewSpannerMigrator() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrator.Close() err=%s", err)
				}
			}()

			// First apply schema migrations to set up the tables
			if err := svc.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
				t.Fatalf("SpannerMigrator.MigrateUpSchema() error = %v", err)
			}

			// Run pre-migration assertions
			for _, a := range tt.preAssertions {
				if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
					t.Fatalf("Pre-assertion %q failed to execute: %v", a.name, err)
				} else if !result {
					t.Errorf("Pre-assertion %q returned false", a.name)
				}
			}

			if err := svc.MigrateUpData(ctx, tt.args.dataSourceURL); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrator.MigrateUpData() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run post-migration assertions only if we don't expect an error
			if !tt.wantErr {
				for _, a := range tt.postAssertions {
					if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
						t.Fatalf("Post-assertion %q failed to execute: %v", a.name, err)
					} else if !result {
						t.Errorf("Post-assertion %q returned false", a.name)
					}
				}
			}
		})
	}
}

func TestSpannerMigrator_MigrateDropSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		schemaSourceURL string
	}
	type assertion struct {
		name  string
		query string
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		preAssertions  []assertion
		postAssertions []assertion
	}{
		{
			name: "successful drop schema",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Users table should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Users' AND table_schema = '')`,
				},
				{
					name:  "Users_Username index should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.indexes WHERE index_name = 'Users_Username' AND table_schema = '')`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "No user tables should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '' AND table_type = 'BASE TABLE')`,
				},
				{
					name:  "No user indexes should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.indexes WHERE table_schema = '' AND index_type = 'INDEX')`,
				},
			},
		},
		{
			name: "drop schema with search indexes, foreign keys, and views",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations_full",
			},
			wantErr: false,
			preAssertions: []assertion{
				{
					name:  "Products table should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'Products' AND table_schema = '')`,
				},
				{
					name:  "ProductsSearchIndex should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.indexes WHERE index_name = 'ProductsSearchIndex')`,
				},
				{
					name:  "FK_Orders_Products foreign key should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'FK_Orders_Products' AND constraint_type = 'FOREIGN KEY')`,
				},
				{
					name:  "OrderSummary view should exist before drop",
					query: `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'OrderSummary' AND table_type = 'VIEW')`,
				},
			},
			postAssertions: []assertion{
				{
					name:  "No user tables should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '' AND table_type = 'BASE TABLE')`,
				},
				{
					name:  "No user indexes should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.indexes WHERE table_schema = '' AND index_type IN ('INDEX', 'SEARCH'))`,
				},
				{
					name:  "No foreign keys should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema = '' AND constraint_type = 'FOREIGN KEY')`,
				},
				{
					name:  "No views should exist after drop",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '' AND table_type = 'VIEW')`,
				},
			},
		},
		{
			name: "drop schema on empty database",
			args: args{
				schemaSourceURL: "",
			},
			wantErr: false,
			postAssertions: []assertion{
				{
					name:  "No user tables should exist after drop on empty database",
					query: `SELECT NOT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '' AND table_type = 'BASE TABLE')`,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dbName := genDBName()
			db, err := container.CreateDatabase(ctx, dbName)
			if err != nil {
				t.Fatalf("SpannerContainer.CreateDatabase() error = %v", err)
			}
			defer func() {
				if err := db.DropDatabase(context.Background()); err != nil {
					t.Errorf("DB.DropDatabase() err=%s", err)
				}
				if err := db.Close(); err != nil {
					t.Errorf("DB.Close() err=%s", err)
				}
			}()

			svc, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("NewSpannerMigrator() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrator.Close() err=%s", err)
				}
			}()

			// Apply schema migrations if provided
			if tt.args.schemaSourceURL != "" {
				if err := svc.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
					t.Fatalf("SpannerMigrator.MigrateUpSchema() error = %v", err)
				}
			}

			// Run pre-drop assertions
			for _, a := range tt.preAssertions {
				if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
					t.Fatalf("Pre-assertion %q failed to execute: %v", a.name, err)
				} else if !result {
					t.Errorf("Pre-assertion %q returned false", a.name)
				}
			}

			if err := svc.MigrateDropSchema(ctx); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrator.MigrateDropSchema() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run post-drop assertions only if drop succeeded
			if !tt.wantErr {
				for _, a := range tt.postAssertions {
					if result, err := assertionQuery(ctx, db.Client, a.query); err != nil {
						t.Fatalf("Post-assertion %q failed to execute: %v", a.name, err)
					} else if !result {
						t.Errorf("Post-assertion %q returned false", a.name)
					}
				}
			}
		})
	}
}

func genDBName() string {
	var randStr strings.Builder

	for range 10 {
		n, _ := rand.Int(rand.Reader, big.NewInt(26))
		randStr.WriteString(string('a' + rune(n.Int64())))
	}

	return randStr.String()
}

// assertionQuery executes a SQL query that returns a single boolean value
func assertionQuery(ctx context.Context, client *spanner.Client, query string) (bool, error) {
	iter := client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return false, errors.New("query returned no rows")
		}

		return false, errors.Wrap(err, "spanner.RowIterator.Next()")
	}

	var result bool
	if err := row.Columns(&result); err != nil {
		return false, errors.Wrap(err, "spanner.Row.Columns()")
	}

	return result, nil
}
