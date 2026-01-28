package dbinitiator

import (
	"context"
	"crypto/rand"
	"math/big"
	"strings"
	"testing"
)

func TestSpannerMigrationService_MethodChaining(t *testing.T) {
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

func TestSpannerMigrationService_MigrateUpSchema(t *testing.T) {
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
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "successful schema migration",
			args: args{
				sourceURL: "file://testdata/spanner/migrations",
			},
			wantErr: false,
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

			svc, err := ConnectToSpanner(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("ConnectToSpanner() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrationService.Close() err=%s", err)
				}
			}()

			if err := svc.MigrateUpSchema(ctx, tt.args.sourceURL); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrationService.MigrateUpSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSpannerMigrationService_MigrateUpData(t *testing.T) {
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
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "successful data migration",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations2",
				dataSourceURL:   "file://testdata/spanner/datamigrations",
			},
			wantErr: false,
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

			svc, err := ConnectToSpanner(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("ConnectToSpanner() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrationService.Close() err=%s", err)
				}
			}()

			// First apply schema migrations to set up the tables
			if err := svc.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
				t.Fatalf("SpannerMigrationService.MigrateUpSchema() error = %v", err)
			}

			if err := svc.MigrateUpData(ctx, tt.args.dataSourceURL); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrationService.MigrateUpData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSpannerMigrationService_MigrateDropSchema(t *testing.T) {
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
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "successful drop schema",
			args: args{
				schemaSourceURL: "file://testdata/spanner/migrations",
			},
			wantErr: false,
		},
		{
			name: "drop schema on empty database",
			args: args{
				schemaSourceURL: "",
			},
			wantErr: false,
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

			svc, err := ConnectToSpanner(ctx, container.projectID, container.instanceID, dbName, container.opts...)
			if err != nil {
				t.Fatalf("ConnectToSpanner() error = %v", err)
			}
			defer func() {
				if err := svc.Close(); err != nil {
					t.Errorf("SpannerMigrationService.Close() err=%s", err)
				}
			}()

			// Apply schema migrations if provided
			if tt.args.schemaSourceURL != "" {
				if err := svc.MigrateUpSchema(ctx, tt.args.schemaSourceURL); err != nil {
					t.Fatalf("SpannerMigrationService.MigrateUpSchema() error = %v", err)
				}
			}

			if err := svc.MigrateDropSchema(ctx, tt.args.schemaSourceURL); (err != nil) != tt.wantErr {
				t.Errorf("SpannerMigrationService.MigrateDropSchema() error = %v, wantErr %v", err, tt.wantErr)
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
