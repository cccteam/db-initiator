package dbinitiator

import (
	"context"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type connectPgTestCase struct {
	name                   string
	migrationSourceDir     string
	expectConnectError     bool
	expectMigrateUpError   bool
	expectMigrateDownError bool
}

func TestConnectToPostgres(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup PostgreSQL Container (once for all subtests)
	pgContainer, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer should not return an error: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Errorf("pgContainer.Terminate should not return an error: %v", err)
		}
	})

	tests := []connectPgTestCase{
		{
			name:                   "SuccessfulConnectAndMigrate",
			migrationSourceDir:     "file://testdata/postgres/migrations_connect_test",
			expectConnectError:     false,
			expectMigrateUpError:   false,
			expectMigrateDownError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			initialDB, err := pgContainer.CreateDatabase(ctx, tt.name)
			if err != nil {
				t.Fatalf("pgContainer.CreateDatabase should succeed for db %s: %v", tt.name, err)
			}
			defer initialDB.Close()

			// Execution: Call ConnectToPostgres
			connectedDB, err := ConnectToPostgres(ctx, pgContainer.unprivilegedUsername, pgContainer.password, pgContainer.host, pgContainer.port.Port(), tt.name, pgContainer.unprivilegedUsername)

			if tt.expectConnectError {
				if err == nil {
					t.Fatalf("ConnectToPostgres should have failed for db %s, but got nil error", tt.name)
				}

				return // End this subtest if connection error was expected and occurred (or correctly didn't occur but was expected)
			}
			if err != nil {
				t.Fatalf("ConnectToPostgres should succeed for db %s: %v", tt.name, err)
			}
			if connectedDB == nil {
				t.Fatalf("connectedDB should not be nil for db %s", tt.name)
			}
			defer connectedDB.Close()

			err = connectedDB.MigrateUp(tt.migrationSourceDir)
			if tt.expectMigrateUpError {
				if err == nil {
					t.Errorf("connectedDB.MigrateUp should have failed for db %s, but got nil error", tt.name)
				}
			} else {
				if err != nil {
					t.Errorf("connectedDB.MigrateUp should succeed for db %s: %v", tt.name, err)
				}
			}

			// Only attempt MigrateDown if MigrateUp was expected to succeed
			if !tt.expectMigrateUpError {
				err = connectedDB.MigrateDown(tt.migrationSourceDir)
				if tt.expectMigrateDownError {
					if err == nil {
						t.Errorf("connectedDB.MigrateDown should have failed for db %s, but got nil error", tt.name)
					}
				} else {
					if err != nil {
						t.Errorf("connectedDB.MigrateDown should succeed for db %s: %v", tt.name, err)
					}
				}
			}
		})
	}
}

func TestPostgres_FullMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("New(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		upSourceURL   []string
		downSourceURL string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantUpErr   bool
		wantDownErr bool
	}{
		{
			name: "FullMigration",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migrations",
			},
		},
		{
			name: "Migration up error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_error"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_error",
			},
			wantDownErr: true,
		},
		{
			name: "Migration up source error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_does_not_exist"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down source error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_does_not_exist",
			},
			wantDownErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, err := container.CreateDatabase(ctx, tt.name)
			if (err != nil) != tt.wantErr {
				t.Fatalf("PostgresContainer.CreateDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if err := db.MigrateUp(tt.args.upSourceURL...); (err != nil) != tt.wantUpErr {
				t.Fatalf("db.MigrateUp() error = %v, wantUpErr %v", err, tt.wantUpErr)
			}
			if tt.wantUpErr {
				return
			}

			if err := db.MigrateDown(tt.args.downSourceURL); (err != nil) != tt.wantDownErr {
				t.Fatalf("db.MigrateDown() error = %v, wantDownErr %v", err, tt.wantDownErr)
			}
		})
	}
}

func TestPostgres_FullMigrationWithNewPostgresDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pgC, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("New(): %s", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	type args struct {
		upSourceURL   []string
		downSourceURL string
	}
	tests := []struct {
		name        string
		args        args
		wantUpErr   bool
		wantDownErr bool
	}{
		{
			name: "FullMigration",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migrations",
			},
		},
		{
			name: "Migration up error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_error"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_error",
			},
			wantDownErr: true,
		},
		{
			name: "Migration up source error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_does_not_exist"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down source error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_does_not_exist",
			},
			wantDownErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, err := NewPostgresDatabase(ctx, pgC.superUsername, pgC.password, pgC.host, pgC.port.Port(), tt.name, pgC.unprivilegedUsername)
			if err != nil {
				t.Fatalf("NewPostgresDatabase() error = %v", err)
			}
			defer db.Close()

			if err := db.MigrateUp(tt.args.upSourceURL...); (err != nil) != tt.wantUpErr {
				t.Fatalf("db.MigrateUp() error = %v, wantUpErr %v", err, tt.wantUpErr)
			}
			if tt.wantUpErr {
				return
			}

			if err := db.MigrateDown(tt.args.downSourceURL); (err != nil) != tt.wantDownErr {
				t.Fatalf("db.MigrateDown() error = %v, wantDownErr %v", err, tt.wantDownErr)
			}
		})
	}
}
