package dbinitiator

import (
	"context"
	"testing"
)

func TestNewPostgresMigrator(t *testing.T) {
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

	tests := []struct {
		name                   string
		migrationSourceDir     string
		expectMigrateUpError   bool
		expectMigrateDownError bool
	}{
		{
			name:                   "SuccessfulConnectAndMigrate",
			migrationSourceDir:     "file://testdata/postgres/migrations_connect_test",
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

			// Execution: Call NewPostgresMigrator
			pgMigrator := NewPostgresMigrator(pgContainer.unprivilegedUsername, pgContainer.password, pgContainer.host, pgContainer.port.Port(), tt.name)

			if pgMigrator == nil {
				t.Fatalf("PostgresMigration should not be nil for db %s", tt.name)
			}

			if err := pgMigrator.MigrateUpSchema(ctx, tt.migrationSourceDir); tt.expectMigrateUpError {
				if err == nil {
					t.Errorf("migrationService.MigrateUp should have failed for db %s, but got nil error", tt.name)
				}
			} else {
				if err != nil {
					t.Errorf("migrationService.MigrateUp should succeed for db %s: %v", tt.name, err)
				}
			}
		})
	}
}
