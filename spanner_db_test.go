package dbinitiator

import (
	"context"
	"testing"
)

func TestConnectToSpanner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup Spanner Emulator (once for all subtests)
	spannerContainer, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer should not return an error: %v", err)
	}
	t.Cleanup(func() {
		if spannerContainer != nil {
			if err := spannerContainer.Terminate(ctx); err != nil {
				t.Errorf("spannerContainer.Terminate should not return an error: %v", err)
			}
		}
	})

	tests := []struct {
		name                   string
		dbName                 string
		migrationSourceDir     string
		expectConnectError     bool
		expectMigrateUpError   bool
		expectMigrateDownError bool
	}{
		{
			name:                   "SuccessfulConnectAndMigrate",
			dbName:                 "testconnectspannerdb",
			migrationSourceDir:     "file://testdata/spanner/migrations_connect_test",
			expectConnectError:     false,
			expectMigrateUpError:   false,
			expectMigrateDownError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup: Create the specific database for this sub-test
			initialDB, err := spannerContainer.CreateDatabase(ctx, tt.dbName)
			if !tt.expectConnectError { // Only expect successful DB creation if not expecting a connect error
				if err != nil {
					t.Fatalf("spannerContainer.CreateDatabase should succeed for db %s: %v", tt.dbName, err)
				}
			} else if err != nil {
				// If CreateDatabase itself fails and we expected a connect error, this might be the error.
				t.Fatalf("spannerContainer.CreateDatabase failed for %s: %v, cannot proceed to ConnectToSpanner test", tt.dbName, err)
			}

			defer func() {
				if initialDB != nil { // initialDB might be nil if CreateDatabase failed
					if err := initialDB.DropDatabase(ctx); err != nil {
						t.Errorf("initialDB.DropDatabase failed for %s: %v", tt.dbName, err)
					}
					if err := initialDB.Close(); err != nil {
						t.Errorf("initialDB.Close failed for %s: %v", tt.dbName, err)
					}
				}
			}()

			// Execution: Call ConnectToSpanner
			migrationService, err := ConnectToSpanner(ctx, spannerContainer.projectID, spannerContainer.instanceID, tt.dbName, spannerContainer.opts...)
			if tt.expectConnectError {
				if err == nil {
					t.Errorf("ConnectToSpanner should have failed for db %s, but got nil error", tt.dbName)
				}
				return // End this subtest
			}
			if err != nil {
				t.Fatalf("ConnectToSpanner should succeed for db %s: %v", tt.dbName, err)
			}
			if migrationService == nil {
				t.Fatalf("SpannerMigrationService should not be nil for db %s", tt.dbName)
			}
			defer func() {
				if err := migrationService.Close(); err != nil {
					t.Errorf("SpannerMigrationService.Close failed for %s: %v", tt.dbName, err)
				}
			}()

			err = migrationService.MigrateUp(tt.migrationSourceDir)
			if tt.expectMigrateUpError {
				if err == nil {
					t.Errorf("SpannerMigrationService.MigrateUp should have failed for db %s, but got nil error", tt.dbName)
				}
			} else {
				if err != nil {
					t.Errorf("SpannerMigrationService.MigrateUp should succeed for db %s: %v", tt.dbName, err)
				}
			}
		})
	}
}
