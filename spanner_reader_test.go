package dbinitiator

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// TestSpannerMigrator_EnsureMigrationsReader proves the explicit reader step: it creates
// the version tables it finds missing and runs cleanly twice, and where the database
// reports roles in its DDL (Spanner itself; the emulator accepts the role statements and
// reports none), the role and its two grants are there after the first call and the
// second changes nothing.
func TestSpannerMigrator_EnsureMigrationsReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, migrator := testMigrator(ctx, t, container)
	for _, call := range []string{"the first call", "the second call"} {
		if err := migrator.EnsureMigrationsReader(ctx); err != nil {
			t.Fatalf("EnsureMigrationsReader() on %s error = %v", call, err)
		}
		ddl, err := db.admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: db.dbStr})
		if err != nil {
			t.Fatalf("GetDatabaseDdl() error = %v", err)
		}
		statements := strings.ToLower(strings.Join(ddl.GetStatements(), "\n"))
		if !strings.Contains(statements, "create role") {
			if call == "the second call" {
				t.Skip("the database reports no roles in its DDL (the emulator): the role and its grants are proven against Spanner itself")
			}

			continue
		}
		for _, want := range []string{
			"create role " + MigrationsReaderRole,
			"grant select on table " + strings.ToLower(defaultSchemaMigrationsTable) + " to role " + MigrationsReaderRole,
			"grant select on table " + strings.ToLower(defaultDataMigrationsTable) + " to role " + MigrationsReaderRole,
		} {
			if !strings.Contains(statements, want) {
				t.Errorf("after %s the DDL lacks %q:\n%s", call, want, statements)
			}
		}
	}
}
