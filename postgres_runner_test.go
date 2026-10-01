package dbinitiator

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-playground/errors/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgVersionRow reads the Postgres version table's row; ok is false when it has none.
func pgVersionRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (version int64, dirty, ok bool) {
	t.Helper()

	err := pool.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false
	}
	if err != nil {
		t.Fatalf("reading schema_migrations: %v", err)
	}

	return version, dirty, true
}

// pgTableExists reports whether a table exists in the connection's schema search path.
func pgTableExists(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()

	var found bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&found); err != nil {
		t.Fatalf("checking table %s: %v", table, err)
	}

	return found
}

// pgCount counts a table's rows.
func pgCount(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()

	var n int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}

	return n
}

// TestPostgresMigrator_Lifecycle walks a database through the plain path and a failed file:
// the file rolls back whole, the version stays, the fixed file applies, and a rerun is no
// change.
//
// Deliberately not a table: each step depends on the state the previous one left behind.
func TestPostgresMigrator_Lifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, err := container.CreateDatabase(ctx, "lifecycle")
	if err != nil {
		t.Fatalf("PostgresContainer.CreateDatabase() error = %v", err)
	}
	defer db.Close()
	migrator := NewPostgresMigrator(container.unprivilegedUsername, container.password, container.host, container.port.Port(), db.dbName, SSLModeDisable)

	source := writeSource(t, map[string]string{
		"000001_a.up.sql":   "CREATE TABLE a (id serial PRIMARY KEY);",
		"000001_a.down.sql": "DROP TABLE a;",
		"000002_b.up.sql":   "CREATE TABLE b (id serial PRIMARY KEY);\nINSERT INTO nope (id) VALUES (1);",
		"000002_b.down.sql": "DROP TABLE b;",
	})

	// A fresh database has no version.
	v, err := migrator.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	if v.String() != "no version" {
		t.Errorf("SchemaVersion() = %q, want no version", v)
	}

	// File 2's second statement fails: the file rolls back whole and the version stays at 1.
	err = migrator.MigrateUpSchema(ctx, source)
	if err == nil || !strings.Contains(err.Error(), "000002_b.up.sql failed and was rolled back; the database stays at version 1") {
		t.Fatalf("MigrateUpSchema() error = %v, want the rollback message", err)
	}
	if version, dirty, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 1 || dirty {
		t.Errorf("schema_migrations = (%d, %v, %v), want (1, false, true)", version, dirty, ok)
	}
	if !pgTableExists(ctx, t, db.Pool, "a") {
		t.Error("table a missing after file 1")
	}
	if pgTableExists(ctx, t, db.Pool, "b") {
		t.Error("table b exists: file 2 should have rolled back whole")
	}

	// Fixed, file 2 applies.
	writeMigration(t, source, "000002_b.up.sql", "CREATE TABLE b (id serial PRIMARY KEY);\nINSERT INTO b (id) VALUES (1);")
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema() after the fix error = %v", err)
	}
	if version, dirty, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 2 || dirty {
		t.Errorf("schema_migrations = (%d, %v, %v), want (2, false, true)", version, dirty, ok)
	}
	if !pgTableExists(ctx, t, db.Pool, "b") {
		t.Error("table b missing after the fixed file 2")
	}

	// A rerun is no change.
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema() rerun error = %v, want nil for no change", err)
	}
	v, err = migrator.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	if v.String() != "version 2" {
		t.Errorf("SchemaVersion() = %q, want version 2", v)
	}
}

// TestPostgresMigrator_Force moves the version without running anything.
func TestPostgresMigrator_Force(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name    string
		force   int
		wantErr bool
		want    string
	}{
		{name: "forced to 7", force: 7, want: "version 7"},
		{name: "forced to 0", force: 0, want: "version 0"},
		{name: "forced to -1 has no version", force: -1, want: "no version"},
		{name: "forced below -1 refuses", force: -2, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := container.CreateDatabase(ctx, tt.name)
			if err != nil {
				t.Fatalf("PostgresContainer.CreateDatabase() error = %v", err)
			}
			defer db.Close()
			migrator := NewPostgresMigrator(container.unprivilegedUsername, container.password, container.host, container.port.Port(), db.dbName, SSLModeDisable)

			err = migrator.ForceSchema(ctx, tt.force)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ForceSchema(%d) error = %v, wantErr %v", tt.force, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			v, err := migrator.SchemaVersion(ctx)
			if err != nil {
				t.Fatalf("SchemaVersion() error = %v", err)
			}
			if v.String() != tt.want {
				t.Errorf("SchemaVersion() after ForceSchema(%d) = %q, want %q", tt.force, v, tt.want)
			}
			version, dirty, ok := pgVersionRow(ctx, t, db.Pool)
			if tt.force >= 0 && (!ok || version != int64(tt.force) || dirty) {
				t.Errorf("schema_migrations = (%d, %v, %v), want (%d, false, true)", version, dirty, ok, tt.force)
			}
			if tt.force < 0 && ok {
				t.Error("schema_migrations has a row after ForceSchema(-1), want none")
			}
		})
	}
}

// TestPostgresMigrator_Unsupported pins the data-migration methods' refusal on Postgres.
func TestPostgresMigrator_Unsupported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	migrator := NewPostgresMigrator("u", "p", "localhost", "5432", "db", SSLModeDisable)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "MigrateUpData", call: func() error {
			return migrator.MigrateUpData(ctx, "file://nowhere")
		}},
		{name: "DataVersion", call: func() error {
			_, err := migrator.DataVersion(ctx)

			return err
		}},
		{name: "ForceData", call: func() error {
			return migrator.ForceData(ctx, 1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.call(); !errors.Is(err, errPostgresDataMigrations) {
				t.Errorf("%s error = %v, want errPostgresDataMigrations", tt.name, err)
			}
		})
	}
}

// TestPostgresDatabase_Concurrent starts two runners on one database together: both finish,
// every file applies once, and the version is right, which is the advisory lock at work.
func TestPostgresDatabase_Concurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name    string
		runners int
	}{
		{name: "two runners", runners: 2},
		{name: "four runners", runners: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := container.CreateDatabase(ctx, tt.name)
			if err != nil {
				t.Fatalf("PostgresContainer.CreateDatabase() error = %v", err)
			}
			defer db.Close()

			// Every file inserts one row into a table the first file creates, so a file
			// applied twice would fail on the primary key.
			files := map[string]string{"000001_t.up.sql": "CREATE TABLE t (n int PRIMARY KEY);\nINSERT INTO t (n) VALUES (1);"}
			for n := 2; n <= 5; n++ {
				files[strings.Repeat("0", 5)+string(rune('0'+n))+"_n.up.sql"] = "INSERT INTO t (n) VALUES (" + string(rune('0'+n)) + ");"
			}
			source := writeSource(t, files)

			var wg sync.WaitGroup
			results := make([]error, tt.runners)
			for i := range tt.runners {
				wg.Add(1)
				go func() {
					defer wg.Done()
					migrator := NewPostgresMigrator(container.unprivilegedUsername, container.password, container.host, container.port.Port(), db.dbName, SSLModeDisable)
					results[i] = migrator.MigrateUpSchema(ctx, source)
				}()
			}
			wg.Wait()

			for i, err := range results {
				if err != nil {
					t.Errorf("runner %d error = %v", i, err)
				}
			}
			if version, dirty, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 5 || dirty {
				t.Errorf("schema_migrations = (%d, %v, %v), want (5, false, true)", version, dirty, ok)
			}
			if n := pgCount(ctx, t, db.Pool, "t"); n != 5 {
				t.Errorf("t has %d rows, want 5: every file once", n)
			}
		})
	}
}

// TestPostgresDatabase_SourcesAndDown applies two sources, each from its own first version,
// walks the fixtures' down file, forces the version to the schema's last and walks its down
// files back to no version; a failing down file rolls back and the version stays.
//
// Deliberately not a table: each step depends on the state the previous one left behind.
func TestPostgresDatabase_SourcesAndDown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, err := container.CreateDatabase(ctx, "sources")
	if err != nil {
		t.Fatalf("PostgresContainer.CreateDatabase() error = %v", err)
	}
	defer db.Close()
	migrator := NewPostgresMigrator(container.unprivilegedUsername, container.password, container.host, container.port.Port(), db.dbName, SSLModeDisable)

	schema := writeSource(t, map[string]string{
		"000001_a.up.sql":   "CREATE TABLE a (id int PRIMARY KEY);",
		"000001_a.down.sql": "DROP TABLE a;",
		"000002_b.up.sql":   "CREATE TABLE b (id int PRIMARY KEY);",
		"000002_b.down.sql": "DROP TABLE b;",
	})
	fixtures := writeSource(t, map[string]string{
		"000001_rows.up.sql":   "INSERT INTO a (id) VALUES (1), (2);",
		"000001_rows.down.sql": "DELETE FROM a;",
	})

	// Each source applies from its own first version; the row ends at the second source's.
	if err := db.MigrateUp(schema, fixtures); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	if version, _, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 1 {
		t.Errorf("schema_migrations version = %d, want the second source's 1", version)
	}
	if n := pgCount(ctx, t, db.Pool, "a"); n != 2 {
		t.Errorf("a has %d rows, want 2", n)
	}

	// The fixtures' down file empties a and leaves no version.
	if err := db.MigrateDown(fixtures); err != nil {
		t.Fatalf("MigrateDown(fixtures) error = %v", err)
	}
	if _, _, ok := pgVersionRow(ctx, t, db.Pool); ok {
		t.Error("schema_migrations has a row after MigrateDown, want none")
	}
	if n := pgCount(ctx, t, db.Pool, "a"); n != 0 {
		t.Errorf("a has %d rows after the fixtures' down, want 0", n)
	}

	// Forced to the schema's last version, a failing down file rolls back and the version stays.
	if err := migrator.ForceSchema(ctx, 2); err != nil {
		t.Fatalf("ForceSchema(2) error = %v", err)
	}
	writeMigration(t, schema, "000002_b.down.sql", "DROP TABLE nope;")
	err = db.MigrateDown(schema)
	if err == nil || !strings.Contains(err.Error(), "000002_b.down.sql failed and was rolled back; the database stays at version 2") {
		t.Fatalf("MigrateDown() error = %v, want the rollback message", err)
	}
	if version, dirty, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 2 || dirty {
		t.Errorf("schema_migrations = (%d, %v, %v), want (2, false, true)", version, dirty, ok)
	}
	if !pgTableExists(ctx, t, db.Pool, "b") {
		t.Error("table b missing: the failed down file should have rolled back")
	}

	// Restored, the down files walk back to no version, and down on no version is no change.
	writeMigration(t, schema, "000002_b.down.sql", "DROP TABLE b;")
	if err := db.MigrateDown(schema); err != nil {
		t.Fatalf("MigrateDown(schema) error = %v", err)
	}
	if _, _, ok := pgVersionRow(ctx, t, db.Pool); ok {
		t.Error("schema_migrations has a row after MigrateDown, want none")
	}
	for _, table := range []string{"a", "b"} {
		if pgTableExists(ctx, t, db.Pool, table) {
			t.Errorf("table %s exists after MigrateDown", table)
		}
	}
	if err := db.MigrateDown(schema); err != nil {
		t.Fatalf("MigrateDown() on no version error = %v, want nil", err)
	}
	if err := db.MigrateUp(schema); err != nil {
		t.Fatalf("MigrateUp(schema) again error = %v", err)
	}
	if version, _, ok := pgVersionRow(ctx, t, db.Pool); !ok || version != 2 {
		t.Errorf("schema_migrations version = %d, want 2 after applying the schema again", version)
	}
}
