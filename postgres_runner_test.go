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

// testPostgresMigrator creates a database of that name and a migrator on it, as the
// database's owner; the database's pool goes with the test.
func testPostgresMigrator(ctx context.Context, t *testing.T, container *PostgresContainer, name string) (*PostgresDatabase, *PostgresMigrator) {
	t.Helper()

	db, err := container.CreateDatabase(ctx, name)
	if err != nil {
		t.Fatalf("PostgresContainer.CreateDatabase() error = %v", err)
	}
	t.Cleanup(db.Close)

	migrator := NewPostgresMigrator(container.unprivilegedUsername, container.password, container.host, container.port.Port(), db.dbName, SSLModeDisable)

	return db, migrator
}

// pgVersionRow reads a Postgres version table's row; ok is false when it has none.
func pgVersionRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) (version int64, dirty, ok bool) {
	t.Helper()

	err := pool.QueryRow(ctx, "SELECT version, dirty FROM "+pgx.Identifier{strings.Trim(table, `"`)}.Sanitize()).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false
	}
	if err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}

	return version, dirty, true
}

// pgAssertClean checks a version table says clean at a version.
func pgAssertClean(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, version int64) {
	t.Helper()

	got, dirty, ok := pgVersionRow(ctx, t, pool, table)
	if !ok {
		t.Fatalf("%s has no row, want clean at version %d", table, version)
	}
	if got != version || dirty {
		t.Errorf("%s = (version %d, dirty %v), want clean at version %d", table, got, dirty, version)
	}
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

// pgDatabaseExists reports whether the container's server has a database of that name.
func pgDatabaseExists(ctx context.Context, t *testing.T, container *PostgresContainer, name string) bool {
	t.Helper()

	pool, err := container.superUserConnection(ctx, container.defaultDatabase)
	if err != nil {
		t.Fatalf("PostgresContainer.superUserConnection() error = %v", err)
	}
	var found bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&found); err != nil {
		t.Fatalf("checking database %s: %v", name, err)
	}

	return found
}

// TestPostgresMigrator_Lifecycle walks a database through the plain path and a failed file:
// the file rolls back whole, the version stays, the fixed file applies, a rerun is no
// change, and the data table moves on its own.
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

	db, migrator := testPostgresMigrator(ctx, t, container, "lifecycle")
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
	pgAssertClean(ctx, t, db.Pool, "schema_migrations", 1)
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
	pgAssertClean(ctx, t, db.Pool, "schema_migrations", 2)
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

	// The data table moves on its own.
	data := writeSource(t, map[string]string{
		"000001_more.up.sql": "INSERT INTO b (id) VALUES (2);",
	})
	if err := migrator.MigrateUpData(ctx, data); err != nil {
		t.Fatalf("MigrateUpData() error = %v", err)
	}
	pgAssertClean(ctx, t, db.Pool, "data_migrations", 1)
	pgAssertClean(ctx, t, db.Pool, "schema_migrations", 2)
	if n := pgCount(ctx, t, db.Pool, "b"); n != 2 {
		t.Errorf("b has %d rows after the data migration, want 2", n)
	}
	dv, err := migrator.DataVersion(ctx)
	if err != nil {
		t.Fatalf("DataVersion() error = %v", err)
	}
	if dv.String() != "version 1" {
		t.Errorf("DataVersion() = %q, want version 1", dv)
	}
}

// TestPostgresMigrator_VersionAndForce reads and forces both tables.
func TestPostgresMigrator_VersionAndForce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name    string
		data    bool
		force   int
		wantErr bool
		want    string
	}{
		{name: "schema forced to 7", force: 7, want: "version 7"},
		{name: "schema forced to 0", force: 0, want: "version 0"},
		{name: "schema forced to -1 has no version", force: -1, want: "no version"},
		{name: "schema forced below -1 refuses", force: -2, wantErr: true},
		{name: "data forced to 7", data: true, force: 7, want: "version 7"},
		{name: "data forced to -1 has no version", data: true, force: -1, want: "no version"},
		{name: "data forced below -1 refuses", data: true, force: -2, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, migrator := testPostgresMigrator(ctx, t, container, tt.name)
			version, force := migrator.SchemaVersion, migrator.ForceSchema
			table, other := "schema_migrations", "data_migrations"
			if tt.data {
				version, force = migrator.DataVersion, migrator.ForceData
				table, other = other, table
			}

			// A fresh database has no version, and reading creates the table.
			v, err := version(ctx)
			if err != nil {
				t.Fatalf("Version() error = %v", err)
			}
			if v.String() != "no version" || v.Known {
				t.Errorf("Version() on a fresh database = %q, want no version", v)
			}

			err = force(ctx, tt.force)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Force(%d) error = %v, wantErr %v", tt.force, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			v, err = version(ctx)
			if err != nil {
				t.Fatalf("Version() error = %v", err)
			}
			if v.String() != tt.want || v.Dirty {
				t.Errorf("Version() after Force(%d) = %q, want %q", tt.force, v, tt.want)
			}
			row, dirty, ok := pgVersionRow(ctx, t, db.Pool, table)
			if tt.force >= 0 && (!ok || row != int64(tt.force) || dirty) {
				t.Errorf("%s = (%d, %v, %v), want (%d, false, true)", table, row, dirty, ok, tt.force)
			}
			if tt.force < 0 && ok {
				t.Errorf("%s has a row after Force(-1), want none", table)
			}
			if pgTableExists(ctx, t, db.Pool, other) {
				t.Errorf("%s exists, but only %s was touched", other, table)
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
			pgAssertClean(ctx, t, db.Pool, "schema_migrations", 5)
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

	db, migrator := testPostgresMigrator(ctx, t, container, "sources")

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
	if version, _, ok := pgVersionRow(ctx, t, db.Pool, "schema_migrations"); !ok || version != 1 {
		t.Errorf("schema_migrations version = %d, want the second source's 1", version)
	}
	if n := pgCount(ctx, t, db.Pool, "a"); n != 2 {
		t.Errorf("a has %d rows, want 2", n)
	}

	// The fixtures' down file empties a and leaves no version.
	if err := db.MigrateDown(fixtures); err != nil {
		t.Fatalf("MigrateDown(fixtures) error = %v", err)
	}
	if _, _, ok := pgVersionRow(ctx, t, db.Pool, "schema_migrations"); ok {
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
	pgAssertClean(ctx, t, db.Pool, "schema_migrations", 2)
	if !pgTableExists(ctx, t, db.Pool, "b") {
		t.Error("table b missing: the failed down file should have rolled back")
	}

	// Restored, the down files walk back to no version, and down on no version is no change.
	writeMigration(t, schema, "000002_b.down.sql", "DROP TABLE b;")
	if err := db.MigrateDown(schema); err != nil {
		t.Fatalf("MigrateDown(schema) error = %v", err)
	}
	if _, _, ok := pgVersionRow(ctx, t, db.Pool, "schema_migrations"); ok {
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
	if version, _, ok := pgVersionRow(ctx, t, db.Pool, "schema_migrations"); !ok || version != 2 {
		t.Errorf("schema_migrations version = %d, want 2 after applying the schema again", version)
	}
}

// TestPostgresDatabase_DropDatabase drops a database the container or NewPostgresDatabase
// created, and refuses to drop it twice.
func TestPostgresDatabase_DropDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "16")
	if err != nil {
		t.Fatalf("NewPostgresContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name                   string
		viaNewPostgresDatabase bool
		again                  bool
	}{
		{name: "a database the container created is dropped"},
		{name: "a database NewPostgresDatabase created is dropped", viaNewPostgresDatabase: true},
		{name: "dropping a dropped database refuses", again: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				db  *PostgresDatabase
				err error
			)
			if tt.viaNewPostgresDatabase {
				db, err = NewPostgresDatabase(ctx, container.superUsername, container.password, container.host, container.port.Port(), tt.name, container.unprivilegedUsername, SSLModeDisable)
			} else {
				db, err = container.CreateDatabase(ctx, tt.name)
			}
			if err != nil {
				t.Fatalf("creating the database error = %v", err)
			}
			defer db.Close()
			if !pgDatabaseExists(ctx, t, container, db.dbName) {
				t.Fatalf("database %s missing after its creation", db.dbName)
			}

			if err := db.DropDatabase(ctx); err != nil {
				t.Fatalf("DropDatabase() error = %v", err)
			}
			if pgDatabaseExists(ctx, t, container, db.dbName) {
				t.Errorf("database %s exists after DropDatabase()", db.dbName)
			}
			if !tt.again {
				return
			}
			if err := db.DropDatabase(ctx); err == nil {
				t.Error("second DropDatabase() error = nil, want one: the database is gone")
			}
		})
	}
}
