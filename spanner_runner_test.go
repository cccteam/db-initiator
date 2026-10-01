package dbinitiator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-playground/errors/v5"
)

// The fixtures the runner tests apply: a table with a value column, so that a unique index
// or a foreign key over it can fail against seeded rows.
const (
	sqlCreateT        = "CREATE TABLE T (Id INT64, Val INT64) PRIMARY KEY (Id);"
	sqlCreateParent   = "CREATE TABLE Parent (Id INT64) PRIMARY KEY (Id);"
	sqlCreateA        = "CREATE TABLE A (Id INT64) PRIMARY KEY (Id);"
	sqlCreateAChanged = "CREATE TABLE A (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id);"
	sqlCreateB        = "CREATE TABLE B (Id INT64) PRIMARY KEY (Id);"
	sqlCreateC        = "CREATE TABLE C (Id INT64) PRIMARY KEY (Id);"
	sqlUniqueIndexU   = "CREATE UNIQUE INDEX U ON T(Val);"
	sqlForeignKeyFK   = "ALTER TABLE T ADD CONSTRAINT FK FOREIGN KEY (Val) REFERENCES Parent (Id);"
	sqlBadIndex       = "CREATE INDEX Bad ON T(Missing);"
	sqlGoodIndex      = "CREATE INDEX Good ON T(Val);"
)

// versionRow is a version table's one row as the database holds it.
type versionRow struct {
	version    int64
	dirty      bool
	applied    spanner.NullInt64
	checkpoint spanner.NullString
	operation  spanner.NullString
}

// testMigrator creates a database and a migrator on it; both go with the test.
func testMigrator(ctx context.Context, t *testing.T, container *SpannerContainer) (*SpannerDB, *SpannerMigrator) {
	t.Helper()

	dbName := genDBName()
	db, err := container.CreateDatabase(ctx, dbName)
	if err != nil {
		t.Fatalf("SpannerContainer.CreateDatabase() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.DropDatabase(context.Background()); err != nil {
			t.Errorf("SpannerDB.DropDatabase() error = %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("SpannerDB.Close() error = %v", err)
		}
	})

	migrator, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, dbName, container.opts...)
	if err != nil {
		t.Fatalf("NewSpannerMigrator() error = %v", err)
	}
	t.Cleanup(func() {
		if err := migrator.Close(); err != nil {
			t.Errorf("SpannerMigrator.Close() error = %v", err)
		}
	})

	return db, migrator
}

// writeSource writes migration files into a temporary directory and returns its file:// URL.
func writeSource(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, sql := range files {
		writeMigration(t, "file://"+dir, name, sql)
	}

	return "file://" + dir
}

// writeMigration writes, or rewrites, one file of a source.
func writeMigration(t *testing.T, sourceURL, name, sql string) {
	t.Helper()

	dir := strings.TrimPrefix(sourceURL, "file://")
	if err := os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", name, err)
	}
}

// readVersionRow reads a version table's row; ok is false when the table has none.
func readVersionRow(ctx context.Context, t *testing.T, client *spanner.Client, table string) (versionRow, bool) {
	t.Helper()

	iter := client.Single().Query(ctx, spanner.NewStatement("SELECT Version, Dirty, Applied, Checkpoint, Operation FROM `"+table+"`"))
	defer iter.Stop()

	rows := []versionRow{}
	if err := iter.Do(func(row *spanner.Row) error {
		var r versionRow
		if err := row.Columns(&r.version, &r.dirty, &r.applied, &r.checkpoint, &r.operation); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		rows = append(rows, r)

		return nil
	}); err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}
	if len(rows) > 1 {
		t.Fatalf("%s holds %d rows, want at most one", table, len(rows))
	}
	if len(rows) == 0 {
		return versionRow{}, false
	}

	return rows[0], true
}

// writeVersionRow writes a version table's row by hand, as a stopped run or the old library
// would have left it.
func writeVersionRow(ctx context.Context, t *testing.T, client *spanner.Client, table string, r *versionRow) {
	t.Helper()

	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(table, spanner.AllKeys()),
		spanner.Insert(table, []string{"Version", "Dirty", "Applied", "Checkpoint", "Operation"}, []any{r.version, r.dirty, r.applied, r.checkpoint, r.operation}),
	}); err != nil {
		t.Fatalf("writing %s: %v", table, err)
	}
}

// assertClean checks a version table says clean at a version with no progress recorded.
func assertClean(ctx context.Context, t *testing.T, client *spanner.Client, table string, version int64) {
	t.Helper()

	row, ok := readVersionRow(ctx, t, client, table)
	if !ok {
		t.Fatalf("%s has no row, want clean at version %d", table, version)
	}
	if row.version != version || row.dirty || row.applied.Valid || row.checkpoint.Valid || row.operation.Valid {
		t.Errorf("%s = %+v, want clean at version %d with no progress", table, row, version)
	}
}

// assertDirty checks the schema migrations table says dirty at a version with applied
// statements recorded and no operation in flight.
func assertDirty(ctx context.Context, t *testing.T, client *spanner.Client, version, applied int64) {
	t.Helper()

	table := "SchemaMigrations"
	row, ok := readVersionRow(ctx, t, client, table)
	if !ok {
		t.Fatalf("%s has no row, want dirty at version %d", table, version)
	}
	if row.version != version || !row.dirty || !row.applied.Valid || row.applied.Int64 != applied || !row.checkpoint.Valid || len(row.checkpoint.StringVal) != 64 || row.operation.Valid {
		t.Errorf("%s = %+v, want dirty at version %d with %d applied, a checkpoint and no operation", table, row, version, applied)
	}
}

// ddl runs DDL statements on a database directly.
func ddl(ctx context.Context, t *testing.T, db *SpannerDB, stmts ...string) *databasepb.UpdateDatabaseDdlMetadata {
	t.Helper()

	op, err := db.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: db.dbStr, Statements: stmts})
	if err != nil {
		t.Fatalf("DatabaseAdminClient.UpdateDatabaseDdl() error = %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("UpdateDatabaseDdlOperation.Wait() error = %v", err)
	}
	md, err := op.Metadata()
	if err != nil {
		t.Fatalf("UpdateDatabaseDdlOperation.Metadata() error = %v", err)
	}

	return md
}

// insertT seeds T with (Id, Val) pairs.
func insertT(ctx context.Context, t *testing.T, client *spanner.Client, rows ...[2]int64) {
	t.Helper()

	mutations := make([]*spanner.Mutation, 0, len(rows))
	for _, row := range rows {
		mutations = append(mutations, spanner.Insert("T", []string{"Id", "Val"}, []any{row[0], row[1]}))
	}
	if _, err := client.Apply(ctx, mutations); err != nil {
		t.Fatalf("spanner.Client.Apply() error = %v", err)
	}
}

// deleteT removes a row of T.
func deleteT(ctx context.Context, t *testing.T, client *spanner.Client, id int64) {
	t.Helper()

	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.Delete("T", spanner.Key{id})}); err != nil {
		t.Fatalf("spanner.Client.Apply() error = %v", err)
	}
}

// exists reports whether a table, index or constraint of that name exists.
func exists(ctx context.Context, t *testing.T, client *spanner.Client, kind, name string) bool {
	t.Helper()

	queries := map[string]string{
		"table":      "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @name)",
		"index":      "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = '' AND INDEX_NAME = @name)",
		"constraint": "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '' AND CONSTRAINT_NAME = @name)",
	}
	stmt := spanner.Statement{SQL: queries[kind], Params: map[string]any{"name": name}}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Fatalf("checking %s %s: %v", kind, name, err)
	}
	var found bool
	if err := row.Columns(&found); err != nil {
		t.Fatalf("checking %s %s: %v", kind, name, err)
	}

	return found
}

// schemaListing renders the database's tables, columns, indexes and constraints from the
// information schema, so two databases can be compared.
func schemaListing(ctx context.Context, t *testing.T, client *spanner.Client) string {
	t.Helper()

	queries := []string{
		"SELECT CONCAT('column ', TABLE_NAME, '.', COLUMN_NAME, ' ', SPANNER_TYPE, ' ', IS_NULLABLE) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' ORDER BY TABLE_NAME, ORDINAL_POSITION",
		"SELECT CONCAT('index ', TABLE_NAME, '.', INDEX_NAME, ' ', INDEX_TYPE, ' ', CAST(IS_UNIQUE AS STRING)) FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = '' ORDER BY TABLE_NAME, INDEX_NAME",
		"SELECT CONCAT('constraint ', TABLE_NAME, '.', CONSTRAINT_NAME, ' ', CONSTRAINT_TYPE) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '' AND CONSTRAINT_TYPE IN ('FOREIGN KEY', 'PRIMARY KEY') ORDER BY TABLE_NAME, CONSTRAINT_NAME",
	}

	var b strings.Builder
	for _, query := range queries {
		if err := client.Single().Query(ctx, spanner.NewStatement(query)).Do(func(row *spanner.Row) error {
			var line string
			if err := row.Columns(&line); err != nil {
				return errors.Wrap(err, "spanner.Row.Columns()")
			}
			b.WriteString(line)
			b.WriteString("\n")

			return nil
		}); err != nil {
			t.Fatalf("listing the schema: %v", err)
		}
	}

	return b.String()
}

// countT counts T's rows.
func countT(ctx context.Context, t *testing.T, client *spanner.Client) int64 {
	t.Helper()

	iter := client.Single().Query(ctx, spanner.NewStatement("SELECT COUNT(*) FROM T"))
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Fatalf("counting T: %v", err)
	}
	var n int64
	if err := row.Columns(&n); err != nil {
		t.Fatalf("counting T: %v", err)
	}

	return n
}

// TestSpannerMigrator_Lifecycle walks a fresh database through the plain path: every file
// applies in order, a second run is no change, a file added later applies alone, and the
// data table moves on its own.
//
// Deliberately not a table: each step depends on the state the previous one left behind.
func TestSpannerMigrator_Lifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, migrator := testMigrator(ctx, t, container)
	schema := writeSource(t, map[string]string{
		"000001_t.up.sql":      sqlCreateT,
		"000001_t.down.sql":    "DROP TABLE T;",
		"000002_a.up.sql":      "-- the A table\n" + sqlCreateA,
		"000002_a.down.sql":    "DROP TABLE A;",
		"000003_seed.up.sql":   "INSERT INTO T (Id, Val) VALUES (1, 1);\nINSERT INTO T (Id, Val) VALUES (2, 2);",
		"000003_seed.down.sql": "DELETE FROM T WHERE true;",
		"000004_b.up.sql":      sqlCreateB + "\n" + sqlGoodIndex,
	})

	// Every file applies in order.
	if err := migrator.MigrateUpSchema(ctx, schema); err != nil {
		t.Fatalf("MigrateUpSchema() error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 4)
	for _, table := range []string{"T", "A", "B"} {
		if !exists(ctx, t, db.Client, "table", table) {
			t.Errorf("table %s missing after the run", table)
		}
	}
	if n := countT(ctx, t, db.Client); n != 2 {
		t.Errorf("T has %d rows, want 2 from the DML file", n)
	}
	v, err := migrator.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	if v.String() != "version 4" {
		t.Errorf("SchemaVersion() = %q, want %q", v, "version 4")
	}

	// A second run is no change.
	if err := migrator.MigrateUpSchema(ctx, schema); err != nil {
		t.Fatalf("second MigrateUpSchema() error = %v, want nil for no change", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 4)

	// A fifth file applies alone.
	writeMigration(t, schema, "000005_c.up.sql", sqlCreateC)
	if err := migrator.MigrateUpSchema(ctx, schema); err != nil {
		t.Fatalf("third MigrateUpSchema() error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 5)
	if !exists(ctx, t, db.Client, "table", "C") {
		t.Error("table C missing after the fifth file")
	}
	if n := countT(ctx, t, db.Client); n != 2 {
		t.Errorf("T has %d rows after the fifth file, want the seed's 2 untouched", n)
	}

	// The data table moves on its own.
	data := writeSource(t, map[string]string{
		"000001_more.up.sql": "INSERT INTO T (Id, Val) VALUES (3, 3);",
	})
	if err := migrator.MigrateUpData(ctx, data); err != nil {
		t.Fatalf("MigrateUpData() error = %v", err)
	}
	assertClean(ctx, t, db.Client, "DataMigrations", 1)
	assertClean(ctx, t, db.Client, "SchemaMigrations", 5)
	if n := countT(ctx, t, db.Client); n != 3 {
		t.Errorf("T has %d rows after the data migration, want 3", n)
	}
	dv, err := migrator.DataVersion(ctx)
	if err != nil {
		t.Fatalf("DataVersion() error = %v", err)
	}
	if dv.String() != "version 1" {
		t.Errorf("DataVersion() = %q, want %q", dv, "version 1")
	}
}

// TestSpannerMigrator_Takeover runs the new runner on a version table the old library
// created and filled: the progress columns are added and the row's meaning is kept.
func TestSpannerMigrator_Takeover(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	// Files 1 to 3 are never applied: the database says it is at 3 by hand.
	threeFiles := map[string]string{
		"000001_a.up.sql": sqlCreateA,
		"000002_b.up.sql": sqlCreateB,
		"000003_c.up.sql": sqlCreateC,
	}
	fourFiles := map[string]string{
		"000001_a.up.sql": sqlCreateA,
		"000002_b.up.sql": sqlCreateB,
		"000003_c.up.sql": sqlCreateC,
		"000004_d.up.sql": "CREATE TABLE D (Id INT64) PRIMARY KEY (Id);",
	}

	tests := []struct {
		name        string
		data        bool
		dirty       bool
		force       int
		files       map[string]string
		wantErr     string
		wantVersion int64
		wantD       bool
	}{
		{name: "schema clean at 3, three files: no change", files: threeFiles, wantVersion: 3},
		{name: "schema clean at 3, four files: file 4 applies", files: fourFiles, wantVersion: 4, wantD: true},
		{name: "schema dirty at 3 refuses", dirty: true, files: fourFiles, wantErr: "version 3 is dirty from a run that recorded no progress; force the version that matches the database's real state"},
		{name: "schema dirty at 3, forced to 3, file 4 applies", dirty: true, force: 3, files: fourFiles, wantVersion: 4, wantD: true},
		{name: "data clean at 3, three files: no change", data: true, files: threeFiles, wantVersion: 3},
		{name: "data clean at 3, four files: file 4 applies", data: true, files: fourFiles, wantVersion: 4, wantD: true},
		{name: "data dirty at 3 refuses", data: true, dirty: true, files: fourFiles, wantErr: "version 3 is dirty from a run that recorded no progress"},
		{name: "data dirty at 3, forced to 3, file 4 applies", data: true, dirty: true, force: 3, files: fourFiles, wantVersion: 4, wantD: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, migrator := testMigrator(ctx, t, container)
			table := "SchemaMigrations"
			if tt.data {
				table = "DataMigrations"
			}

			// The old library's exact DDL and row.
			ddl(ctx, t, db, "CREATE TABLE "+table+" (\n    Version INT64 NOT NULL,\n    Dirty    BOOL NOT NULL\n\t) PRIMARY KEY(Version)")
			if _, err := db.Apply(ctx, []*spanner.Mutation{spanner.Insert(table, []string{"Version", "Dirty"}, []any{int64(3), tt.dirty})}); err != nil {
				t.Fatalf("spanner.Client.Apply() error = %v", err)
			}

			source := writeSource(t, tt.files)
			up, force := migrator.MigrateUpSchema, migrator.ForceSchema
			if tt.data {
				up, force = migrator.MigrateUpData, migrator.ForceData
			}

			if tt.force > 0 {
				if err := force(ctx, tt.force); err != nil {
					t.Fatalf("Force() error = %v", err)
				}
			}

			err := up(ctx, source)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("MigrateUp() error = %v, want one containing %q", err, tt.wantErr)
				}
				row, ok := readVersionRow(ctx, t, db.Client, table)
				if !ok || row.version != 3 || !row.dirty || row.applied.Valid {
					t.Errorf("%s = %+v, want the old row (3, dirty) untouched with the progress columns added", table, row)
				}

				return
			}
			if err != nil {
				t.Fatalf("MigrateUp() error = %v", err)
			}
			assertClean(ctx, t, db.Client, table, tt.wantVersion)
			if got := exists(ctx, t, db.Client, "table", "D"); got != tt.wantD {
				t.Errorf("table D exists = %v, want %v", got, tt.wantD)
			}
			for _, table := range []string{"A", "B", "C"} {
				if exists(ctx, t, db.Client, "table", table) {
					t.Errorf("table %s exists, but files 1 to 3 were never to run", table)
				}
			}
		})
	}
}

// TestSpannerMigrator_FailureAndResume is the experiment's shape: a batch whose middle
// statement fails against seeded rows stops the file with its progress recorded; a rerun
// without a fix fails the same way; once the rows are fixed the rerun continues from the
// failed statement and ends with the schema a one-go run would have produced.
func TestSpannerMigrator_FailureAndResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name      string
		schema    string
		failing   string
		seed      [][2]int64
		fix       func(ctx context.Context, t *testing.T, client *spanner.Client)
		wantCause string
		wantKind  string
		wantName  string
	}{
		{
			name:    "unique index over duplicate values",
			schema:  sqlCreateT,
			failing: sqlUniqueIndexU,
			seed:    [][2]int64{{1, 7}, {2, 7}},
			fix: func(ctx context.Context, t *testing.T, client *spanner.Client) {
				deleteT(ctx, t, client, 2)
			},
			wantCause: "uniqueness",
			wantKind:  "index",
			wantName:  "U",
		},
		{
			name:    "foreign key over rows with no parent",
			schema:  sqlCreateT + "\n" + sqlCreateParent,
			failing: sqlForeignKeyFK,
			seed:    [][2]int64{{1, 7}},
			fix: func(ctx context.Context, t *testing.T, client *spanner.Client) {
				if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert("Parent", []string{"Id"}, []any{int64(7)})}); err != nil {
					t.Fatalf("spanner.Client.Apply() error = %v", err)
				}
			},
			wantCause: "oreign key",
			wantKind:  "constraint",
			wantName:  "FK",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files := map[string]string{
				"000001_t.up.sql":     tt.schema,
				"000002_refit.up.sql": sqlCreateA + "\n" + tt.failing + "\n" + sqlCreateB,
			}

			db, migrator := testMigrator(ctx, t, container)
			source := writeSource(t, files)
			writeMigration(t, source, "000002_refit.up.sql", "")
			if err := migrator.MigrateUpSchema(ctx, writeSource(t, map[string]string{"000001_t.up.sql": tt.schema})); err != nil {
				t.Fatalf("MigrateUpSchema(file 1) error = %v", err)
			}
			writeMigration(t, source, "000002_refit.up.sql", files["000002_refit.up.sql"])
			insertT(ctx, t, db.Client, tt.seed...)

			// The failure: statement 2 of 3, one applied.
			err := migrator.MigrateUpSchema(ctx, source)
			var dirty *DirtyError
			if !errors.As(err, &dirty) {
				t.Fatalf("MigrateUpSchema() error = %v, want a *DirtyError", err)
			}
			if dirty.Version != 2 || dirty.File != "000002_refit.up.sql" || dirty.Applied != 1 || dirty.Total != 3 || dirty.RolledBack != 0 {
				t.Errorf("DirtyError = %+v, want version 2, file 000002_refit.up.sql, 1 of 3 applied", dirty)
			}
			for _, want := range []string{"000002_refit.up.sql stopped at statement 2 of 3", strings.TrimSuffix(tt.failing, ";"), tt.wantCause, "continues from statement 2"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
			assertDirty(ctx, t, db.Client, 2, 1)
			if !exists(ctx, t, db.Client, "table", "A") {
				t.Error("table A missing: statement 1 should have applied")
			}
			if exists(ctx, t, db.Client, tt.wantKind, tt.wantName) {
				t.Errorf("%s %s exists: statement 2 should have failed", tt.wantKind, tt.wantName)
			}
			if exists(ctx, t, db.Client, "table", "B") {
				t.Error("table B exists: statement 3 should not have run")
			}

			// A rerun without a fix fails the same way and the progress stays.
			err = migrator.MigrateUpSchema(ctx, source)
			if !errors.As(err, &dirty) || dirty.Applied != 1 {
				t.Fatalf("rerun MigrateUpSchema() error = %v, want the same *DirtyError with 1 applied", err)
			}
			assertDirty(ctx, t, db.Client, 2, 1)

			// Fixed, the rerun continues from statement 2.
			tt.fix(ctx, t, db.Client)
			if err := migrator.MigrateUpSchema(ctx, source); err != nil {
				t.Fatalf("MigrateUpSchema() after the fix error = %v", err)
			}
			assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
			if !exists(ctx, t, db.Client, tt.wantKind, tt.wantName) || !exists(ctx, t, db.Client, "table", "B") {
				t.Errorf("%s %s and table B should exist after the resume", tt.wantKind, tt.wantName)
			}

			// The schema equals that of a database that applied the files in one go.
			oneGo, oneGoMigrator := testMigrator(ctx, t, container)
			if err := oneGoMigrator.MigrateUpSchema(ctx, source); err != nil {
				t.Fatalf("one-go MigrateUpSchema() error = %v", err)
			}
			if got, want := schemaListing(ctx, t, db.Client), schemaListing(ctx, t, oneGo.Client); got != want {
				t.Errorf("schema after the resume differs from the one-go schema:\n%s\nwant\n%s", got, want)
			}
		})
	}
}

// TestSpannerMigrator_Refusals covers the ways a run stops with nothing resumable, or with
// a refusal: a batch Spanner refuses on validation, a DML group that rolls back, a changed
// applied part, and a dirty or clean version the build does not carry.
func TestSpannerMigrator_Refusals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, container *SpannerContainer)
	}{
		{name: "synchronous validation error applies nothing and the corrected file applies", run: testSynchronousRefusal},
		{name: "a DML group that fails rolls back whole", run: testDMLRollback},
		{name: "a changed applied part refuses until the file is restored", run: testChangedAppliedPart},
		{name: "a dirty version the build does not carry", run: testNotCarried(true)},
		{name: "a clean version the build does not carry", run: testNotCarried(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(ctx, t, container)
		})
	}
}

// testSynchronousRefusal: a batch Spanner refuses on validation applies nothing; the applied
// part being empty, the file may change entirely and the rerun applies it.
func testSynchronousRefusal(ctx context.Context, t *testing.T, container *SpannerContainer) {
	t.Helper()

	db, migrator := testMigrator(ctx, t, container)
	source := writeSource(t, map[string]string{
		"000001_t.up.sql":   sqlCreateT,
		"000002_bad.up.sql": sqlCreateA + "\n" + sqlBadIndex,
	})

	err := migrator.MigrateUpSchema(ctx, source)
	var dirty *DirtyError
	if !errors.As(err, &dirty) {
		t.Fatalf("MigrateUpSchema() error = %v, want a *DirtyError", err)
	}
	if dirty.Applied != 0 || dirty.Total != 2 || !strings.Contains(err.Error(), "stopped at statement 1 of 2") || !strings.Contains(err.Error(), "nothing applied") {
		t.Errorf("DirtyError = %v, want statement 1 of 2 with nothing applied", err)
	}
	assertDirty(ctx, t, db.Client, 2, 0)
	if exists(ctx, t, db.Client, "table", "A") {
		t.Error("table A exists: a refused batch applies nothing")
	}

	writeMigration(t, source, "000002_bad.up.sql", sqlCreateA+"\n"+sqlGoodIndex)
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema() after the correction error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
	if !exists(ctx, t, db.Client, "table", "A") || !exists(ctx, t, db.Client, "index", "Good") {
		t.Error("table A and index Good should exist after the corrected file applied")
	}
}

// testDMLRollback: a DML group that fails applies nothing, names the failed statement, and
// applies after the fix.
func testDMLRollback(ctx context.Context, t *testing.T, container *SpannerContainer) {
	t.Helper()

	db, migrator := testMigrator(ctx, t, container)
	source := writeSource(t, map[string]string{
		"000001_t.up.sql":    sqlCreateT,
		"000002_seed.up.sql": "INSERT INTO T (Id, Val) VALUES (1, 1);\nINSERT INTO T (Id, Val) VALUES (1, 2);\nINSERT INTO T (Id, Val) VALUES (3, 3);",
	})

	err := migrator.MigrateUpSchema(ctx, source)
	var dirty *DirtyError
	if !errors.As(err, &dirty) {
		t.Fatalf("MigrateUpSchema() error = %v, want a *DirtyError", err)
	}
	if dirty.Applied != 0 || dirty.Total != 3 || dirty.RolledBack != 1 || dirty.Statement != "INSERT INTO T (Id, Val) VALUES (1, 2)" {
		t.Errorf("DirtyError = %+v, want statement 2 named, 0 applied, 1 rolled back", dirty)
	}
	for _, want := range []string{"stopped at statement 2 of 3", "rolled back", "continues from statement 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	assertDirty(ctx, t, db.Client, 2, 0)
	if n := countT(ctx, t, db.Client); n != 0 {
		t.Errorf("T has %d rows, want 0: the group rolled back", n)
	}

	writeMigration(t, source, "000002_seed.up.sql", "INSERT INTO T (Id, Val) VALUES (1, 1);\nINSERT INTO T (Id, Val) VALUES (2, 2);\nINSERT INTO T (Id, Val) VALUES (3, 3);")
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema() after the fix error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
	if n := countT(ctx, t, db.Client); n != 3 {
		t.Errorf("T has %d rows, want 3", n)
	}
}

// testChangedAppliedPart: after a failure at statement 2, a change to statement 1 refuses
// until the file is restored.
func testChangedAppliedPart(ctx context.Context, t *testing.T, container *SpannerContainer) {
	t.Helper()

	db, migrator := testMigrator(ctx, t, container)
	refit := sqlCreateA + "\n" + sqlUniqueIndexU + "\n" + sqlCreateB
	source := writeSource(t, map[string]string{"000001_t.up.sql": sqlCreateT})
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema(file 1) error = %v", err)
	}
	insertT(ctx, t, db.Client, [2]int64{1, 7}, [2]int64{2, 7})
	writeMigration(t, source, "000002_refit.up.sql", refit)

	var dirty *DirtyError
	if err := migrator.MigrateUpSchema(ctx, source); !errors.As(err, &dirty) || dirty.Applied != 1 {
		t.Fatalf("MigrateUpSchema() error = %v, want a *DirtyError with 1 applied", err)
	}

	writeMigration(t, source, "000002_refit.up.sql", sqlCreateAChanged+"\n"+sqlUniqueIndexU+"\n"+sqlCreateB)
	err := migrator.MigrateUpSchema(ctx, source)
	if err == nil || !strings.Contains(err.Error(), "000002_refit.up.sql changed in the part already applied (1 statement); restore the file or force a version") {
		t.Fatalf("MigrateUpSchema() with the changed file error = %v, want the changed-part refusal", err)
	}
	assertDirty(ctx, t, db.Client, 2, 1)

	writeMigration(t, source, "000002_refit.up.sql", refit)
	deleteT(ctx, t, db.Client, 2)
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema() with the file restored error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
	if !exists(ctx, t, db.Client, "index", "U") || !exists(ctx, t, db.Client, "table", "B") {
		t.Error("index U and table B should exist after the resume")
	}
}

// testNotCarried: a version the source does not carry refuses, dirty or clean, and the
// row is left alone.
func testNotCarried(dirty bool) func(ctx context.Context, t *testing.T, container *SpannerContainer) {
	return func(ctx context.Context, t *testing.T, container *SpannerContainer) {
		t.Helper()

		db, migrator := testMigrator(ctx, t, container)
		source := writeSource(t, map[string]string{"000001_t.up.sql": sqlCreateT})
		if err := migrator.MigrateUpSchema(ctx, source); err != nil {
			t.Fatalf("MigrateUpSchema() error = %v", err)
		}
		writeVersionRow(ctx, t, db.Client, "SchemaMigrations", &versionRow{
			version:    41,
			dirty:      dirty,
			applied:    spanner.NullInt64{Int64: 1, Valid: dirty},
			checkpoint: spanner.NullString{StringVal: strings.Repeat("0", 64), Valid: dirty},
		})

		err := migrator.MigrateUpSchema(ctx, source)
		if err == nil || !strings.Contains(err.Error(), "the database is at version 41, which this build does not carry") {
			t.Fatalf("MigrateUpSchema() error = %v, want the does-not-carry refusal", err)
		}
		row, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations")
		if !ok || row.version != 41 || row.dirty != dirty {
			t.Errorf("SchemaMigrations = %+v, want the row at 41 untouched", row)
		}
	}
}

// TestSpannerMigrator_InterruptedRun resolves a DDL operation a dead run left in flight:
// the test sends the batch itself and writes the row with the operation's name.
func TestSpannerMigrator_InterruptedRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name        string
		seed        [][2]int64
		batch       []string
		unknown     bool
		wantErr     string
		wantApplied int64
		wantClean   bool
	}{
		{
			name:      "a succeeded operation counts its whole batch",
			batch:     []string{strings.TrimSuffix(sqlCreateA, ";"), strings.TrimSuffix(sqlCreateB, ";")},
			wantClean: true,
		},
		{
			name:        "a failed operation counts its commit timestamps",
			seed:        [][2]int64{{1, 7}, {2, 7}},
			batch:       []string{strings.TrimSuffix(sqlCreateA, ";"), strings.TrimSuffix(sqlUniqueIndexU, ";"), strings.TrimSuffix(sqlCreateB, ";")},
			wantErr:     "stopped at statement 2 of 3",
			wantApplied: 1,
		},
		{
			name:    "an unknown operation name refuses",
			batch:   []string{strings.TrimSuffix(sqlCreateA, ";"), strings.TrimSuffix(sqlCreateB, ";")},
			unknown: true,
			wantErr: "cannot be fetched; force the version that matches the database's real state",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, migrator := testMigrator(ctx, t, container)
			source := writeSource(t, map[string]string{"000001_t.up.sql": sqlCreateT})
			if err := migrator.MigrateUpSchema(ctx, source); err != nil {
				t.Fatalf("MigrateUpSchema(file 1) error = %v", err)
			}
			insertT(ctx, t, db.Client, tt.seed...)
			writeMigration(t, source, "000002_refit.up.sql", strings.Join(tt.batch, ";\n")+";")

			// The batch, as a run that died while waiting would have sent it.
			name := db.dbStr + "/operations/00000000-0000-0000-0000-000000000000"
			if !tt.unknown {
				op, err := db.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: db.dbStr, Statements: tt.batch})
				if err != nil {
					t.Fatalf("DatabaseAdminClient.UpdateDatabaseDdl() error = %v", err)
				}
				_ = op.Wait(ctx)
				name = op.Name()
			}
			writeVersionRow(ctx, t, db.Client, "SchemaMigrations", &versionRow{
				version:    2,
				dirty:      true,
				applied:    spanner.NullInt64{Int64: 0, Valid: true},
				checkpoint: spanner.NullString{StringVal: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Valid: true},
				operation:  spanner.NullString{StringVal: name, Valid: true},
			})

			err := migrator.MigrateUpSchema(ctx, source)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("MigrateUpSchema() error = %v, want one containing %q", err, tt.wantErr)
				}
				row, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations")
				if !ok {
					t.Fatal("SchemaMigrations has no row")
				}
				if tt.unknown {
					if !row.operation.Valid || row.operation.StringVal != name {
						t.Errorf("SchemaMigrations = %+v, want the unknown operation kept on the row", row)
					}

					return
				}
				assertDirty(ctx, t, db.Client, 2, tt.wantApplied)

				return
			}
			if err != nil {
				t.Fatalf("MigrateUpSchema() error = %v", err)
			}
			assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
			if !exists(ctx, t, db.Client, "table", "A") || !exists(ctx, t, db.Client, "table", "B") {
				t.Error("tables A and B should exist")
			}
		})
	}
}

// TestSpannerMigrator_VersionAndForce reads and forces both tables.
func TestSpannerMigrator_VersionAndForce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
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

			db, migrator := testMigrator(ctx, t, container)
			version, force := migrator.SchemaVersion, migrator.ForceSchema
			table, other := "SchemaMigrations", "DataMigrations"
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
			if tt.force >= 0 {
				assertClean(ctx, t, db.Client, table, int64(tt.force))
			} else if _, ok := readVersionRow(ctx, t, db.Client, table); ok {
				t.Errorf("%s has a row after Force(-1), want none", table)
			}
			if exists(ctx, t, db.Client, "table", other) {
				t.Errorf("%s exists, but only %s was touched", other, table)
			}
		})
	}
}

// TestSpannerDB_SourcesAndDown applies two sources, each from its own first version, walks
// the fixtures' down file, forces the version to the schema's last and walks its down files
// back to no version.
//
// Deliberately not a table: each step depends on the state the previous one left behind.
func TestSpannerDB_SourcesAndDown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, migrator := testMigrator(ctx, t, container)
	schema := writeSource(t, map[string]string{
		"000001_t.up.sql":   sqlCreateT,
		"000001_t.down.sql": "DROP TABLE T;",
		"000002_a.up.sql":   sqlCreateA,
		"000002_a.down.sql": "DROP TABLE A;",
	})
	fixtures := writeSource(t, map[string]string{
		"000001_rows.up.sql":   "INSERT INTO T (Id, Val) VALUES (1, 1);\nINSERT INTO T (Id, Val) VALUES (2, 2);",
		"000001_rows.down.sql": "DELETE FROM T WHERE true;",
	})

	// Each source applies from its own first version; the row ends at the second source's.
	if err := db.MigrateUp(schema, fixtures); err != nil {
		t.Fatalf("MigrateUp() error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 1)
	if !exists(ctx, t, db.Client, "table", "A") {
		t.Error("table A missing: the first source should have applied before the second")
	}
	if n := countT(ctx, t, db.Client); n != 2 {
		t.Errorf("T has %d rows, want the fixtures' 2", n)
	}

	// The fixtures' down file empties T and leaves no version.
	if err := db.MigrateDown(fixtures); err != nil {
		t.Fatalf("MigrateDown(fixtures) error = %v", err)
	}
	if _, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations"); ok {
		t.Error("SchemaMigrations has a row after MigrateDown, want none")
	}
	if n := countT(ctx, t, db.Client); n != 0 {
		t.Errorf("T has %d rows after the fixtures' down, want 0", n)
	}

	// Forced to the schema's last version, its down files walk back to no version.
	if err := migrator.ForceSchema(ctx, 2); err != nil {
		t.Fatalf("ForceSchema(2) error = %v", err)
	}
	if err := db.MigrateDown(schema); err != nil {
		t.Fatalf("MigrateDown(schema) error = %v", err)
	}
	if _, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations"); ok {
		t.Error("SchemaMigrations has a row after MigrateDown, want none")
	}
	for _, table := range []string{"T", "A"} {
		if exists(ctx, t, db.Client, "table", table) {
			t.Errorf("table %s exists after MigrateDown", table)
		}
	}

	// Down on no version is no change, and the schema applies again from the start.
	if err := db.MigrateDown(schema); err != nil {
		t.Fatalf("MigrateDown() on no version error = %v, want nil", err)
	}
	if err := db.MigrateUp(schema); err != nil {
		t.Fatalf("MigrateUp(schema) again error = %v", err)
	}
	assertClean(ctx, t, db.Client, "SchemaMigrations", 2)
}

// TestSpannerMigrator_DownRefusals: a dirty version refuses to migrate down, and a failing
// down file leaves the version dirty with no progress.
func TestSpannerMigrator_DownRefusals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	tests := []struct {
		name      string
		dirty     bool
		down      string
		wantErr   string
		wantDirty bool
	}{
		{name: "a dirty version refuses", dirty: true, down: "DROP TABLE T;", wantErr: "force the version that matches its real state before migrating down", wantDirty: true},
		{name: "a failing down file leaves the version dirty", down: "DROP TABLE Nope;", wantErr: "000001_t.down.sql failed; the database is dirty at version 1", wantDirty: true},
		{name: "a good down file reverts", down: "DROP TABLE T;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, _ := testMigrator(ctx, t, container)
			source := writeSource(t, map[string]string{
				"000001_t.up.sql":   sqlCreateT,
				"000001_t.down.sql": tt.down,
			})
			if err := db.MigrateUp(source); err != nil {
				t.Fatalf("MigrateUp() error = %v", err)
			}
			if tt.dirty {
				writeVersionRow(ctx, t, db.Client, "SchemaMigrations", &versionRow{version: 1, dirty: true})
			}

			err := db.MigrateDown(source)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("MigrateDown() error = %v, want one containing %q", err, tt.wantErr)
				}
				row, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations")
				if !ok || row.version != 1 || row.dirty != tt.wantDirty || row.applied.Valid {
					t.Errorf("SchemaMigrations = %+v, want version 1 dirty with no progress", row)
				}

				return
			}
			if err != nil {
				t.Fatalf("MigrateDown() error = %v", err)
			}
			if _, ok := readVersionRow(ctx, t, db.Client, "SchemaMigrations"); ok {
				t.Error("SchemaMigrations has a row after MigrateDown, want none")
			}
		})
	}
}

// TestSpannerMigrator_DirtyErrorPrints pins the whole sentence a failed file prints.
func TestSpannerMigrator_DirtyErrorPrints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	db, migrator := testMigrator(ctx, t, container)
	source := writeSource(t, map[string]string{"000001_t.up.sql": sqlCreateT})
	if err := migrator.MigrateUpSchema(ctx, source); err != nil {
		t.Fatalf("MigrateUpSchema(file 1) error = %v", err)
	}
	insertT(ctx, t, db.Client, [2]int64{1, 7}, [2]int64{2, 7})
	writeMigration(t, source, "000041_Refits.up.sql", sqlCreateA+"\n"+sqlUniqueIndexU+"\n"+sqlCreateB)

	err = migrator.MigrateUpSchema(ctx, source)
	var dirty *DirtyError
	if !errors.As(err, &dirty) {
		t.Fatalf("MigrateUpSchema() error = %v, want a *DirtyError", err)
	}
	want := fmt.Sprintf("000041_Refits.up.sql stopped at statement 2 of 3 (CREATE UNIQUE INDEX U ON T(Val)): %v; fix the cause and rerun, which continues from statement 2, or force a version", dirty.Cause)
	if got := dirty.Error(); got != want {
		t.Errorf("DirtyError.Error() =\n%s\nwant\n%s", got, want)
	}
	if !strings.Contains(dirty.Cause.Error(), "niqueness") {
		t.Errorf("DirtyError.Cause = %v, want Spanner's uniqueness violation", dirty.Cause)
	}
	assertDirty(ctx, t, db.Client, 41, 1)
	v, err := migrator.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	if v.String() != "version 41, dirty: 1 statement applied" {
		t.Errorf("SchemaVersion() = %q, want %q", v, "version 41, dirty: 1 statement applied")
	}
}
