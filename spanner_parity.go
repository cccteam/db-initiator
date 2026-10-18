package dbinitiator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/option"
)

// parityDifferenceLimit is how many row differences one table reports before the rest are
// counted: enough to see the pattern, not a dump of the table.
const parityDifferenceLimit = 20

// paramTable names the table parameter of the information schema queries.
const paramTable = "table"

// parityNameLetters are the letters a throwaway database's name is drawn from.
const parityNameLetters = "abcdefghijklmnopqrstuvwxyz"

// ParitySource is one side of a parity check: the schema migrations to apply and, when
// the application has them, the data migrations, each a source URL as [SpannerMigrator]
// takes it (file://<directory>). An empty Data applies no data migrations.
type ParitySource struct {
	Schema string
	Data   string
}

// ParityReport is what a parity check found: the differences between the two databases,
// each one sentence, and none when they are identical.
type ParityReport struct {
	Differences []string
}

// Identical reports whether the two databases came out the same.
func (r *ParityReport) Identical() bool {
	return len(r.Differences) == 0
}

// String is the differences, one per line, or "identical".
func (r *ParityReport) String() string {
	if r.Identical() {
		return "identical"
	}

	return strings.Join(r.Differences, "\n")
}

// SpannerParity applies two migration sources to two fresh databases on the instance and
// compares what results: the schema as the database reports it (every DDL statement of
// GetDatabaseDdl, order aside, so a fold tidied by hand still passes) and every table's
// rows as values, ordered by primary key. Two things are left out on purpose: the version
// tables, which record how the schema was reached and are the one thing meant to differ,
// and columns that take commit timestamps (allow_commit_timestamp), whose values are the
// time of each run. Both databases are dropped when the check ends, whatever its result.
//
// It is the check a consolidated migration sequence is accepted by: the branch's sequence
// on one side, the folded file on the other, applied to the emulator in CI or wherever an
// instance can hold two throwaway databases.
func SpannerParity(ctx context.Context, projectID, instanceID string, a, b ParitySource, opts ...option.ClientOption) (*ParityReport, error) {
	left, err := snapshotParity(ctx, projectID, instanceID, "a", a, opts)
	if err != nil {
		return nil, errors.Wrap(err, "the first source")
	}
	right, err := snapshotParity(ctx, projectID, instanceID, "b", b, opts)
	if err != nil {
		return nil, errors.Wrap(err, "the second source")
	}

	return &ParityReport{Differences: compareParity(left, right)}, nil
}

// Parity is [SpannerParity] on the container's instance.
func (sc *SpannerContainer) Parity(ctx context.Context, a, b ParitySource) (*ParityReport, error) {
	report, err := SpannerParity(ctx, sc.projectID, sc.instanceID, a, b, sc.opts...)
	if err != nil {
		return nil, errors.Wrap(err, "SpannerParity()")
	}

	return report, nil
}

// paritySnapshot is one side of the check once its migrations ran: the schema's DDL
// statements and every table's rows.
type paritySnapshot struct {
	ddl    []string
	tables map[string]*parityTable
}

// parityTable is one table's rows as values, ordered by primary key, with the columns
// the values are read from.
type parityTable struct {
	columns []string
	rows    []parityRow
}

// parityRow is one row: its key, for the report, and its values rendered as JSON.
type parityRow struct {
	key    string
	values []string
}

// snapshotParity creates a database, applies the source to it and reads the schema and the
// rows back; the database is dropped before it returns.
func snapshotParity(ctx context.Context, projectID, instanceID, side string, src ParitySource, opts []option.ClientOption) (_ *paritySnapshot, err error) {
	if src.Schema == "" {
		return nil, errors.New("a parity source names no schema migrations")
	}
	name, err := parityDatabaseName(side)
	if err != nil {
		return nil, err
	}
	db, err := NewSpannerDatabase(ctx, projectID, instanceID, name, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "NewSpannerDatabase()")
	}
	defer func() {
		if dropErr := db.DropDatabase(ctx); dropErr != nil && err == nil {
			err = errors.Wrap(dropErr, "SpannerDB.DropDatabase()")
		}
		if closeErr := db.Close(); closeErr != nil && err == nil {
			err = errors.Wrap(closeErr, "SpannerDB.Close()")
		}
	}()

	migrator, err := NewSpannerMigrator(ctx, projectID, instanceID, name, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "NewSpannerMigrator()")
	}
	defer func() {
		if closeErr := migrator.Close(); closeErr != nil && err == nil {
			err = errors.Wrap(closeErr, "SpannerMigrator.Close()")
		}
	}()
	if err := migrator.MigrateUpSchema(ctx, src.Schema); err != nil {
		return nil, errors.Wrap(err, "SpannerMigrator.MigrateUpSchema()")
	}
	if src.Data != "" {
		if err := migrator.MigrateUpData(ctx, src.Data); err != nil {
			return nil, errors.Wrap(err, "SpannerMigrator.MigrateUpData()")
		}
	}

	ddl, err := db.admin.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{Database: db.dbStr})
	if err != nil {
		return nil, errors.Wrap(err, "database.DatabaseAdminClient.GetDatabaseDdl()")
	}
	statements := make([]string, 0, len(ddl.GetStatements()))
	for _, s := range ddl.GetStatements() {
		statements = append(statements, strings.TrimSpace(s))
	}
	sort.Strings(statements)

	tables, err := parityTables(ctx, db.Client, []string{migrator.schemaMigrationsTable, migrator.dataMigrationsTable})
	if err != nil {
		return nil, err
	}

	return &paritySnapshot{ddl: statements, tables: tables}, nil
}

// parityDatabaseName names a throwaway database for one side: parity-<side>-<ten letters>.
func parityDatabaseName(side string) (string, error) {
	var b strings.Builder
	b.WriteString("parity-" + side + "-")
	for range 10 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(parityNameLetters))))
		if err != nil {
			return "", errors.Wrap(err, "rand.Int()")
		}
		b.WriteByte(parityNameLetters[n.Int64()])
	}

	return b.String(), nil
}

// parityTables reads every base table's rows as values, the version tables and the
// commit-timestamp columns left out.
func parityTables(ctx context.Context, client *spanner.Client, versionTables []string) (map[string]*parityTable, error) {
	names, err := parityTableNames(ctx, client)
	if err != nil {
		return nil, err
	}
	skipped, err := commitTimestampColumns(ctx, client)
	if err != nil {
		return nil, err
	}

	tables := make(map[string]*parityTable, len(names))
	for _, name := range names {
		if slices.Contains(versionTables, name) {
			continue
		}
		table, err := readParityTable(ctx, client, name, skipped[name])
		if err != nil {
			return nil, err
		}
		tables[name] = table
	}

	return tables, nil
}

// parityTableNames lists the database's base tables.
func parityTableNames(ctx context.Context, client *spanner.Client) ([]string, error) {
	stmt := spanner.NewStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
	var names []string
	if err := client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		names = append(names, name)

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "listing the tables")
	}

	return names, nil
}

// commitTimestampColumns lists, per table, the columns that take commit timestamps.
func commitTimestampColumns(ctx context.Context, client *spanner.Client) (map[string][]string, error) {
	stmt := spanner.NewStatement("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMN_OPTIONS " +
		"WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND OPTION_NAME = 'allow_commit_timestamp' AND OPTION_VALUE = 'TRUE'")
	skipped := map[string][]string{}
	if err := client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var table, column string
		if err := row.Columns(&table, &column); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		skipped[table] = append(skipped[table], column)

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "listing the commit-timestamp columns")
	}

	return skipped, nil
}

// readParityTable reads one table's rows, ordered by its primary key, without the skipped
// columns.
func readParityTable(ctx context.Context, client *spanner.Client, table string, skipped []string) (*parityTable, error) {
	columns, keys, err := parityColumns(ctx, client, table)
	if err != nil {
		return nil, err
	}
	columns = slices.DeleteFunc(columns, func(c string) bool { return slices.Contains(skipped, c) })
	if len(columns) == 0 {
		return &parityTable{}, nil
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = "`" + c + "`"
	}
	order := ""
	if len(keys) > 0 {
		order = " ORDER BY `" + strings.Join(keys, "`, `") + "`"
	}
	stmt := spanner.NewStatement("SELECT " + strings.Join(quoted, ", ") + " FROM `" + table + "`" + order)

	result := &parityTable{columns: columns}
	if err := client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		rendered, err := renderParityRow(row, columns, keys)
		if err != nil {
			return err
		}
		result.rows = append(result.rows, rendered)

		return nil
	}); err != nil {
		return nil, errors.Wrapf(err, "reading the rows of %s", table)
	}

	return result, nil
}

// parityColumns lists a table's columns in ordinal order and its primary key columns in
// key order.
func parityColumns(ctx context.Context, client *spanner.Client, table string) (columns, keys []string, err error) {
	stmt := spanner.Statement{
		SQL:    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION",
		Params: map[string]any{paramTable: table},
	}
	if err := client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		columns = append(columns, name)

		return nil
	}); err != nil {
		return nil, nil, errors.Wrapf(err, "reading the columns of %s", table)
	}
	stmt = spanner.Statement{
		SQL: "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' " +
			"AND TABLE_NAME = @table AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION",
		Params: map[string]any{paramTable: table},
	}
	if err := client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		keys = append(keys, name)

		return nil
	}); err != nil {
		return nil, nil, errors.Wrapf(err, "reading the primary key of %s", table)
	}

	return columns, keys, nil
}

// renderParityRow renders a row's values as JSON, one string per column, and its key as
// the key columns' values.
func renderParityRow(row *spanner.Row, columns, keys []string) (parityRow, error) {
	values := make([]string, len(columns))
	byColumn := make(map[string]string, len(columns))
	for i, column := range columns {
		var v spanner.GenericColumnValue
		if err := row.Column(i, &v); err != nil {
			return parityRow{}, errors.Wrapf(err, "reading column %s", column)
		}
		rendered, err := json.Marshal(v.Value.AsInterface())
		if err != nil {
			return parityRow{}, errors.Wrapf(err, "rendering column %s", column)
		}
		values[i] = string(rendered)
		byColumn[column] = values[i]
	}
	keyValues := make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := byColumn[k]; ok {
			keyValues = append(keyValues, v)
		}
	}

	return parityRow{key: strings.Join(keyValues, ", "), values: values}, nil
}

// compareParity lists the differences between two snapshots: the DDL statements one side
// has and the other lacks, the tables one side has, and the rows that differ, table by
// table.
func compareParity(a, b *paritySnapshot) []string {
	var out []string
	for _, s := range a.ddl {
		if !slices.Contains(b.ddl, s) {
			out = append(out, "schema: only the first side has: "+s)
		}
	}
	for _, s := range b.ddl {
		if !slices.Contains(a.ddl, s) {
			out = append(out, "schema: only the second side has: "+s)
		}
	}

	names := map[string]bool{}
	for n := range a.tables {
		names[n] = true
	}
	for n := range b.tables {
		names[n] = true
	}
	for _, name := range slices.Sorted(mapsKeys(names)) {
		left, right := a.tables[name], b.tables[name]
		switch {
		case left == nil:
			out = append(out, fmt.Sprintf("table %s: only the second side has it", name))
		case right == nil:
			out = append(out, fmt.Sprintf("table %s: only the first side has it", name))
		default:
			out = append(out, compareParityTable(name, left, right)...)
		}
	}

	return out
}

// mapsKeys returns the keys of a set.
func mapsKeys(set map[string]bool) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		for k := range set {
			if !yield(k) {
				return
			}
		}
	}
}

// compareParityTable lists one table's differences: the columns read, the row count, then
// the rows in key order, column by column, up to the limit.
func compareParityTable(name string, a, b *parityTable) []string {
	if !slices.Equal(a.columns, b.columns) {
		return []string{fmt.Sprintf("table %s: columns compared differ: %v and %v", name, a.columns, b.columns)}
	}
	var out []string
	if len(a.rows) != len(b.rows) {
		out = append(out, fmt.Sprintf("table %s: %d rows and %d rows", name, len(a.rows), len(b.rows)))
	}
	reported := 0
	for i := range min(len(a.rows), len(b.rows)) {
		left, right := a.rows[i], b.rows[i]
		for c := range a.columns {
			if left.values[c] == right.values[c] {
				continue
			}
			reported++
			if reported > parityDifferenceLimit {
				continue
			}
			out = append(out, fmt.Sprintf("table %s, row (%s), column %s: %s and %s", name, left.key, a.columns[c], left.values[c], right.values[c]))
		}
	}
	if reported > parityDifferenceLimit {
		out = append(out, fmt.Sprintf("table %s: %d more differing values not listed", name, reported-parityDifferenceLimit))
	}

	return out
}
