package runner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	ccclogger "github.com/cccteam/logger"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/iterator"
)

// Spanner applies migrations to one Spanner database and keeps one version table. A DDL
// group goes up as one batch, which Spanner validates whole before applying anything and
// then applies in order; a DML group runs in one read-write transaction. The version row
// records how far a file got, so a failed file resumes from its failed statement.
type Spanner struct {
	admin    *database.DatabaseAdminClient
	client   *spanner.Client
	database string
	table    string
}

// NewSpanner returns a runner for the database at databasePath
// (projects/<p>/instances/<i>/databases/<d>) keeping its versions in table.
func NewSpanner(admin *database.DatabaseAdminClient, client *spanner.Client, databasePath, table string) *Spanner {
	return &Spanner{
		admin:    admin,
		client:   client,
		database: databasePath,
		table:    table,
	}
}

// The version table's columns. Version and Dirty are the old library's; the other three
// record the progress of a file that stopped, and are NULL when no file is in progress.
const (
	columnVersion    = "Version"
	columnDirty      = "Dirty"
	columnApplied    = "Applied"
	columnCheckpoint = "Checkpoint"
	columnOperation  = "Operation"
)

// Up applies every pending migration of the source, ascending. A dirty version resumes
// from its failed statement when the runner recorded progress and the file's applied part
// is unchanged; otherwise the run refuses and says how to recover. No pending migration
// is a normal result.
func (s *Spanner) Up(ctx context.Context, src *Source) error {
	if err := s.ensureTable(ctx); err != nil {
		return err
	}

	v, err := s.read(ctx)
	if err != nil {
		return err
	}

	current, resumed := -1, false
	if v.Known {
		current = v.Number
		switch {
		case v.Dirty:
			if err := s.resume(ctx, src, v); err != nil {
				return err
			}
			resumed = true
		case !src.Has(current):
			return errors.Newf("the database is at version %d, which this build does not carry", current)
		}
	}

	pending := src.Pending(current)
	if len(pending) == 0 && !resumed {
		ccclogger.FromCtx(ctx).Infof("No change: the database is at %s and the source has nothing above it", v)

		return nil
	}

	for _, m := range pending {
		stmts, err := Split(m.SQL)
		if err != nil {
			return errors.Wrapf(err, "splitting %s", m.File)
		}
		if err := s.apply(ctx, m, stmts, 0); err != nil {
			return err
		}
	}

	return nil
}

// resume continues the dirty version's file: an in-flight DDL operation is resolved into
// applied statements first, then the file's applied part is checked against the checkpoint.
func (s *Spanner) resume(ctx context.Context, src *Source, v Version) error {
	file, ok := src.Up(v.Number)
	if !ok {
		_, err := Resume(v, nil, nil)

		return err
	}

	stmts, err := Split(file.SQL)
	if err != nil {
		return errors.Wrapf(err, "splitting %s", file.File)
	}

	if v.Progress && v.Operation != "" {
		v, err = s.resolveOperation(ctx, v, file, stmts)
		if err != nil {
			return err
		}
	}

	start, err := Resume(v, file, stmts)
	if err != nil {
		return err
	}

	return s.apply(ctx, file, stmts, start)
}

// resolveOperation fetches the DDL operation a stopped run left in flight and counts its
// committed statements into Applied: all of them when it succeeded, its commit timestamps
// when it failed. The operation's statements must be the file's from Applied on; otherwise
// the file changed in its applied part.
func (s *Spanner) resolveOperation(ctx context.Context, v Version, file *Migration, stmts []string) (Version, error) {
	op := s.admin.UpdateDatabaseDdlOperation(v.Operation)
	waitErr := op.Wait(ctx)
	if !op.Done() {
		return v, errors.Wrapf(waitErr, "DDL operation %s, recorded for version %d, cannot be fetched; force the version that matches the database's real state", v.Operation, v.Number)
	}

	md, err := op.Metadata()
	if err != nil {
		return v, errors.Wrapf(err, "metadata of DDL operation %s", v.Operation)
	}

	n := len(md.GetCommitTimestamps())
	if waitErr == nil {
		n = len(md.GetStatements())
	}
	if v.Applied+n > len(stmts) || !slices.Equal(stmts[v.Applied:v.Applied+n], md.GetStatements()[:n]) {
		return v, errors.Newf("%s changed in the part already applied: DDL operation %s applied %s the file no longer carries at that place; restore the file or force a version",
			file.File, v.Operation, plural(n, "statement"))
	}

	v.Applied += n
	v.Checkpoint = Checkpoint(stmts[:v.Applied])
	v.Operation = ""
	if err := s.write(ctx, v); err != nil {
		return v, err
	}
	ccclogger.FromCtx(ctx).Infof("DDL operation %s of %s had finished with %s applied", op.Name(), file.File, plural(n, "statement"))

	return v, nil
}

// apply runs a file's statements from index start, group by group, recording progress on
// the version row, and marks the version clean at the end.
func (s *Spanner) apply(ctx context.Context, m *Migration, stmts []string, start int) error {
	log := ccclogger.FromCtx(ctx)

	if err := s.write(ctx, s.progress(m, stmts, start, "")); err != nil {
		return err
	}
	switch {
	case start > 0 && start < len(stmts):
		log.Infof("Continuing %s from statement %d of %d", m.File, start+1, len(stmts))
	case start > 0:
		log.Infof("Finishing %s: all %s had been applied", m.File, plural(len(stmts), "statement"))
	}

	for _, g := range Groups(stmts, start) {
		var err error
		switch g.Kind {
		case DDL:
			err = s.applyDDL(ctx, m, stmts, g)
		case DML:
			err = s.applyDML(ctx, m, stmts, g)
		}
		if err != nil {
			return err
		}
	}

	if err := s.write(ctx, Version{Number: m.Version, Known: true}); err != nil {
		return err
	}
	log.Infof("Applied %s (%s)", m.File, plural(len(stmts), "statement"))

	return nil
}

// progress is the version row for a file in progress with applied statements committed.
func (s *Spanner) progress(m *Migration, stmts []string, applied int, operation string) Version {
	return Version{
		Number:     m.Version,
		Known:      true,
		Dirty:      true,
		Progress:   true,
		Applied:    applied,
		Checkpoint: Checkpoint(stmts[:applied]),
		Operation:  operation,
	}
}

// applyDDL sends a DDL group as one batch. A synchronous refusal applied nothing. Once the
// operation is accepted its name goes on the version row, so a run that dies while waiting
// can be resolved later; on failure the operation's commit timestamps say how many
// statements applied.
func (s *Spanner) applyDDL(ctx context.Context, m *Migration, stmts []string, g Group) error {
	op, err := s.ddlBatch(ctx, g.Statements)
	if err != nil {
		return &DirtyError{
			Version:   m.Version,
			File:      m.File,
			Applied:   g.Start,
			Total:     len(stmts),
			Statement: g.Statements[0],
			Cause:     errors.Wrapf(err, "Spanner refused the batch of %s, nothing applied", plural(len(g.Statements), "DDL statement")),
		}
	}

	if err := s.write(ctx, s.progress(m, stmts, g.Start, op.Name())); err != nil {
		return err
	}

	waitErr := op.Wait(ctx)
	if !op.Done() {
		return errors.Wrapf(waitErr, "waiting for DDL operation %s of %s; the next run resolves it", op.Name(), m.File)
	}
	if waitErr != nil {
		md, err := op.Metadata()
		if err != nil {
			return errors.Wrapf(err, "metadata of DDL operation %s", op.Name())
		}
		applied := g.Start + len(md.GetCommitTimestamps())
		if err := s.write(ctx, s.progress(m, stmts, applied, "")); err != nil {
			return err
		}

		return &DirtyError{
			Version:   m.Version,
			File:      m.File,
			Applied:   applied,
			Total:     len(stmts),
			Statement: stmts[min(applied, len(stmts)-1)],
			Cause:     waitErr,
		}
	}

	return s.write(ctx, s.progress(m, stmts, g.Start+len(g.Statements), ""))
}

// applyDML runs a DML group in one read-write transaction: all of it applies, or none.
func (s *Spanner) applyDML(ctx context.Context, m *Migration, stmts []string, g Group) error {
	failed, err := s.dmlBatch(ctx, g.Statements)
	if err != nil {
		return &DirtyError{
			Version:    m.Version,
			File:       m.File,
			Applied:    g.Start,
			Total:      len(stmts),
			RolledBack: failed,
			Statement:  g.Statements[min(failed, len(g.Statements)-1)],
			Cause:      err,
		}
	}

	return s.write(ctx, s.progress(m, stmts, g.Start+len(g.Statements), ""))
}

// Down reverts versions from the current one to no version, running each version's down
// file if it has one. While a down file runs the version is marked dirty without progress,
// so a failure leaves the version to be forced by hand.
func (s *Spanner) Down(ctx context.Context, src *Source) error {
	log := ccclogger.FromCtx(ctx)

	if err := s.ensureTable(ctx); err != nil {
		return err
	}

	v, err := s.read(ctx)
	if err != nil {
		return err
	}
	if !v.Known {
		log.Info("No change: the database has no version to revert")

		return nil
	}
	if v.Dirty {
		return errors.Newf("the database is at %s; force the version that matches its real state before migrating down", v)
	}
	if !src.Has(v.Number) {
		return errors.Newf("the database is at version %d, which this build does not carry", v.Number)
	}

	current := v.Number
	for {
		if m, ok := src.Down(current); ok {
			if err := s.revert(ctx, m); err != nil {
				return err
			}
			log.Infof("Reverted %s", m.File)
		}

		prev, ok := src.Prev(current)
		if !ok {
			return s.write(ctx, Version{})
		}
		if err := s.write(ctx, Version{Number: prev, Known: true}); err != nil {
			return err
		}
		current = prev
	}
}

// revert runs a down file with the version marked dirty and no progress recorded.
func (s *Spanner) revert(ctx context.Context, m *Migration) error {
	stmts, err := Split(m.SQL)
	if err != nil {
		return errors.Wrapf(err, "splitting %s", m.File)
	}

	if err := s.write(ctx, Version{Number: m.Version, Known: true, Dirty: true}); err != nil {
		return err
	}

	for _, g := range Groups(stmts, 0) {
		if err := s.runGroup(ctx, g); err != nil {
			return errors.Wrapf(err, "%s failed; the database is dirty at version %d, force the version that matches its real state", m.File, m.Version)
		}
	}

	return nil
}

// runGroup runs a group without recording progress.
func (s *Spanner) runGroup(ctx context.Context, g Group) error {
	switch g.Kind {
	case DDL:
		op, err := s.ddlBatch(ctx, g.Statements)
		if err != nil {
			return err
		}
		if err := op.Wait(ctx); err != nil {
			return errors.Wrapf(err, "DDL operation %s", op.Name())
		}
	case DML:
		if _, err := s.dmlBatch(ctx, g.Statements); err != nil {
			return err
		}
	}

	return nil
}

// ddlBatch sends DDL statements as one batch and returns the accepted operation.
func (s *Spanner) ddlBatch(ctx context.Context, stmts []string) (*database.UpdateDatabaseDdlOperation, error) {
	op, err := s.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.database,
		Statements: stmts,
	})
	if err != nil {
		return nil, errors.Wrap(err, "DatabaseAdminClient.UpdateDatabaseDdl()")
	}

	return op, nil
}

// dmlBatch runs DML statements in one read-write transaction. On failure nothing is
// applied, and the index of the statement that failed comes back with the error.
func (s *Spanner) dmlBatch(ctx context.Context, stmts []string) (int, error) {
	batch := make([]spanner.Statement, len(stmts))
	for i, sql := range stmts {
		batch[i] = spanner.Statement{SQL: sql}
	}

	failed := 0
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		counts, err := txn.BatchUpdate(ctx, batch)
		if err != nil {
			failed = len(counts)

			return errors.Wrap(err, "spanner.ReadWriteTransaction.BatchUpdate()")
		}

		return nil
	})
	if err != nil {
		return failed, errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return 0, nil
}

// Version reads the version table, creating it when the database has none.
func (s *Spanner) Version(ctx context.Context) (Version, error) {
	if err := s.ensureTable(ctx); err != nil {
		return Version{}, err
	}

	return s.read(ctx)
}

// Force sets the version clean, with no progress recorded. Version -1 deletes the row, so
// the database has no version.
func (s *Spanner) Force(ctx context.Context, version int) error {
	if version < -1 {
		return errors.Newf("version %d cannot be forced: versions are 0 or above, and -1 means no version", version)
	}

	if err := s.ensureTable(ctx); err != nil {
		return err
	}

	v := Version{}
	if version >= 0 {
		v = Version{Number: version, Known: true}
	}
	if err := s.write(ctx, v); err != nil {
		return err
	}
	ccclogger.FromCtx(ctx).Infof("Forced %s to %s", s.table, v)

	return nil
}

// ensureTable creates the version table, or adds the progress columns to one the old
// library created.
func (s *Spanner) ensureTable(ctx context.Context) error {
	columns, err := s.columns(ctx)
	if err != nil {
		return err
	}

	// The progress columns, added to a version table the old library created.
	progressColumns := []struct {
		name string
		ddl  string
	}{
		{name: columnApplied, ddl: "Applied INT64"},
		{name: columnCheckpoint, ddl: "Checkpoint STRING(64)"},
		{name: columnOperation, ddl: "Operation STRING(MAX)"},
	}

	var ddl []string
	if len(columns) == 0 {
		ddl = append(ddl, fmt.Sprintf("CREATE TABLE `%s` (Version INT64 NOT NULL, Dirty BOOL NOT NULL, Applied INT64, Checkpoint STRING(64), Operation STRING(MAX)) PRIMARY KEY (Version)", s.table))
	} else {
		for _, column := range progressColumns {
			if !slices.Contains(columns, column.name) {
				ddl = append(ddl, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", s.table, column.ddl))
			}
		}
	}
	if len(ddl) == 0 {
		return nil
	}

	op, err := s.ddlBatch(ctx, ddl)
	if err != nil {
		return errors.Wrapf(err, "creating the version table %s", s.table)
	}
	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "creating the version table %s: DDL operation %s", s.table, op.Name())
	}

	return nil
}

// columns lists the version table's columns; none means the table does not exist.
func (s *Spanner) columns(ctx context.Context) ([]string, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND TABLE_NAME = @table",
		Params: map[string]any{"table": s.table},
	}

	var columns []string
	if err := s.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return errors.Wrap(err, "spanner.Row.Columns()")
		}
		columns = append(columns, name)

		return nil
	}); err != nil {
		return nil, errors.Wrapf(err, "reading the columns of %s", s.table)
	}

	return columns, nil
}

// read returns the version row. No row, or the old library's -1 row, is no version.
func (s *Spanner) read(ctx context.Context) (Version, error) {
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT %s FROM `%s` LIMIT 1", strings.Join([]string{columnVersion, columnDirty, columnApplied, columnCheckpoint, columnOperation}, ", "), s.table)}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return Version{}, nil
	}
	if err != nil {
		return Version{}, errors.Wrapf(err, "reading the version from %s", s.table)
	}

	var (
		version    int64
		dirty      bool
		applied    spanner.NullInt64
		checkpoint spanner.NullString
		operation  spanner.NullString
	)
	if err := row.Columns(&version, &dirty, &applied, &checkpoint, &operation); err != nil {
		return Version{}, errors.Wrapf(err, "reading the version row of %s", s.table)
	}
	if version < 0 {
		return Version{}, nil
	}

	return Version{
		Number:     int(version),
		Known:      true,
		Dirty:      dirty,
		Progress:   applied.Valid,
		Applied:    int(applied.Int64),
		Checkpoint: checkpoint.StringVal,
		Operation:  operation.StringVal,
	}, nil
}

// write replaces the version row: every row is deleted and, for a known version, one is
// inserted, in one commit.
func (s *Spanner) write(ctx context.Context, v Version) error {
	mutations := []*spanner.Mutation{spanner.Delete(s.table, spanner.AllKeys())}
	if v.Known {
		mutations = append(mutations, spanner.Insert(s.table,
			[]string{columnVersion, columnDirty, columnApplied, columnCheckpoint, columnOperation},
			[]any{
				int64(v.Number),
				v.Dirty,
				spanner.NullInt64{Int64: int64(v.Applied), Valid: v.Progress},
				spanner.NullString{StringVal: v.Checkpoint, Valid: v.Progress},
				spanner.NullString{StringVal: v.Operation, Valid: v.Operation != ""},
			},
		))
	}

	if _, err := s.client.Apply(ctx, mutations); err != nil {
		return errors.Wrapf(err, "writing %s to %s", v, s.table)
	}

	return nil
}
