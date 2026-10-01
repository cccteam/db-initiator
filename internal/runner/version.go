package runner

import (
	"fmt"
	"strings"
)

// noVersion is how a database with nothing applied prints.
const noVersion = "no version"

// Version is what a version table says about a database.
type Version struct {
	// Number is the version; meaningful when Known.
	Number int
	// Known is false when the table holds no version: nothing has been applied yet.
	Known bool
	// Dirty is true while a file is in progress, and after one stopped before its end.
	Dirty bool
	// Progress is true when the runner recorded how far the in-progress file got: Applied
	// and Checkpoint are set. False on a database the old library left dirty, or on a
	// Postgres database, which keeps no progress.
	Progress bool
	// Applied is the number of the in-progress file's statements that committed.
	Applied int
	// Checkpoint is the hex SHA-256 over the applied statements' text.
	Checkpoint string
	// Operation names the Spanner DDL operation that was in flight when the runner stopped,
	// or is empty.
	Operation string
}

// String prints the version in one phrase: "no version", "version 41", "version 41, dirty",
// "version 41, dirty: 2 statements applied".
func (v Version) String() string {
	if !v.Known {
		return noVersion
	}

	var b strings.Builder
	fmt.Fprintf(&b, "version %d", v.Number)
	if !v.Dirty {
		return b.String()
	}

	b.WriteString(", dirty")
	if v.Progress {
		fmt.Fprintf(&b, ": %s applied", plural(v.Applied, "statement"))
	}
	if v.Operation != "" {
		fmt.Fprintf(&b, ", DDL operation %s in flight", v.Operation)
	}

	return b.String()
}

// DirtyError says a migration file stopped before its end. The database is dirty at the
// file's version and the version table records how far it got, so a rerun continues from
// the failed statement once the cause is fixed.
type DirtyError struct {
	// Version is the file's version.
	Version int
	// File is the file's name.
	File string
	// Applied is the number of the file's statements that are applied; the rerun continues
	// from the next one.
	Applied int
	// Total is the file's statement count.
	Total int
	// RolledBack is the number of statements of a DML group that ran and were rolled back
	// with the group; zero for a DDL failure, where nothing rolls back.
	RolledBack int
	// Statement is the statement that failed.
	Statement string
	// Cause is the database's error.
	Cause error
}

// Error prints one sentence with the recovery.
func (e *DirtyError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s stopped at statement %d of %d (%s): %v", e.File, e.Applied+e.RolledBack+1, e.Total, excerpt(e.Statement), e.Cause)
	if e.RolledBack > 0 {
		fmt.Fprintf(&b, "; its DML group rolled back, including the %s before it", plural(e.RolledBack, "statement"))
	}
	fmt.Fprintf(&b, "; fix the cause and rerun, which continues from statement %d, or force a version", e.Applied+1)

	return b.String()
}

// Unwrap returns the database's error.
func (e *DirtyError) Unwrap() error {
	return e.Cause
}

// excerptLength is how much of a statement an error quotes.
const excerptLength = 72

// excerpt is the start of a statement, on one line, for an error message.
func excerpt(stmt string) string {
	stmt = strings.Join(strings.Fields(stmt), " ")
	if len(stmt) > excerptLength {
		return stmt[:excerptLength] + "..."
	}

	return stmt
}

// plural counts a noun: "1 statement", "2 statements".
func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}

	return fmt.Sprintf("%d %ss", n, noun)
}
