package runner

import (
	"github.com/go-playground/errors/v5"
)

// Resume decides where an up run continues on a dirty version: the index of the first
// statement still to run, or a refusal that names the two ways out. The version's
// operation, if any, must be resolved into Applied before the call. The file is the up
// migration for the dirty version, nil when the source does not carry it, and stmts are its
// statements.
func Resume(v Version, file *Migration, stmts []string) (int, error) {
	if !v.Dirty {
		return 0, errors.Newf("version %d is not dirty; nothing to resume", v.Number)
	}
	if v.Operation != "" {
		return 0, errors.Newf("version %d has DDL operation %s unresolved", v.Number, v.Operation)
	}
	if !v.Progress {
		return 0, errors.Newf("version %d is dirty from a run that recorded no progress; force the version that matches the database's real state", v.Number)
	}
	if file == nil {
		return 0, errors.Newf("the database is at version %d, which this build does not carry", v.Number)
	}
	if v.Applied > len(stmts) || Checkpoint(stmts[:v.Applied]) != v.Checkpoint {
		return 0, errors.Newf("%s changed in the part already applied (%s); restore the file or force a version", file.File, plural(v.Applied, "statement"))
	}

	return v.Applied, nil
}
