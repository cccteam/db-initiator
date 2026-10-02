package runner

import (
	"errors"
	"testing"
)

func TestVersion_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version Version
		want    string
	}{
		{name: "no version", version: Version{}, want: "no version"},
		{name: "clean", version: Version{Number: 41, Known: true}, want: "version 41"},
		{name: "dirty without progress", version: Version{Number: 41, Known: true, Dirty: true}, want: "version 41, dirty"},
		{name: "dirty with no statements applied", version: Version{Number: 41, Known: true, Dirty: true, Progress: true}, want: "version 41, dirty: 0 statements applied"},
		{name: "dirty with one statement applied", version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1}, want: "version 41, dirty: 1 statement applied"},
		{name: "dirty with two statements applied", version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 2}, want: "version 41, dirty: 2 statements applied"},
		{
			name:    "dirty with an operation in flight",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 2, Operation: "projects/p/instances/i/databases/d/operations/op1"},
			want:    "version 41, dirty: 2 statements applied, DDL operation projects/p/instances/i/databases/d/operations/op1 in flight",
		},
		{name: "version zero", version: Version{Number: 0, Known: true}, want: "version 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.version.String(); got != tt.want {
				t.Errorf("Version.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDirtyError_Error(t *testing.T) {
	t.Parallel()

	cause := errors.New("Found uniqueness violation on index U")

	tests := []struct {
		name string
		err  *DirtyError
		want string
	}{
		{
			name: "DDL stopped at the second statement",
			err:  &DirtyError{Version: 41, File: "000041_Refits.up.sql", Applied: 1, Total: 3, Statement: "CREATE UNIQUE INDEX U ON T(Val)", Cause: cause},
			want: "000041_Refits.up.sql stopped at statement 2 of 3 (CREATE UNIQUE INDEX U ON T(Val)): Found uniqueness violation on index U; fix the cause and rerun, which continues from statement 2, or force a version",
		},
		{
			name: "DDL batch refused, nothing applied",
			err:  &DirtyError{Version: 41, File: "000041_Refits.up.sql", Applied: 0, Total: 3, Statement: "CREATE TABLE A (Id INT64) PRIMARY KEY (Id)", Cause: cause},
			want: "000041_Refits.up.sql stopped at statement 1 of 3 (CREATE TABLE A (Id INT64) PRIMARY KEY (Id)): Found uniqueness violation on index U; fix the cause and rerun, which continues from statement 1, or force a version",
		},
		{
			name: "DML group rolled back after its first statement",
			err:  &DirtyError{Version: 2, File: "000002_seed.up.sql", Applied: 1, Total: 4, RolledBack: 1, Statement: "INSERT INTO T (Id) VALUES (1)", Cause: cause},
			want: "000002_seed.up.sql stopped at statement 3 of 4 (INSERT INTO T (Id) VALUES (1)): Found uniqueness violation on index U; its DML group rolled back, including the 1 statement before it; fix the cause and rerun, which continues from statement 2, or force a version",
		},
		{
			name: "a long statement is excerpted on one line",
			err: &DirtyError{
				Version: 1, File: "000001_a.up.sql", Applied: 0, Total: 1,
				Statement: "CREATE TABLE Products (\n  Id STRING(36) NOT NULL,\n  Name STRING(255) NOT NULL,\n  Description STRING(MAX),\n  Price FLOAT64 NOT NULL\n) PRIMARY KEY (Id)",
				Cause:     cause,
			},
			want: "000001_a.up.sql stopped at statement 1 of 1 (CREATE TABLE Products ( Id STRING(36) NOT NULL, Name STRING(255) NOT NUL...): Found uniqueness violation on index U; fix the cause and rerun, which continues from statement 1, or force a version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.err.Error(); got != tt.want {
				t.Errorf("DirtyError.Error() =\n%q\nwant\n%q", got, tt.want)
			}
			if !errors.Is(tt.err, cause) {
				t.Error("errors.Is(err, cause) = false, want the cause to unwrap")
			}
			var dirty *DirtyError
			if !errors.As(tt.err, &dirty) {
				t.Error("errors.As(err, *DirtyError) = false")
			}
		})
	}
}
