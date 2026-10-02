package runner

import (
	"strings"
	"testing"
)

func TestResume(t *testing.T) {
	t.Parallel()

	stmts := []string{
		"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)",
		"CREATE UNIQUE INDEX U ON T(Val)",
		"CREATE TABLE B (Id INT64) PRIMARY KEY (Id)",
	}
	file := &Migration{Version: 41, Name: "Refits", Direction: Up, File: "000041_Refits.up.sql"}
	changed := []string{
		"CREATE TABLE A (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
		stmts[1],
		stmts[2],
	}

	tests := []struct {
		name      string
		version   Version
		file      *Migration
		stmts     []string
		wantStart int
		wantErr   string
	}{
		{
			name:    "a clean version has nothing to resume",
			version: Version{Number: 41, Known: true},
			file:    file,
			stmts:   stmts,
			wantErr: "version 41 is not dirty",
		},
		{
			name:    "dirty from the old library, no progress recorded",
			version: Version{Number: 41, Known: true, Dirty: true},
			file:    file,
			stmts:   stmts,
			wantErr: "version 41 is dirty from a run that recorded no progress; force the version that matches the database's real state",
		},
		{
			name:    "no progress recorded wins over a missing file",
			version: Version{Number: 41, Known: true, Dirty: true},
			file:    nil,
			wantErr: "recorded no progress",
		},
		{
			name:    "the build does not carry the dirty version",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1, Checkpoint: Checkpoint(stmts[:1])},
			file:    nil,
			wantErr: "the database is at version 41, which this build does not carry",
		},
		{
			name:    "an unresolved DDL operation",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1, Checkpoint: Checkpoint(stmts[:1]), Operation: "projects/p/instances/i/databases/d/operations/op1"},
			file:    file,
			stmts:   stmts,
			wantErr: "has DDL operation projects/p/instances/i/databases/d/operations/op1 unresolved",
		},
		{
			name:    "the file is shorter than the applied part",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 3, Checkpoint: Checkpoint(stmts)},
			file:    file,
			stmts:   stmts[:2],
			wantErr: "000041_Refits.up.sql changed in the part already applied (3 statements); restore the file or force a version",
		},
		{
			name:    "the applied part changed",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1, Checkpoint: Checkpoint(stmts[:1])},
			file:    file,
			stmts:   changed,
			wantErr: "000041_Refits.up.sql changed in the part already applied (1 statement); restore the file or force a version",
		},
		{
			name:      "the applied part is intact",
			version:   Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1, Checkpoint: Checkpoint(stmts[:1])},
			file:      file,
			stmts:     stmts,
			wantStart: 1,
		},
		{
			name:      "the part after the applied one may change",
			version:   Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 1, Checkpoint: Checkpoint(stmts[:1])},
			file:      file,
			stmts:     []string{stmts[0], "CREATE UNIQUE INDEX U ON T(Val, Other)"},
			wantStart: 1,
		},
		{
			name:      "nothing applied: the whole file may change",
			version:   Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 0, Checkpoint: Checkpoint(nil)},
			file:      file,
			stmts:     changed,
			wantStart: 0,
		},
		{
			name:      "nothing applied of a file with no statements",
			version:   Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 0, Checkpoint: Checkpoint(nil)},
			file:      file,
			stmts:     nil,
			wantStart: 0,
		},
		{
			name:    "nothing applied but a foreign checkpoint",
			version: Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 0, Checkpoint: "not-a-checkpoint"},
			file:    file,
			stmts:   stmts,
			wantErr: "changed in the part already applied (0 statements)",
		},
		{
			name:      "everything applied, the clean mark never written",
			version:   Version{Number: 41, Known: true, Dirty: true, Progress: true, Applied: 3, Checkpoint: Checkpoint(stmts)},
			file:      file,
			stmts:     stmts,
			wantStart: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			start, err := Resume(tt.version, tt.file, tt.stmts)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Resume() error = %v, want one containing %q", err, tt.wantErr)
				}

				return
			}
			if err != nil {
				t.Fatalf("Resume() error = %v", err)
			}
			if start != tt.wantStart {
				t.Errorf("Resume() = %d, want %d", start, tt.wantStart)
			}
		})
	}
}
