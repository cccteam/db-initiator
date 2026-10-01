package runner

import (
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		want    []string
		wantErr bool
	}{
		{
			name: "empty file",
			sql:  "",
			want: []string{},
		},
		{
			name: "whitespace only",
			sql:  "  \n\t \n",
			want: []string{},
		},
		{
			name: "line comment alone",
			sql:  "-- nothing to run\n",
			want: []string{},
		},
		{
			name: "block comment alone",
			sql:  "/* nothing\n   to run */",
			want: []string{},
		},
		{
			name: "hash comment alone",
			sql:  "# nothing to run\n",
			want: []string{},
		},
		{
			name: "one statement with semicolon",
			sql:  "CREATE TABLE T (Id INT64) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "last statement without semicolon",
			sql:  "CREATE TABLE A (Id INT64) PRIMARY KEY (Id);\nCREATE TABLE B (Id INT64) PRIMARY KEY (Id)",
			want: []string{"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)", "CREATE TABLE B (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "line comment before a statement",
			sql:  "-- the table\nCREATE TABLE T (Id INT64) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "line comment at the end of a line keeps the line break",
			sql:  "CREATE TABLE T (\n  Id INT64, -- the key\n  Name STRING(MAX)\n) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE T (\n  Id INT64, \n  Name STRING(MAX)\n) PRIMARY KEY (Id)"},
		},
		{
			name: "block comment inside a statement",
			sql:  "CREATE TABLE T (Id INT64 /* the key */, Name STRING(MAX)) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE T (Id INT64 , Name STRING(MAX)) PRIMARY KEY (Id)"},
		},
		{
			name: "block comment with no space around it keeps the tokens apart",
			sql:  "CREATE TABLE T (Id/* the key */INT64) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "every comment kind mixed",
			sql:  "# a\n-- b\n/* c */ CREATE TABLE T (Id INT64) PRIMARY KEY (Id); -- d\n/* e */",
			want: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "trailing line comment without a line break",
			sql:  "CREATE TABLE T (Id INT64) PRIMARY KEY (Id) -- done",
			want: []string{"CREATE TABLE T (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "semicolon inside a string literal",
			sql:  "INSERT INTO T (Id, Name) VALUES ('a;b', 'x');INSERT INTO T (Id, Name) VALUES ('c', 'y');",
			want: []string{"INSERT INTO T (Id, Name) VALUES ('a;b', 'x')", "INSERT INTO T (Id, Name) VALUES ('c', 'y')"},
		},
		{
			name: "comment markers inside a string literal",
			sql:  "INSERT INTO T (Id, Name) VALUES ('a -- b', '/* c */ # d');",
			want: []string{"INSERT INTO T (Id, Name) VALUES ('a -- b', '/* c */ # d')"},
		},
		{
			name: "backticked identifiers",
			sql:  "CREATE TABLE `Order` (`Id` INT64, `Group` STRING(MAX)) PRIMARY KEY (`Id`);",
			want: []string{"CREATE TABLE `Order` (`Id` INT64, `Group` STRING(MAX)) PRIMARY KEY (`Id`)"},
		},
		{
			name: "leading whitespace is trimmed",
			sql:  "\n\n   CREATE TABLE A (Id INT64) PRIMARY KEY (Id);\n\n",
			want: []string{"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "CRLF line ends are kept inside a statement",
			sql:  "CREATE TABLE A (\r\n  Id INT64\r\n) PRIMARY KEY (Id);\r\nCREATE TABLE B (Id INT64) PRIMARY KEY (Id);\r\n",
			want: []string{"CREATE TABLE A (\r\n  Id INT64\r\n) PRIMARY KEY (Id)", "CREATE TABLE B (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "empty statements between semicolons are dropped",
			sql:  "CREATE TABLE A (Id INT64) PRIMARY KEY (Id);;;\n;CREATE TABLE B (Id INT64) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)", "CREATE TABLE B (Id INT64) PRIMARY KEY (Id)"},
		},
		{
			name: "a statement spanning lines keeps its inner newlines",
			sql:  "CREATE TABLE A (\n  Id INT64,\n  Name STRING(MAX)\n) PRIMARY KEY (Id);",
			want: []string{"CREATE TABLE A (\n  Id INT64,\n  Name STRING(MAX)\n) PRIMARY KEY (Id)"},
		},
		{
			name: "DML in any case",
			sql:  "insert into T (Id) values (1);Update T set Id = 2;DELETE FROM T WHERE Id = 2;",
			want: []string{"insert into T (Id) values (1)", "Update T set Id = 2", "DELETE FROM T WHERE Id = 2"},
		},
		{
			name: "DDL then DML then DDL stay in file order",
			sql:  "CREATE TABLE A (Id INT64) PRIMARY KEY (Id);\nINSERT INTO A (Id) VALUES (1);\nCREATE INDEX AById ON A(Id);",
			want: []string{"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)", "INSERT INTO A (Id) VALUES (1)", "CREATE INDEX AById ON A(Id)"},
		},
		{
			name:    "unclosed block comment",
			sql:     "CREATE TABLE A (Id INT64) PRIMARY KEY (Id); /* never closed",
			wantErr: true,
		},
		{
			name:    "unclosed string literal",
			sql:     "INSERT INTO T (Id) VALUES ('never closed);",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Split(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Split() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Split() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSplit_Fixtures splits the files the emulator tests apply: what comes out is what
// Spanner accepts, so the two agree on the statement counts and kinds.
func TestSplit_Fixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		file      string
		wantCount int
		wantKind  Kind
	}{
		{
			name:      "schema with comments, indexes, foreign key and view",
			file:      "../../testdata/spanner/migrations_full/000001_schema.up.sql",
			wantCount: 8,
			wantKind:  DDL,
		},
		{
			name:      "seed with comments and multi-line inserts",
			file:      "../../testdata/spanner/datamigrations_full/000001_seed_data.up.sql",
			wantCount: 6,
			wantKind:  DML,
		},
		{
			name:      "two statements without a trailing newline",
			file:      "../../testdata/spanner/migrations/000001_users.up.sql",
			wantCount: 2,
			wantKind:  DDL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("os.ReadFile() error = %v", err)
			}

			got, err := Split(string(raw))
			if err != nil {
				t.Fatalf("Split() error = %v", err)
			}
			if len(got) != tt.wantCount {
				t.Fatalf("Split() returned %d statements, want %d: %q", len(got), tt.wantCount, got)
			}
			for i, stmt := range got {
				if strings.Contains(stmt, "--") || strings.Contains(stmt, "/*") {
					t.Errorf("statement %d keeps a comment: %q", i+1, stmt)
				}
				if kind := KindOf(stmt); kind != tt.wantKind {
					t.Errorf("statement %d is %s, want %s: %q", i+1, kind, tt.wantKind, stmt)
				}
				if stmt != strings.TrimSpace(stmt) {
					t.Errorf("statement %d is not trimmed: %q", i+1, stmt)
				}
			}
		})
	}
}

func TestKindOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt string
		want Kind
	}{
		{name: "insert", stmt: "INSERT INTO T (Id) VALUES (1)", want: DML},
		{name: "insert lower case", stmt: "insert into T (Id) values (1)", want: DML},
		{name: "insert split over lines", stmt: "INSERT\nINTO T (Id) VALUES (1)", want: DML},
		{name: "update", stmt: "UPDATE T SET Name = 'x' WHERE Id = 1", want: DML},
		{name: "delete", stmt: "Delete FROM T WHERE Id = 1", want: DML},
		{name: "create table", stmt: "CREATE TABLE T (Id INT64) PRIMARY KEY (Id)", want: DDL},
		{name: "alter table", stmt: "ALTER TABLE T ADD COLUMN Name STRING(MAX)", want: DDL},
		{name: "drop", stmt: "DROP TABLE T", want: DDL},
		{name: "create index", stmt: "CREATE UNIQUE INDEX TByName ON T(Name)", want: DDL},
		{name: "a table named Insert is still DDL", stmt: "CREATE TABLE Insert (Id INT64) PRIMARY KEY (Id)", want: DDL},
		{name: "empty", stmt: "", want: DDL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := KindOf(tt.stmt); got != tt.want {
				t.Errorf("KindOf() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestGroups(t *testing.T) {
	t.Parallel()

	stmts := []string{
		"CREATE TABLE A (Id INT64) PRIMARY KEY (Id)",
		"CREATE TABLE B (Id INT64) PRIMARY KEY (Id)",
		"INSERT INTO A (Id) VALUES (1)",
		"INSERT INTO B (Id) VALUES (1)",
		"CREATE INDEX AById ON A(Id)",
	}

	tests := []struct {
		name  string
		stmts []string
		start int
		want  []Group
	}{
		{
			name:  "from the start: DDL, DML, DDL",
			stmts: stmts,
			start: 0,
			want: []Group{
				{Kind: DDL, Start: 0, Statements: stmts[0:2]},
				{Kind: DML, Start: 2, Statements: stmts[2:4]},
				{Kind: DDL, Start: 4, Statements: stmts[4:5]},
			},
		},
		{
			name:  "from inside the first group",
			stmts: stmts,
			start: 1,
			want: []Group{
				{Kind: DDL, Start: 1, Statements: stmts[1:2]},
				{Kind: DML, Start: 2, Statements: stmts[2:4]},
				{Kind: DDL, Start: 4, Statements: stmts[4:5]},
			},
		},
		{
			name:  "from inside the DML group",
			stmts: stmts,
			start: 3,
			want: []Group{
				{Kind: DML, Start: 3, Statements: stmts[3:4]},
				{Kind: DDL, Start: 4, Statements: stmts[4:5]},
			},
		},
		{
			name:  "from the end",
			stmts: stmts,
			start: 5,
			want:  nil,
		},
		{
			name:  "no statements",
			stmts: nil,
			start: 0,
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tt.want, Groups(tt.stmts, tt.start)); diff != "" {
				t.Errorf("Groups() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCheckpoint(t *testing.T) {
	t.Parallel()

	const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name  string
		a     []string
		b     []string
		equal bool
	}{
		{name: "same statements give the same hash", a: []string{"CREATE TABLE A", "CREATE TABLE B"}, b: []string{"CREATE TABLE A", "CREATE TABLE B"}, equal: true},
		{name: "a one-character change differs", a: []string{"CREATE TABLE A", "CREATE TABLE B"}, b: []string{"CREATE TABLE A", "CREATE TABLE C"}, equal: false},
		{name: "order matters", a: []string{"A", "B"}, b: []string{"B", "A"}, equal: false},
		{name: "statement boundaries matter", a: []string{"A", "B"}, b: []string{"A\nB"}, equal: false},
		{name: "a prefix differs from the whole", a: []string{"A"}, b: []string{"A", "B"}, equal: false},
		{name: "nil and empty are the same zero statements", a: nil, b: []string{}, equal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := Checkpoint(tt.a) == Checkpoint(tt.b); got != tt.equal {
				t.Errorf("Checkpoint(a) == Checkpoint(b) = %v, want %v", got, tt.equal)
			}
		})
	}

	t.Run("the hash of zero statements is stable", func(t *testing.T) {
		t.Parallel()

		if got := Checkpoint(nil); got != emptyHash {
			t.Errorf("Checkpoint(nil) = %s, want %s", got, emptyHash)
		}
		if got := len(Checkpoint([]string{"A"})); got != 64 {
			t.Errorf("len(Checkpoint()) = %d, want 64", got)
		}
	})
}
