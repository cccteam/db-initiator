package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
)

func TestParseFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		file     string
		want     *Migration
		wantErr  string
		errNames bool
	}{
		{
			name: "up with leading zeros",
			file: "000041_Refits.up.sql",
			want: &Migration{Version: 41, Name: "Refits", Direction: Up, File: "000041_Refits.up.sql"},
		},
		{
			name: "down without leading zeros",
			file: "41_Refits.down.sql",
			want: &Migration{Version: 41, Name: "Refits", Direction: Down, File: "41_Refits.down.sql"},
		},
		{
			name: "name with underscores and dots",
			file: "000002_demo_world.v2.up.sql",
			want: &Migration{Version: 2, Name: "demo_world.v2", Direction: Up, File: "000002_demo_world.v2.up.sql"},
		},
		{
			name: "empty name",
			file: "000001_.up.sql",
			want: &Migration{Version: 1, Name: "", Direction: Up, File: "000001_.up.sql"},
		},
		{
			name:    "no version",
			file:    "Refits.up.sql",
			wantErr: "is not a migration file name",
		},
		{
			name:    "no direction",
			file:    "000041_Refits.sql",
			wantErr: "is not a migration file name",
		},
		{
			name:    "direction in upper case",
			file:    "000041_Refits.UP.sql",
			wantErr: "is not a migration file name",
		},
		{
			name:    "no underscore after the version",
			file:    "000041.up.sql",
			wantErr: "is not a migration file name",
		},
		{
			name:    "version too large to hold",
			file:    "99999999999999999999_Refits.up.sql",
			wantErr: "is not a number this runner can hold",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseFileName(tt.file)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseFileName() error = %v, want one containing %q", err, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.file) {
					t.Errorf("parseFileName() error = %v, want it to name %s", err, tt.file)
				}

				return
			}
			if err != nil {
				t.Fatalf("parseFileName() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("parseFileName() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		files        map[string]string
		wantVersions []int
		wantDowns    []int
		wantErr      string
	}{
		{
			name:         "empty directory",
			files:        map[string]string{},
			wantVersions: []int{},
			wantDowns:    []int{},
		},
		{
			name: "ups and downs",
			files: map[string]string{
				"000001_a.up.sql":   "CREATE TABLE A (Id INT64) PRIMARY KEY (Id);",
				"000001_a.down.sql": "DROP TABLE A;",
				"000002_b.up.sql":   "CREATE TABLE B (Id INT64) PRIMARY KEY (Id);",
				"000002_b.down.sql": "DROP TABLE B;",
			},
			wantVersions: []int{1, 2},
			wantDowns:    []int{1, 2},
		},
		{
			name: "versions compare numerically, not by listing order",
			files: map[string]string{
				"10_j.up.sql":   "",
				"2_b.up.sql":    "",
				"1_a.up.sql":    "",
				"0100_h.up.sql": "",
			},
			wantVersions: []int{1, 2, 10, 100},
			wantDowns:    []int{},
		},
		{
			name: "up without down",
			files: map[string]string{
				"000001_a.up.sql": "",
			},
			wantVersions: []int{1},
			wantDowns:    []int{},
		},
		{
			name: "files without the sql suffix are ignored",
			files: map[string]string{
				"000001_a.up.sql": "",
				"README.md":       "notes",
				"000001_a.up.bak": "",
				".keep":           "",
			},
			wantVersions: []int{1},
			wantDowns:    []int{},
		},
		{
			name: "a subdirectory is ignored",
			files: map[string]string{
				"000001_a.up.sql":         "",
				"archive/000002_b.up.sql": "",
			},
			wantVersions: []int{1},
			wantDowns:    []int{},
		},
		{
			name: "a misnamed sql file is refused by name",
			files: map[string]string{
				"000001_a.up.sql": "",
				"seed.sql":        "INSERT INTO A (Id) VALUES (1);",
			},
			wantErr: "seed.sql is not a migration file name",
		},
		{
			name: "two up files with one version",
			files: map[string]string{
				"000001_a.up.sql": "",
				"1_b.up.sql":      "",
			},
			wantErr: "are both the up migration of version 1",
		},
		{
			name: "two down files with one version",
			files: map[string]string{
				"000001_a.up.sql":   "",
				"000001_a.down.sql": "",
				"000001_b.down.sql": "",
			},
			wantErr: "are both the down migration of version 1",
		},
		{
			name: "a down without an up",
			files: map[string]string{
				"000001_a.up.sql":   "",
				"000002_b.down.sql": "",
			},
			wantErr: "000002_b.down.sql has no up migration for version 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fsys := fstest.MapFS{}
			for name, sql := range tt.files {
				fsys[name] = &fstest.MapFile{Data: []byte(sql)}
			}

			src, err := Load(fsys)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %v, want one containing %q", err, tt.wantErr)
				}

				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if diff := cmp.Diff(tt.wantVersions, src.Versions()); diff != "" {
				t.Errorf("Versions() mismatch (-want +got):\n%s", diff)
			}
			downs := []int{}
			for _, version := range src.Versions() {
				if _, ok := src.Down(version); ok {
					downs = append(downs, version)
				}
			}
			if diff := cmp.Diff(tt.wantDowns, downs); diff != "" {
				t.Errorf("down versions mismatch (-want +got):\n%s", diff)
			}
			for _, version := range src.Versions() {
				m, ok := src.Up(version)
				if !ok {
					t.Fatalf("Up(%d) missing", version)
				}
				if m.SQL != tt.files[m.File] {
					t.Errorf("Up(%d).SQL = %q, want the file's text %q", version, m.SQL, tt.files[m.File])
				}
			}
		})
	}
}

func TestSource_Navigation(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"000001_a.up.sql":   &fstest.MapFile{},
		"000001_a.down.sql": &fstest.MapFile{},
		"000003_c.up.sql":   &fstest.MapFile{},
		"000007_g.up.sql":   &fstest.MapFile{},
		"000007_g.down.sql": &fstest.MapFile{},
	}
	src, err := Load(fsys)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	tests := []struct {
		name        string
		version     int
		wantHas     bool
		wantDown    bool
		wantPending []int
		wantPrev    int
		wantPrevOK  bool
	}{
		{name: "no version", version: -1, wantHas: false, wantPending: []int{1, 3, 7}, wantPrevOK: false},
		{name: "first", version: 1, wantHas: true, wantDown: true, wantPending: []int{3, 7}, wantPrevOK: false},
		{name: "a gap", version: 2, wantHas: false, wantPending: []int{3, 7}, wantPrev: 1, wantPrevOK: true},
		{name: "middle without down", version: 3, wantHas: true, wantDown: false, wantPending: []int{7}, wantPrev: 1, wantPrevOK: true},
		{name: "last", version: 7, wantHas: true, wantDown: true, wantPending: nil, wantPrev: 3, wantPrevOK: true},
		{name: "above the last", version: 9, wantHas: false, wantPending: nil, wantPrev: 7, wantPrevOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := src.Has(tt.version); got != tt.wantHas {
				t.Errorf("Has(%d) = %v, want %v", tt.version, got, tt.wantHas)
			}
			if _, got := src.Down(tt.version); got != tt.wantDown {
				t.Errorf("Down(%d) ok = %v, want %v", tt.version, got, tt.wantDown)
			}
			var pending []int
			for _, m := range src.Pending(tt.version) {
				pending = append(pending, m.Version)
			}
			if diff := cmp.Diff(tt.wantPending, pending); diff != "" {
				t.Errorf("Pending(%d) mismatch (-want +got):\n%s", tt.version, diff)
			}
			prev, ok := src.Prev(tt.version)
			if ok != tt.wantPrevOK || (ok && prev != tt.wantPrev) {
				t.Errorf("Prev(%d) = %d, %v, want %d, %v", tt.version, prev, ok, tt.wantPrev, tt.wantPrevOK)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatalf("os.Mkdir() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000001_a.up.sql"), []byte("CREATE TABLE A (Id INT64) PRIMARY KEY (Id);"), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	notes := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(notes, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	// A relative source resolves against the working directory, which for a test is the
	// package directory; the temporary directory is reached by a relative path from there.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() error = %v", err)
	}
	relative, err := filepath.Rel(cwd, dir)
	if err != nil {
		t.Fatalf("filepath.Rel() error = %v", err)
	}

	tests := []struct {
		name         string
		sourceURL    string
		wantVersions []int
		wantErr      string
	}{
		{name: "relative path", sourceURL: "file://" + relative, wantVersions: []int{1}},
		{name: "relative path with a dot", sourceURL: "file://./" + relative, wantVersions: []int{1}},
		{name: "absolute path", sourceURL: "file://" + dir, wantVersions: []int{1}},
		{name: "missing directory", sourceURL: "file://" + filepath.Join(root, "nowhere"), wantErr: "no such file or directory"},
		{name: "a file, not a directory", sourceURL: "file://" + notes, wantErr: "is not a directory"},
		{name: "not a file URL", sourceURL: "gs://bucket/migrations", wantErr: "is not a file:// URL"},
		{name: "bare path", sourceURL: dir, wantErr: "is not a file:// URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, err := Open(tt.sourceURL)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Open() error = %v, want one containing %q", err, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.sourceURL) {
					t.Errorf("Open() error = %v, want it to name the source %q", err, tt.sourceURL)
				}

				return
			}
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			if diff := cmp.Diff(tt.wantVersions, src.Versions()); diff != "" {
				t.Errorf("Versions() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
