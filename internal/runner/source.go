// Package runner applies migration files to a database and keeps the version table that
// says how far they got. It replaces the golang-migrate library: the same file names, the
// same version tables, and on Spanner a file that fails part-way records its progress so the
// next run continues from the failed statement.
package runner

import (
	"io/fs"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/go-playground/errors/v5"
)

// Direction is the way a migration file moves the database.
type Direction string

const (
	// Up applies a version.
	Up Direction = "up"
	// Down reverts a version.
	Down Direction = "down"
)

// Migration is one migration file.
type Migration struct {
	// Version is the number the file name starts with, compared numerically.
	Version int
	// Name is the part of the file name between the version and the direction.
	Name string
	// Direction is up or down.
	Direction Direction
	// File is the file name, as the source lists it.
	File string
	// SQL is the file's text.
	SQL string
}

// Source is the migration files of one directory: every version has an up file, and may
// have a down file.
type Source struct {
	versions []int
	ups      map[int]*Migration
	downs    map[int]*Migration
}

// fileName matches <version>_<name>.up.sql and <version>_<name>.down.sql.
var fileName = regexp.MustCompile(`^(\d+)_(.*)\.(up|down)\.sql$`)

const (
	sourceScheme = "file://"
	sqlSuffix    = ".sql"
)

// Open reads the migration files at a file://<path> source. The path is relative to the
// working directory, or absolute as file:///path.
func Open(sourceURL string) (*Source, error) {
	dir, ok := strings.CutPrefix(sourceURL, sourceScheme)
	if !ok {
		return nil, errors.Newf("migration source %q is not a file:// URL", sourceURL)
	}

	info, err := os.Stat(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "migration source %q", sourceURL)
	}
	if !info.IsDir() {
		return nil, errors.Newf("migration source %q is not a directory", sourceURL)
	}

	src, err := Load(os.DirFS(dir))
	if err != nil {
		return nil, errors.Wrapf(err, "migration source %q", sourceURL)
	}

	return src, nil
}

// Load reads the migration files at the root of fsys. A .sql file whose name does not fit
// the convention is refused by name; a file without the .sql suffix is ignored; two files
// with the same version and direction are refused; a down file without an up file is refused.
func Load(fsys fs.FS) (*Source, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, errors.Wrap(err, "fs.ReadDir()")
	}

	src := &Source{
		ups:   map[int]*Migration{},
		downs: map[int]*Migration{},
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), sqlSuffix) {
			continue
		}

		m, err := parseFileName(entry.Name())
		if err != nil {
			return nil, err
		}

		raw, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, errors.Wrapf(err, "fs.ReadFile(%q)", entry.Name())
		}
		m.SQL = string(raw)

		files := src.ups
		if m.Direction == Down {
			files = src.downs
		}
		if other, dup := files[m.Version]; dup {
			return nil, errors.Newf("%s and %s are both the %s migration of version %d", other.File, m.File, m.Direction, m.Version)
		}
		files[m.Version] = m
	}

	for version, m := range src.downs {
		if _, ok := src.ups[version]; !ok {
			return nil, errors.Newf("%s has no up migration for version %d", m.File, version)
		}
	}

	src.versions = make([]int, 0, len(src.ups))
	for version := range src.ups {
		src.versions = append(src.versions, version)
	}
	slices.Sort(src.versions)

	return src, nil
}

// parseFileName reads the version, name and direction out of a migration file name.
func parseFileName(name string) (*Migration, error) {
	match := fileName.FindStringSubmatch(name)
	if match == nil {
		return nil, errors.Newf("%s is not a migration file name: want <version>_<name>.up.sql or <version>_<name>.down.sql", name)
	}

	version, err := strconv.Atoi(match[1])
	if err != nil {
		return nil, errors.Wrapf(err, "%s: the version is not a number this runner can hold", name)
	}

	return &Migration{
		Version:   version,
		Name:      match[2],
		Direction: Direction(match[3]),
		File:      name,
	}, nil
}

// Versions lists the source's versions, ascending.
func (s *Source) Versions() []int {
	return slices.Clone(s.versions)
}

// Has reports whether the source carries a version.
func (s *Source) Has(version int) bool {
	_, ok := s.ups[version]

	return ok
}

// Up returns a version's up migration.
func (s *Source) Up(version int) (*Migration, bool) {
	m, ok := s.ups[version]

	return m, ok
}

// Down returns a version's down migration, if the version has one.
func (s *Source) Down(version int) (*Migration, bool) {
	m, ok := s.downs[version]

	return m, ok
}

// Pending lists the up migrations above a version, ascending. Pass -1 for a database with
// no version yet.
func (s *Source) Pending(current int) []*Migration {
	var pending []*Migration
	for _, version := range s.versions {
		if version > current {
			pending = append(pending, s.ups[version])
		}
	}

	return pending
}

// Prev returns the highest version below the given one, or false when there is none.
func (s *Source) Prev(version int) (int, bool) {
	prev, found := -1, false
	for _, v := range s.versions {
		if v >= version {
			break
		}
		prev, found = v, true
	}

	return prev, found
}
