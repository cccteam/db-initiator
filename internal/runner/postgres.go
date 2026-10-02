package runner

import (
	"context"
	"hash/crc32"
	"strings"

	ccclogger "github.com/cccteam/logger"
	"github.com/go-playground/errors/v5"
	"github.com/jackc/pgx/v5"
)

// Beginner starts transactions: a pgxpool.Pool or a pgx.Conn.
type Beginner interface {
	// Begin starts a transaction.
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Postgres applies migrations to one PostgreSQL database and keeps one version table. Each
// file runs inside one transaction, under an advisory lock on the table's name, with the
// version row replaced in the same transaction: a failed file rolls back whole and the
// version stays where it was, never dirty. Statements that cannot run inside a transaction,
// such as CREATE INDEX CONCURRENTLY, are not supported.
type Postgres struct {
	db    Beginner
	table string
	lock  int64
}

// NewPostgres returns a runner keeping its versions in table.
func NewPostgres(db Beginner, table string) *Postgres {
	return &Postgres{
		db:    db,
		table: table,
		lock:  int64(crc32.ChecksumIEEE([]byte(table))),
	}
}

// Up applies every pending migration of the source, ascending. Each file's transaction
// reads the version again under the lock, so two runners on one database apply every file
// once between them. No pending migration is a normal result.
func (p *Postgres) Up(ctx context.Context, src *Source) error {
	log := ccclogger.FromCtx(ctx)

	v, err := p.Version(ctx)
	if err != nil {
		return err
	}

	current := -1
	if v.Known {
		current = v.Number
		if v.Dirty {
			return errors.Newf("the database is at %s; force the version that matches its real state", v)
		}
		if !src.Has(current) {
			return errors.Newf("the database is at version %d, which this build does not carry", current)
		}
	}

	applied := 0
	for _, m := range src.Pending(current) {
		done, err := p.applyUp(ctx, m)
		if err != nil {
			return err
		}
		if done {
			applied++
			log.Infof("Applied %s", m.File)
		}
	}
	if applied == 0 {
		log.Infof("No change: the database is at %s and the source has nothing above it", v)
	}

	return nil
}

// applyUp runs one up file in one transaction. It reports false when another runner applied
// the version first.
func (p *Postgres) applyUp(ctx context.Context, m *Migration) (bool, error) {
	return p.transact(ctx, func(tx pgx.Tx) (bool, error) {
		v, err := p.readTx(ctx, tx)
		if err != nil {
			return false, err
		}
		if v.Dirty {
			return false, errors.Newf("the database is at %s; force the version that matches its real state", v)
		}
		if v.Known && v.Number >= m.Version {
			return false, nil
		}

		if err := p.exec(ctx, tx, m); err != nil {
			return false, errors.Wrapf(err, "%s failed and was rolled back; the database stays at %s", m.File, v)
		}

		return true, p.writeTx(ctx, tx, Version{Number: m.Version, Known: true})
	})
}

// Down reverts versions from the current one to no version, each in one transaction.
func (p *Postgres) Down(ctx context.Context, src *Source) error {
	log := ccclogger.FromCtx(ctx)

	if err := p.ensureTable(ctx); err != nil {
		return err
	}

	reverted := 0
	for {
		more, err := p.transact(ctx, func(tx pgx.Tx) (bool, error) {
			v, err := p.readTx(ctx, tx)
			if err != nil {
				return false, err
			}
			if !v.Known {
				return false, nil
			}
			if v.Dirty {
				return false, errors.Newf("the database is at %s; force the version that matches its real state before migrating down", v)
			}
			if !src.Has(v.Number) {
				return false, errors.Newf("the database is at version %d, which this build does not carry", v.Number)
			}

			if m, ok := src.Down(v.Number); ok {
				if err := p.exec(ctx, tx, m); err != nil {
					return false, errors.Wrapf(err, "%s failed and was rolled back; the database stays at %s", m.File, v)
				}
				log.Infof("Reverted %s", m.File)
			}
			reverted++

			prev, ok := src.Prev(v.Number)
			if !ok {
				return false, p.writeTx(ctx, tx, Version{})
			}

			return true, p.writeTx(ctx, tx, Version{Number: prev, Known: true})
		})
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	if reverted == 0 {
		log.Info("No change: the database has no version to revert")
	}

	return nil
}

// Version reads the version table, creating it when the database has none.
func (p *Postgres) Version(ctx context.Context) (Version, error) {
	if err := p.ensureTable(ctx); err != nil {
		return Version{}, err
	}

	var v Version
	if _, err := p.transact(ctx, func(tx pgx.Tx) (bool, error) {
		var err error
		v, err = p.readTx(ctx, tx)

		return false, err
	}); err != nil {
		return Version{}, err
	}

	return v, nil
}

// Force sets the version clean. Version -1 deletes the row, so the database has no version.
func (p *Postgres) Force(ctx context.Context, version int) error {
	if version < -1 {
		return errors.Newf("version %d cannot be forced: versions are 0 or above, and -1 means no version", version)
	}

	if err := p.ensureTable(ctx); err != nil {
		return err
	}

	v := Version{}
	if version >= 0 {
		v = Version{Number: version, Known: true}
	}
	if _, err := p.transact(ctx, func(tx pgx.Tx) (bool, error) {
		return false, p.writeTx(ctx, tx, v)
	}); err != nil {
		return err
	}
	ccclogger.FromCtx(ctx).Infof("Forced %s to %s", p.table, v)

	return nil
}

// ensureTable creates the version table under the lock, so two runners starting together
// do not race on it.
func (p *Postgres) ensureTable(ctx context.Context) error {
	_, err := p.transact(ctx, func(tx pgx.Tx) (bool, error) {
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+p.identifier()+" (version bigint not null primary key, dirty boolean not null)"); err != nil {
			return false, errors.Wrapf(err, "creating the version table %s", p.table)
		}

		return false, nil
	})

	return err
}

// transact runs fn in one transaction holding the advisory lock, and commits unless fn
// fails.
func (p *Postgres) transact(ctx context.Context, fn func(tx pgx.Tx) (bool, error)) (bool, error) {
	tx, err := p.db.Begin(ctx)
	if err != nil {
		return false, errors.Wrap(err, "Begin()")
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", p.lock); err != nil {
		return false, errors.Wrap(err, "pg_advisory_xact_lock()")
	}

	result, err := fn(tx)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, errors.Wrap(err, "pgx.Tx.Commit()")
	}

	return result, nil
}

// exec runs a file's text as one multi-statement command. A file with no statements runs
// nothing.
func (p *Postgres) exec(ctx context.Context, tx pgx.Tx, m *Migration) error {
	if strings.TrimSpace(m.SQL) == "" {
		return nil
	}

	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return errors.Wrap(err, "pgx.Tx.Exec()")
	}

	return nil
}

// readTx reads the version row inside a transaction.
func (p *Postgres) readTx(ctx context.Context, tx pgx.Tx) (Version, error) {
	var (
		version int64
		dirty   bool
	)
	err := tx.QueryRow(ctx, "SELECT version, dirty FROM "+p.identifier()+" LIMIT 1").Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, nil
	}
	if err != nil {
		return Version{}, errors.Wrapf(err, "reading the version from %s", p.table)
	}
	if version < 0 {
		return Version{}, nil
	}

	return Version{Number: int(version), Known: true, Dirty: dirty}, nil
}

// writeTx replaces the version row inside a transaction.
func (p *Postgres) writeTx(ctx context.Context, tx pgx.Tx, v Version) error {
	if _, err := tx.Exec(ctx, "DELETE FROM "+p.identifier()); err != nil {
		return errors.Wrapf(err, "clearing %s", p.table)
	}
	if !v.Known {
		return nil
	}

	if _, err := tx.Exec(ctx, "INSERT INTO "+p.identifier()+" (version, dirty) VALUES ($1, $2)", int64(v.Number), v.Dirty); err != nil {
		return errors.Wrapf(err, "writing %s to %s", v, p.table)
	}

	return nil
}

// identifier is the quoted table name.
func (p *Postgres) identifier() string {
	return pgx.Identifier{p.table}.Sanitize()
}
