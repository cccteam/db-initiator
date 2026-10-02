# DB Initiator

A Go library for database testing and migrations. Spin up ephemeral containerized databases for integration tests or run migrations against existing databases.

## Features

- **Ephemeral test databases** – Start Docker containers for isolated integration testing
- **Database migrations** – Run schema and data migrations with the library's own runner, which continues a failed Spanner file from its failed statement once the cause is fixed
- **Backup and restore** - (Spanner only) - Supports backing up and restoring Spanner databases.  Supports setting backup age limit (in seconds) via `MaxBackupAge` property

## Supported Databases

| Database | Container | Migrations | Backup/Restore |
|----------|-----------|------------|----------------|
| PostgreSQL | ✓ | ✓* | ✗ |
| Spanner | ✓ (emulator) | ✓ | ✓ |

#### *PostgreSQL Limitations

Compared to the Spanner implementation, PostgreSQL currently has the following limitations:

- No separate schema vs data migrations (`MigrateUpData`, `DataVersion` and `ForceData` return an error)
- No configurable migrations table name
- `PostgresMigrator` does not implement `MigrateDropSchema`

## Migrations

A migration source is a directory, given as `file://<path>`: relative to the working directory, or absolute as `file:///path`. It holds files named `<version>_<name>.up.sql` and, optionally, `<version>_<name>.down.sql`. The version is a number compared numerically, so leading zeros are fine. A `.sql` file whose name does not follow the convention is refused by name; files without the `.sql` suffix are ignored.

`MigrateUpSchema` and `MigrateUpData` apply every file above the database's current version, ascending, and record the version in the `SchemaMigrations` and `DataMigrations` tables on Spanner, or `schema_migrations` on PostgreSQL. A database already at the last version is a normal result, not an error. `SchemaVersion`, `DataVersion`, `ForceSchema` and `ForceData` read and set the version tables; forcing `-1` leaves the database with no version.

### Spanner

A file is split into statements with the comments stripped, since Spanner's DDL API refuses them. `INSERT`, `UPDATE` and `DELETE` statements are DML and every other statement is DDL. Consecutive statements of one kind form a group: a DDL group goes to Spanner as one batch, a DML group runs in one read-write transaction.

Spanner validates a DDL batch as a whole before applying anything, then applies its statements in order, and a statement that fails at apply time (a unique index over duplicate values, a foreign key over rows with no parent) leaves the earlier ones applied. The runner records how far the file got on the version row, in the `Applied`, `Checkpoint` and `Operation` columns, which it adds to a version table the earlier releases created, and returns a `DirtyError` naming the file, the statement and Spanner's message. Fix the cause and run again: the file continues from the failed statement. A DML group that fails rolls back whole, and nothing of it is applied.

The run refuses, and says how to recover, when the file changed in its already-applied part (restore the file, or force a version) and when the database is dirty from a run that recorded no progress, as releases before 0.4 left it (force the version that matches the database's real state).

### PostgreSQL

Each file runs inside one transaction under an advisory lock, with the version row replaced in the same transaction: a file that fails rolls back whole and the version stays where it was, never dirty. Two runners started together on one database apply every file once between them. Statements that cannot run inside a transaction, such as `CREATE INDEX CONCURRENTLY`, are not supported.

## Upgrading from 0.3

Release 0.4 replaces the golang-migrate library with the runner above. Every live database carries over: the version tables keep their names and columns, and gain the three progress columns on the first run. In each module that uses db-initiator:

- delete the `replace github.com/golang-migrate/migrate/v4 ... => github.com/jtwatson/migrate/v4 ...` line from `go.mod` and run `go mod tidy`;
- delete the `errors.Is(err, migrate.ErrNoChange)` checks and the import: a database with nothing to apply is a nil result now.

## License

See [LICENSE](LICENSE) for details.
