# DB Initiator

A Go library for database testing and migrations. Spin up ephemeral containerized databases for integration tests or run migrations against existing databases.

## Features

- **Ephemeral test databases** – Start Docker containers for isolated integration testing
- **Database migrations** – Run schema and data migrations using [golang-migrate](https://github.com/golang-migrate/migrate)

## Supported Databases

| Database | Container | Migrations |
|----------|-----------|------------|
| PostgreSQL | ✓ | ✓* |
| Spanner | ✓ (emulator) | ✓ |

#### *PostgreSQL Limitations

Compared to the Spanner implementation, PostgreSQL currently has the following limitations:

- No separate schema vs data migrations (single `MigrateUp` only)
- No configurable migrations table name
- `PostgresMigrator` only supports `MigrateUp` (no down/drop migrations)

## License

See [LICENSE](LICENSE) for details.