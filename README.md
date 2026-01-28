# DB Initiator

A Go library for database testing and migrations. Spin up ephemeral containerized databases for integration tests or run migrations against existing databases.

## Features

- **Ephemeral test databases** – Start Docker containers for isolated integration testing
- **Database migrations** – Run schema and data migrations using [golang-migrate](https://github.com/golang-migrate/migrate)

## Supported Databases

| Database | Container | Migrations |
|----------|-----------|------------|
| PostgreSQL | ✓ | ✓ |
| Spanner | ✓ (emulator) | ✓ |

## Installation

```bash
go get github.com/cccteam/db-initiator
```

## Quick Start

### PostgreSQL Container

```go
ctx := context.Background()

// Start a PostgreSQL container
pg, err := dbinitiator.NewPostgresContainer(ctx, "16")
if err != nil {
    log.Fatal(err)
}
defer pg.Terminate(ctx)

// Create a test database with migrations
db, err := pg.CreateDatabase(ctx, "mytest")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

if err := db.RunMigrations("file://./migrations"); err != nil {
    log.Fatal(err)
}
```

### Spanner Emulator

```go
ctx := context.Background()

// Start a Spanner emulator container
spanner, err := dbinitiator.NewSpannerContainer(ctx, "latest")
if err != nil {
    log.Fatal(err)
}
defer spanner.Terminate(ctx)

// Create a test database
db, err := spanner.CreateDatabase(ctx, "mytest")
if err != nil {
    log.Fatal(err)
}
```

### Connect to Existing Database

```go
// Connect to an existing Spanner database
migrator, err := dbinitiator.ConnectToSpanner(ctx, "project-id", "instance-id", "database")
if err != nil {
    log.Fatal(err)
}
defer migrator.Close()

if err := migrator.RunSchemaMigrations("file://./migrations"); err != nil {
    log.Fatal(err)
}
```

## License

See [LICENSE](LICENSE) for details.