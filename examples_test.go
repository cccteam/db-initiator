package dbinitiator

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func ExampleSpannerContainer_CreateDatabase() {
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := container.Close()
		log.Println("Error closing container:", closeErr)
	}()

	db, err := container.CreateDatabase(ctx, "test_db")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := db.Close()
		log.Println("Error closing database:", closeErr)
	}()

	// Use the database..
	db.Single().Query(ctx, spanner.NewStatement("SELECT 1"))
}

func ExampleSpannerMigrator_MigrateUpSchema() {
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := container.Close()
		log.Println("Error closing container:", closeErr)
	}()

	db, err := container.CreateDatabase(ctx, "test_db")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := db.Close()
		log.Println("Error closing database:", closeErr)
	}()

	migrator, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, "test_db", container.opts...)
	if err != nil {
		panic(err)
	}

	if err := migrator.MigrateUpSchema(ctx, "file://testdata/spanner/migrations"); err != nil {
		panic(err)
	}
}

func ExampleSpannerMigrator_MigrateUpData() {
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := container.Close()
		log.Println("Error closing container:", closeErr)
	}()

	db, err := container.CreateDatabase(ctx, "test_db")
	if err != nil {
		panic(err)
	}
	defer func() {
		closeErr := db.Close()
		log.Println("Error closing database:", closeErr)
	}()

	migrator, err := NewSpannerMigrator(ctx, container.projectID, container.instanceID, "test_db", container.opts...)
	if err != nil {
		panic(err)
	}

	if err := migrator.MigrateUpData(ctx, "file://testdata/spanner/migrations"); err != nil {
		panic(err)
	}
}

func ExamplePostgresMigrator_MigrateUpSchema() {
	ctx := context.Background()
	container, err := NewPostgresContainer(ctx, "latest")
	if err != nil {
		panic(err)
	}
	defer container.Close()

	db, err := container.CreateDatabase(ctx, "test_db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	migrator := NewPostgresMigrator(container.superUsername, "password", container.host, container.port.Port(), db.dbName)

	if err := migrator.MigrateUpSchema(ctx, "file://testdata/postgres/migrations"); err != nil {
		panic(err)
	}
}
