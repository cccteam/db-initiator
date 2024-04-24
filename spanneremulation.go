// Implements tooling to setup a Spanner database configured for running integration tests against.
package dbinitializer

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/docker/go-connections/nat"
	"github.com/go-playground/errors/v5"
	_ "github.com/golang-migrate/migrate/v4/database/spanner" // spanner driver for the migrate package
	_ "github.com/golang-migrate/migrate/v4/source/file"      // up/down script file source driver for the migrate package
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultPort       = "9010/tcp"
	defaultProjectID  = "unit-testing"
	defaultInstanceID = "test-instance"
)

// Container represents a docker container running a spanner instance.
type Container struct {
	testcontainers.Container
	admin      *database.DatabaseAdminClient
	opts       []option.ClientOption
	port       string
	projectID  string
	instanceID string

	mu      sync.Mutex
	dbCount int
}

// NewContainer returns a initialized SpannerContainer ready to run to create databases for unit tests
func NewContainer(ctx context.Context) (*Container, error) {
	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			Started: true,
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "gcr.io/cloud-spanner-emulator/emulator:latest",
				WaitingFor:   wait.ForLog("Cloud Spanner emulator running"),
				ExposedPorts: []string{defaultPort},
			},
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "testcontainers.GenericContainer()")
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get host for container")
	}

	externalPort, err := container.MappedPort(ctx, nat.Port(defaultPort))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get external port for exposed port %s", defaultPort)
	}

	endPoint := fmt.Sprintf("%s:%s", host, externalPort.Port())

	opts := []option.ClientOption{
		option.WithEndpoint(endPoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	if err := NewInstance(ctx, defaultProjectID, defaultInstanceID, opts...); err != nil {
		return nil, errors.Wrap(err, "failed to create spanner instance")
	}

	admin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	return &Container{
		Container:  container,
		admin:      admin,
		opts:       opts,
		port:       defaultPort,
		projectID:  defaultProjectID,
		instanceID: defaultInstanceID,
	}, nil
}

// CreateTestDatabase creates a database with dbName. Each test should create their own database for testing
func (sp *Container) CreateTestDatabase(ctx context.Context, dbName string) (*DB, error) {
	dbName = sp.validDatabaseName(dbName)

	db, err := newDatabase(ctx, sp.admin, sp.projectID, sp.instanceID, dbName, sp.opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create spanner database %s", dbName)
	}

	return db, nil
}

// Close cleans up open resouces
func (sp *Container) Close() error {
	if err := sp.admin.Close(); err != nil {
		return errors.Wrap(err, "database.DatabaseAdminClient.Close()")
	}

	return nil
}

func (sp *Container) validDatabaseName(dbName string) string {
	b := []byte(dbName)
	b = bytes.ToLower(b)

	for i, v := range b {
		if !bytes.ContainsAny([]byte{v}, "1234567890abcdefghijklmnopqrstuvwxyz-_") {
			b[i] = '-'
		}
	}

	b = bytes.Trim(b, "-_")
	dbName = string(b)

	if l := len(dbName); l > 30 {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		sp.dbCount++
		dbName = fmt.Sprintf("db%d-%s", sp.dbCount, dbName[l-20:])
	}

	return dbName
}
