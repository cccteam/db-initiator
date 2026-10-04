// Implements tooling to start a Firestore emulator for running integration tests against.
package dbinitiator

import (
	"bytes"
	"context"
	"net"
	"os"
	"time"

	"github.com/go-playground/errors/v5"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultFirestorePort = "8080/tcp"
	// firestoreRulesFile is where a rules file given with [WithFirestoreRules] is copied
	// to inside the container, and what the emulator's --rules flag names.
	firestoreRulesFile = "/firestore.rules"
	// firestoreStartupTimeout is how long the emulator has to answer on its root URL
	// once the container runs: the Cloud SDK image starts a Java process, which takes
	// longer than the Spanner emulator does.
	firestoreStartupTimeout = 2 * time.Minute
)

// FirestoreContainer represents a docker container running the Firestore emulator.
// [FirestoreContainer.Terminate] stops the container and [FirestoreContainer.Close]
// should be called to cleanup resources, as with [SpannerContainer].
type FirestoreContainer struct {
	testcontainers.Container
	host string
}

// FirestoreOption changes how [NewFirestoreContainer] starts the emulator.
type FirestoreOption func(*firestoreOptions)

// firestoreOptions are the settings the options write.
type firestoreOptions struct {
	rulesPath string
}

// WithFirestoreRules copies the security rules file at path into the container and starts
// the emulator with it, so requests made without the emulator's owner credential are
// answered as the rules say. Without it the emulator starts with no rules file.
func WithFirestoreRules(path string) FirestoreOption {
	return func(o *firestoreOptions) {
		o.rulesPath = path
	}
}

// NewFirestoreContainer starts the Firestore emulator from the Cloud SDK emulators image
// (gcr.io/google.com/cloudsdktool/google-cloud-cli:<imageVersion>-emulators), waits until
// it answers, and returns a [FirestoreContainer] whose [FirestoreContainer.Host] is the
// address the FIRESTORE_EMULATOR_HOST variable takes. imageVersion is the Cloud SDK
// version, such as "562.0.0".
// [FirestoreContainer.Terminate] stops the container and [FirestoreContainer.Close]
// should be called to cleanup resources.
func NewFirestoreContainer(ctx context.Context, imageVersion string, opts ...FirestoreOption) (*FirestoreContainer, error) {
	var o firestoreOptions
	for _, opt := range opts {
		opt(&o)
	}

	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/google-cloud-cli:" + imageVersion + "-emulators",
		Cmd:          []string{"gcloud", "emulators", "firestore", "start", "--host-port=0.0.0.0:8080"},
		WaitingFor:   wait.ForHTTP("/").WithPort(defaultFirestorePort).WithStartupTimeout(firestoreStartupTimeout),
		ExposedPorts: []string{defaultFirestorePort},
	}
	if o.rulesPath != "" {
		// The file is read before the container starts, so a missing file is refused by
		// its path and no container is left behind.
		rules, err := os.ReadFile(o.rulesPath)
		if err != nil {
			return nil, errors.Wrap(err, "os.ReadFile()")
		}
		req.Files = []testcontainers.ContainerFile{{
			Reader:            bytes.NewReader(rules),
			ContainerFilePath: firestoreRulesFile,
			FileMode:          0o644,
		}}
		req.Cmd = append(req.Cmd, "--rules="+firestoreRulesFile)
	}

	firestoreC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:          true,
		ContainerRequest: req,
	})
	if err != nil {
		return nil, errors.Wrap(err, "testcontainers.GenericContainer()")
	}

	host, err := firestoreHost(ctx, firestoreC)
	if err != nil {
		if terminateErr := firestoreC.Terminate(context.WithoutCancel(ctx)); terminateErr != nil {
			return nil, errors.Wrapf(err, "the container was not terminated either: %v", terminateErr)
		}

		return nil, err
	}

	return &FirestoreContainer{
		Container: firestoreC,
		host:      host,
	}, nil
}

// firestoreHost is the host:port the started container's emulator answers on from
// outside the container.
func firestoreHost(ctx context.Context, firestoreC testcontainers.Container) (string, error) {
	host, err := firestoreC.Host(ctx)
	if err != nil {
		return "", errors.Wrap(err, "failed to get host for container")
	}

	externalPort, err := firestoreC.MappedPort(ctx, defaultFirestorePort)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get external port for exposed port %s", defaultFirestorePort)
	}

	return net.JoinHostPort(host, externalPort.Port()), nil
}

// Host returns the emulator's host:port, the value the FIRESTORE_EMULATOR_HOST variable
// takes: a Firestore client opened with the variable set talks to the emulator.
func (fc *FirestoreContainer) Host() string {
	return fc.host
}

// Close cleans up open resources. The emulator holds none outside its container, which
// [FirestoreContainer.Terminate] stops, so Close returns nil; it is here so a test stops
// the two emulators the same way.
func (fc *FirestoreContainer) Close() error {
	return nil
}
