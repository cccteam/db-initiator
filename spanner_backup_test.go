package dbinitiator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewSpannerBackup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	cfg := SpannerBackup{
		ProjectID:    container.projectID,
		InstanceID:   container.instanceID,
		SourceDb:     "source_db",
		MaxBackupAge: 600,
	}

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "valid backup client",
			args: args{
				ctx: context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := NewSpannerBackup(tt.args.ctx, &cfg, container.opts...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewSpannerBackup() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			t.Cleanup(func() {
				if err := b.Close(); err != nil {
					t.Errorf("SpannerBackup.Close() err = %v", err)
				}
			})

			if b.ProjectID != cfg.ProjectID {
				t.Errorf("ProjectID = %q, want %q", b.ProjectID, cfg.ProjectID)
			}
			if b.InstanceID != cfg.InstanceID {
				t.Errorf("InstanceID = %q, want %q", b.InstanceID, cfg.InstanceID)
			}
		})
	}
}

func TestSpannerBackup_Backup(t *testing.T) {
	// This only tests the scenario where the source database doesnt exist on the request to backup the db.
	// The spanner emulator does not support CreateBackup()
	t.Parallel()
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	// Create the source database that will be backed up.
	sourceDB, err := container.CreateDatabase(ctx, "source_database")
	if err != nil {
		t.Fatalf("container.CreateDatabase(): %s", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	sourceName := container.validDatabaseName("source_database")
	t.Logf("sourceName: %s\n", sourceName)

	cfg := SpannerBackup{
		ProjectID:  container.projectID,
		InstanceID: container.instanceID,
		SourceDb:   "does_not_exist",
	}
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "backup non-existent database",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			b, err := NewSpannerBackup(ctx, &cfg, container.opts...)
			if err != nil {
				t.Fatalf("NewSpannerBackup(): %s", err)
			}
			t.Cleanup(func() { _ = b.Close() })

			backup, err := b.Backup(ctx)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SpannerBackup.Backup() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if backup == nil {
				t.Fatal("SpannerBackup.Backup() returned nil backup with no error")
			}
			if backup.Name == "" {
				t.Error("SpannerBackup.Backup() returned backup with empty Name")
			}
		})
	}
}

func TestSpannerBackup_BackupCanceledContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	sourceDB, err := container.CreateDatabase(ctx, "cancel_source")
	if err != nil {
		t.Fatalf("container.CreateDatabase(): %s", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	sourceName := container.validDatabaseName("cancel_source")

	cfg := SpannerBackup{
		ProjectID:  container.projectID,
		InstanceID: container.instanceID,
		SourceDb:   sourceName,
	}

	b, err := NewSpannerBackup(ctx, &cfg, container.opts...)
	if err != nil {
		t.Fatalf("NewSpannerBackup(): %s", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := b.Backup(canceledCtx); err == nil {
		t.Fatal("SpannerBackup.Backup() with canceled context error = nil, want error")
	}
}

func TestSpannerBackup_RestoreTargetExists(t *testing.T) {
	// The spanner emulator does not support RestoreDatabase(). This exercises the
	// pre-flight check that rejects a restore when the target database already exists,
	// which returns before RestoreDatabase() is ever called.
	t.Parallel()
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	// Create the target database so it already exists at restore time.
	targetDB, err := container.CreateDatabase(ctx, "restore_target")
	if err != nil {
		t.Fatalf("container.CreateDatabase(): %s", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })

	targetName := container.validDatabaseName("restore_target")

	cfg := SpannerBackup{
		ProjectID:  container.projectID,
		InstanceID: container.instanceID,
	}

	b, err := NewSpannerBackup(ctx, &cfg, container.opts...)
	if err != nil {
		t.Fatalf("NewSpannerBackup(): %s", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	backup := &adminpb.Backup{
		Name: fmt.Sprintf("projects/%s/instances/%s/backups/some_backup", container.projectID, container.instanceID),
	}

	err = b.Restore(ctx, backup, targetName)
	want := fmt.Sprintf("target database %s exists and must be dropped", targetName)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("SpannerBackup.Restore() error = %v, want error container %q", err, want)
	}
}

func TestSpannerBackup_validateDatabaseBackupAge(t *testing.T) {
	t.Parallel()

	const maxBackupAge int64 = 600

	tests := []struct {
		name string
		// age is how long ago the backup was created.
		age  time.Duration
		want bool
	}{
		{
			name: "just under max age is eligible for reuse",
			age:  time.Duration(maxBackupAge-2) * time.Second,
			want: true,
		},
		{
			name: "at max age is not eligible",
			age:  time.Duration(maxBackupAge) * time.Second,
			want: false,
		},
		{
			name: "just over max age is not eligible",
			age:  time.Duration(maxBackupAge+2) * time.Second,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &SpannerBackup{MaxBackupAge: maxBackupAge}
			backup := &adminpb.Backup{
				CreateTime: timestamppb.New(time.Now().Add(-tt.age)),
			}

			if got, _ := s.validateDatabaseBackupAge(backup); got != tt.want {
				t.Errorf("validateDatabaseBackupAge() = %v, want %v", got, tt.want)
			}
		})
	}
}
