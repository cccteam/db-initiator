package dbinitiator

import (
	"context"
	"testing"
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
		TargetDb:     "target_db",
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
			b, err := NewSpannerBackup(tt.args.ctx, cfg, container.opts...)
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

			wantTarget := "projects/" + cfg.ProjectID + "/instances/" + cfg.InstanceID + "/databases/" + cfg.TargetDb
			if b.TargetDb != wantTarget {
				t.Errorf("TargetConnectionString = %q, want %q", b.TargetDb, wantTarget)
			}
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
		SourceDb: "does_not_exist",
		TargetDb: "unused_target",
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
			b, err := NewSpannerBackup(ctx, cfg, container.opts...)
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
		TargetDb:   "unused_target",
	}

	b, err := NewSpannerBackup(ctx, cfg, container.opts...)
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
