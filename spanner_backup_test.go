package dbinitiator

import (
	"context"
	"testing"
)

func TestNewSpannerBackup(t *testing.T) {
	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		ctx        context.Context
		projectID  string
		instanceID string
		sourceDb   string
		targetDb   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "valid backup client",
			args: args{
				ctx:        context.Background(),
				projectID:  container.projectID,
				instanceID: container.instanceID,
				sourceDb:   "source_db",
				targetDb:   "target_db",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewSpannerBackup(tt.args.ctx, tt.args.projectID, tt.args.instanceID, tt.args.sourceDb, tt.args.targetDb, container.opts...)
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

			wantTarget := "projects/" + tt.args.projectID + "/instances/" + tt.args.instanceID + "/databases/" + tt.args.targetDb
			if b.TargetConnectionString != wantTarget {
				t.Errorf("TargetConnectionString = %q, want %q", b.TargetConnectionString, wantTarget)
			}
			if b.ProjectID != tt.args.projectID {
				t.Errorf("ProjectID = %q, want %q", b.ProjectID, tt.args.projectID)
			}
			if b.InstanceID != tt.args.instanceID {
				t.Errorf("InstanceID = %q, want %q", b.InstanceID, tt.args.instanceID)
			}
		})
	}
}

func TestSpannerBackup_Backup(t *testing.T) {
	// t.Skip("the Spanner emulator does not implement the backup API (CreateBackup returns Unimplemented); requires real Cloud Spanner")

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

	type args struct {
		sourceDatabase string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "backup non-existent database",
			args:    args{sourceDatabase: "does_not_exist"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			b, err := NewSpannerBackup(ctx, container.projectID, container.instanceID, tt.args.sourceDatabase, "unused_target", container.opts...)
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

func TestSpannerBackup_BackupRestore(t *testing.T) {
	t.Skip("the Spanner emulator does not implement the backup API (CreateBackup returns Unimplemented); requires real Cloud Spanner")

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	// Source database to be backed up.
	sourceDB, err := container.CreateDatabase(ctx, "br_source")
	if err != nil {
		t.Fatalf("container.CreateDatabase(source): %s", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	// Target database must already exist because Restore drops it before restoring.
	targetDB, err := container.CreateDatabase(ctx, "br_target")
	if err != nil {
		t.Fatalf("container.CreateDatabase(target): %s", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })

	sourceName := container.validDatabaseName("br_source")
	targetName := container.validDatabaseName("br_target")

	b, err := NewSpannerBackup(ctx, container.projectID, container.instanceID, sourceName, targetName, container.opts...)
	if err != nil {
		t.Fatalf("NewSpannerBackup(): %s", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if err := b.BackupRestore(ctx, sourceName, targetName); err != nil {
		t.Fatalf("SpannerBackup.BackupRestore() error = %v", err)
	}
}

func TestSpannerBackup_Restore(t *testing.T) {
	t.Skip("the Spanner emulator does not implement the backup API (RestoreBackup and CreateBackup returns Unimplemented); requires real Cloud Spanner")

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	// Source database to be backed up.
	sourceDB, err := container.CreateDatabase(ctx, "restore_source")
	if err != nil {
		t.Fatalf("container.CreateDatabase(source): %s", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	// Target database must already exist because Restore drops it before restoring.
	targetDB, err := container.CreateDatabase(ctx, "restore_target")
	if err != nil {
		t.Fatalf("container.CreateDatabase(target): %s", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })

	sourceName := container.validDatabaseName("restore_source")
	targetName := container.validDatabaseName("restore_target")

	b, err := NewSpannerBackup(ctx, container.projectID, container.instanceID, sourceName, targetName, container.opts...)
	if err != nil {
		t.Fatalf("NewSpannerBackup(): %s", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	backup, err := b.Backup(ctx)
	if err != nil {
		t.Fatalf("SpannerBackup.Backup(): %s", err)
	}

	if err := b.Restore(ctx, backup, targetName); err != nil {
		t.Fatalf("SpannerBackup.Restore() error = %v", err)
	}
}

func TestSpannerBackup_BackupCanceledContext(t *testing.T) {
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

	b, err := NewSpannerBackup(ctx, container.projectID, container.instanceID, sourceName, "unused_target", container.opts...)
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
