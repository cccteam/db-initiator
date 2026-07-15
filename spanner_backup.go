package dbinitiator

import (
	"context"
	"fmt"
	"log"
	"time"

	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errNoBackups = errors.New("no backups found")

type SpannerBackup struct {
	TargetDb     string
	SourceDb     string
	ProjectID    string
	InstanceID   string
	admin        *spannerDB.DatabaseAdminClient
	MaxBackupAge int64
}

func NewSpannerBackup(ctx context.Context, cfg *SpannerBackup, opts ...option.ClientOption) (*SpannerBackup, error) {
	tgtDbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.ProjectID, cfg.InstanceID, cfg.TargetDb)
	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "spannerDB.NewDatabaseAdminClient()")
	}

	return &SpannerBackup{
		TargetDb:     tgtDbStr,
		SourceDb:     cfg.SourceDb,
		admin:        adminClient,
		ProjectID:    cfg.ProjectID,
		InstanceID:   cfg.InstanceID,
		MaxBackupAge: cfg.MaxBackupAge,
	}, nil
}

func (s *SpannerBackup) getMostRecentBackup(ctx context.Context) (*adminpb.Backup, bool, error) {
	log.Println("getting most recent backups")
	instance := fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID)
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.ProjectID, s.InstanceID, s.SourceDb)
	// Filter databases on exact match and only backups that are "READY" - meaning they are ready to be restored elsewhere
	filter := fmt.Sprintf("database=%q AND state:READY", db)
	req := &adminpb.ListBackupsRequest{
		Parent: instance,
		Filter: filter,
	}

	// GCP Docs state the items returned are sorted by createTime in descending order, therefore the first non-nil will be the most recent.
	// https://pkg.go.dev/cloud.google.com/go/spanner/admin/database/apiv1#DatabaseAdminClient.ListBackups

	backupIt := s.admin.ListBackups(ctx, req)
	backup, err := backupIt.Next()
	if errors.Is(err, iterator.Done) {
		return nil, false, errNoBackups
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "getMostRecentBackup()")
	}

	eligible := s.validateDatabaseBackupAge(backup)
	if !eligible {
		log.Printf("recent backup age does not satisfy age requirement: %d seconds. taking fresh backup\n", s.MaxBackupAge)

		return backup, false, nil
	}

	return backup, true, nil
}

func (s *SpannerBackup) Backup(ctx context.Context) (*adminpb.Backup, error) {
	instance := fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID)
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.ProjectID, s.InstanceID, s.SourceDb)
	log.Printf("preparing to back up '%s' database\n", s.SourceDb)

	if err := s.checkExistingDatabase(ctx, s.SourceDb); err != nil {
		return nil, errors.Wrap(err, "checkExistingDatabase()")
	}

	backup, ok, err := s.getMostRecentBackup(ctx)

	switch {
	case err == nil && ok:
		return backup, nil
	case errors.Is(err, errNoBackups):
		log.Printf("no backups found for database: %s. proceeding to take fresh backup.", s.SourceDb)
	case err != nil:
		return nil, errors.Wrap(err, "Backup()")
	}

	ts := time.Now().AddDate(0, 0, 7).UTC()                                                     // Will back up for 1 week
	backupStamp := fmt.Sprintf("%s%03d", ts.Format("20060102_150405"), ts.Nanosecond()/1000000) // The display name of the restored database
	req := &adminpb.CreateBackupRequest{
		Parent:   instance,
		BackupId: s.SourceDb + "_backup_" + backupStamp,
		Backup: &adminpb.Backup{
			Database:   database,
			ExpireTime: timestamppb.New(ts),
		},
	}
	log.Printf("generated backup request for %s\n", s.SourceDb)
	op, err := s.admin.CreateBackup(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "s.admin.CreateBackup()")
	}
	log.Println("running backup...")

	ticker := time.NewTicker(60 * time.Second) // 60s polling time to get metadata.  GCP will show 0% for the backup until about done.
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "Backup()")
		case <-ticker.C:
			backup, err := op.Poll(ctx)
			if err != nil {
				log.Println("polling error: ", err)
				switch status.Code(err) {
				case codes.Canceled, codes.NotFound, codes.PermissionDenied,
					codes.Unauthenticated, codes.InvalidArgument, codes.FailedPrecondition, codes.Unimplemented:
					return nil, errors.Wrap(err, "Backup() polling error")
				default:
					continue
				}
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println(errors.Wrap(err, "op.Metadata()"))

				continue
			}
			if meta != nil {
				progress := meta.GetProgress()
				log.Printf("state: %s  progress: %d%%\n", meta.Database, progress.GetProgressPercent())
			}
			if op.Done() {
				log.Printf("backup complete: %s  size: %d bytes\n", backup.Name, backup.SizeBytes)

				return backup, nil
			}
		}
	}
}

func (s *SpannerBackup) Restore(ctx context.Context, backup *adminpb.Backup, targetDatabase string) error {
	// Spanner emulator does not support RestoreDatabase()
	log.Println("checking for existing target database: ", targetDatabase)

	err := s.checkExistingDatabase(ctx, targetDatabase)
	if err == nil {
		return errors.Newf("target database %s exists and must be dropped", targetDatabase)
	}
	if status.Code(err) != codes.NotFound {
		return errors.Wrap(err, "checkExistingDatabase()")
	}

	req := &adminpb.RestoreDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID), // Spanner Instance
		DatabaseId: targetDatabase,                                                     // Target Database to restore TO
		Source: &adminpb.RestoreDatabaseRequest_Backup{
			Backup: backup.Name, // Restore FROM
		},
	}

	log.Printf("restoring %s\n", backup.Name)
	op, err := s.admin.RestoreDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.RestoreDatabase()")
	}

	ticker := time.NewTicker(60 * time.Second) // 60s polling time to get metadata.  GCP will show 0% for the backup until about done.
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "Restore()")
		case <-ticker.C:
			restore, err := op.Poll(ctx)
			if err != nil {
				log.Println("polling error: ", err)
				switch status.Code(err) {
				case codes.Canceled, codes.NotFound, codes.PermissionDenied,
					codes.Unauthenticated, codes.InvalidArgument, codes.FailedPrecondition, codes.Unimplemented:
					return errors.Wrap(err, "Restore() polling error")
				default:
					continue
				}
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println(errors.Wrap(err, "op.Metadata()"))

				continue
			}
			if meta != nil {
				progress := meta.GetProgress()
				log.Printf("state: %s  progress: %d%%\n", meta.Name, progress.GetProgressPercent())
			}
			if op.Done() {
				log.Printf("database restored: %s\n", restore.Name)

				return nil
			}
		}
	}
}

func (s *SpannerBackup) validateDatabaseBackupAge(b *adminpb.Backup) bool {
	now := time.Now().Unix()
	if (now - b.CreateTime.GetSeconds()) < s.MaxBackupAge {
		log.Printf("most recent backup is newer than desired age: %d\n", s.MaxBackupAge)

		return true
	}

	return false
}

func (s *SpannerBackup) checkExistingDatabase(ctx context.Context, databaseName string) error {
	// Check if db exists

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.ProjectID, s.InstanceID, databaseName)
	log.Printf("checking for existing database: %s\n", databaseName)
	_, err := s.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: database,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return errors.Wrap(err, "checkExistingDatabase()")
		}

		return errors.Wrap(err, "s.admin.GetDatabase()")
	}

	log.Printf("existing database found: %s\n", databaseName)

	return nil
}

func (s *SpannerBackup) Close() error {
	if err := s.admin.Close(); err != nil {
		return errors.Wrap(err, "Close()")
	}

	return nil
}
