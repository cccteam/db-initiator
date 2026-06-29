package dbinitiator

import (
	"context"
	"fmt"
	"log"
	"time"

	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SpannerBackup struct {
	TargetConnectionString string
	SourceDb               string
	ProjectID              string
	InstanceID             string
	admin                  *spannerDB.DatabaseAdminClient
}

func NewSpannerBackup(ctx context.Context, projectID, instanceID, sourceDb, targetDb string, opts ...option.ClientOption) (*SpannerBackup, error) {
	tgtDbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, targetDb)
	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "spannerDB.NewDatabaseAdminClient()")
	}

	return &SpannerBackup{
		TargetConnectionString: tgtDbStr,
		SourceDb:               sourceDb,
		admin:                  adminClient,
		ProjectID:              projectID,
		InstanceID:             instanceID,
	}, nil
}

func (s *SpannerBackup) Backup(ctx context.Context) (*adminpb.Backup, error) {
	log.Printf("preparing to back up '%s' database\n", s.SourceDb)
	instance := fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID)
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.ProjectID, s.InstanceID, s.SourceDb)
	// Check if db exists
	log.Printf("checking that database %s exists\n", s.SourceDb)
	_, err := s.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: database,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			log.Printf("database %s does not exist\n", s.SourceDb)

			return nil, errors.Wrap(err, "s.admin.GetDatabase()")
		}

		return nil, errors.Wrap(err, "s.admin.GetDatabase()")
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
				if status.Code(err) == codes.Canceled {
					return nil, errors.Wrap(err, "Backup()")
				}

				continue
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println("could not get metadata")

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

func (s *SpannerBackup) drop(ctx context.Context) error {
	log.Printf("dropping database %s\n", s.TargetConnectionString)
	req := &adminpb.DropDatabaseRequest{
		Database: s.TargetConnectionString,
	}
	err := s.admin.DropDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.DropDatabase()")
	}
	log.Printf("database %s dropped\n", s.TargetConnectionString)

	return nil
}

func (s *SpannerBackup) Restore(ctx context.Context, backup *adminpb.Backup, targetDatabase string) error {
	if err := s.drop(ctx); err != nil {
		return errors.Wrap(err, "s.drop()")
	}
	req := &adminpb.RestoreDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID), // Spanner Instance
		DatabaseId: targetDatabase,                                                     // Target Database to restore TO
		Source: &adminpb.RestoreDatabaseRequest_Backup{
			Backup: backup.Name, // Restore FROM
		},
	}

	log.Printf("restoring %s\n", req.Source)
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
				if status.Code(err) == codes.Canceled {
					return errors.Wrap(err, "Restore()")
				}

				continue
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println("could not get metadata")

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

func (s *SpannerBackup) Close() error {
	if err := s.admin.Close(); err != nil {
		return errors.Wrap(err, "Close()")
	}

	return nil
}
