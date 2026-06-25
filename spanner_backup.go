package dbinitiator

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-playground/errors/v5"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SpannerBackup struct {
	SourceConnectionString string
	TargetConnectionString string
	SourceDb               string
	TargetDb               string
	ProjectID              string
	InstanceID             string
	admin                  *spannerDB.DatabaseAdminClient
	client                 *spanner.Client
}

func NewSpannerBackup(ctx context.Context, projectID, instanceID, sourceDb, targetDb string, opts ...option.ClientOption) (*SpannerBackup, error) {
	srcDbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, sourceDb)
	tgtDbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, targetDb)
	client, err := spanner.NewClient(ctx, srcDbStr, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "spanner.NewClient()")
	}

	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		client.Close()

		return nil, errors.Wrap(err, "spannerDB.NewDatabaseAdminClient()")
	}

	return &SpannerBackup{
		SourceConnectionString: srcDbStr,
		TargetConnectionString: tgtDbStr,
		SourceDb:               sourceDb,
		TargetDb:               tgtDbStr,
		admin:                  adminClient,
		client:                 client,
		ProjectID:              projectID,
		InstanceID:             instanceID,
	}, nil
}

func (s *SpannerBackup) Backup(ctx context.Context, sourceDatabase string) (*adminpb.Backup, error) {
	fmt.Printf("preparing to back up '%s' database\n", sourceDatabase)
	instance := fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID)
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.ProjectID, s.InstanceID, sourceDatabase)
	expire := time.Now().AddDate(0, 0, 7).UTC() // Will back up for 1 week
	req := &adminpb.CreateBackupRequest{
		Parent:   instance,
		BackupId: sourceDatabase + "_backup_" + time.Now().UTC().Format("20060102_150405"),
		Backup: &adminpb.Backup{
			Database:   database,
			ExpireTime: timestamppb.New(expire),
		},
	}
	fmt.Printf("generated backup request for %s\n", sourceDatabase)
	op, err := s.admin.CreateBackup(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "s.admin.CreateBackup()")
	}
	fmt.Println("running backup...")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			backup, err := op.Poll(ctx)
			if err != nil {
				log.Println("polling error: ", err)

				continue
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println("could not get metadata")

				continue
			}
			if meta != nil {
				progress := meta.GetProgress()
				fmt.Printf("state: %s  progress: %d%%\n", meta.Database, progress.GetProgressPercent())
			}
			if op.Done() {
				fmt.Printf("backup complete: %s  size: %d bytes\n", backup.Name, backup.SizeBytes)

				return backup, nil
			}
		}
	}
}

func (s *SpannerBackup) drop(ctx context.Context, targetDatabase string) error {
	fmt.Printf("dropping database %s\n", targetDatabase)
	req := &adminpb.DropDatabaseRequest{
		Database: s.TargetConnectionString + targetDatabase,
	}
	err := s.admin.DropDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.DropDatabase()")
	}
	fmt.Printf("database %s dropped\n", targetDatabase)

	return nil
}

func (s *SpannerBackup) Restore(ctx context.Context, backup *adminpb.Backup, targetDatabase string) error {
	if err := s.drop(ctx, targetDatabase); err != nil {
		return errors.Wrap(err, "s.Drop()")
	}
	req := &adminpb.RestoreDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s/instances/%s", s.ProjectID, s.InstanceID), // Spanner Instance
		DatabaseId: targetDatabase,                                                     // Target Database to restore TO
		Source: &adminpb.RestoreDatabaseRequest_Backup{
			Backup: backup.Name, // Restore FROM
		},
	}

	fmt.Printf("restoring %s\n", req.Source)
	op, err := s.admin.RestoreDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.RestoreDatabase()")
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			restore, err := op.Poll(ctx)
			if err != nil {
				log.Println("polling error: ", err)

				continue
			}
			meta, err := op.Metadata()
			if err != nil {
				log.Println("could not get metadata")

				continue
			}
			if meta != nil {
				progress := meta.GetProgress()
				fmt.Printf("state: %s  progress: %d%%\n", meta.Name, progress.GetProgressPercent())
			}
			if op.Done() {
				fmt.Printf("database restored: %s\n", restore.Name)

				return nil
			}
		}
	}
}

func (s *SpannerBackup) BackupRestore(ctx context.Context, source, destination string) error {
	backup, err := s.Backup(ctx, source)
	if err != nil {
		log.Println("error backing up ", err)

		return err
	}

	if err := s.Restore(ctx, backup, destination); err != nil {
		return err
	}

	return nil
}

func (s *SpannerBackup) Close() error {
	s.client.Close()
	if err := s.admin.Close(); err != nil {
		return err
	}

	return nil
}
