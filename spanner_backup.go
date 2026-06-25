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
	sourceConnectionString string
	targetConnectionString string
	sourceDb               string
	targetDb               string
	databaseName           string
	projectID              string
	instanceID             string
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
		sourceConnectionString: srcDbStr,
		targetConnectionString: tgtDbStr,
		sourceDb:               sourceDb,
		targetDb:               tgtDbStr,
		admin:                  adminClient,
		client:                 client,
		projectID:              projectID,
		instanceID:             instanceID,
	}, nil
}

func (s *SpannerBackup) Backup(ctx context.Context, sourceDatabase string) error {
	fmt.Printf("preparing to back up '%s' database\n", sourceDatabase)
	expire := time.Now().AddDate(0, 0, 7).UTC() // Will back up for 1 week
	req := &adminpb.CreateBackupRequest{
		Parent:   fmt.Sprintf("projects/%s/instances/%s", s.projectID, s.instanceID),
		BackupId: sourceDatabase,
		Backup: &adminpb.Backup{
			Database:   s.sourceConnectionString,
			ExpireTime: timestamppb.New(expire),
		},
	}
	fmt.Printf("generated backup request for %s %s\n", req.Parent, req.BackupId)
	op, err := s.admin.CreateBackup(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.CreateBackup()")
	}
	fmt.Println("running backup...")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			meta, err := op.Metadata()
			if err != nil {
				log.Println("could not get metadata")

				continue
			}
			if meta != nil {
				progress := meta.GetProgress()
				fmt.Printf("state: %s  progress: %d%%\n", meta.Database, progress.GetProgressPercent())
			}
		}

		if op.Done() {
			backup, err := op.Wait(ctx)
			if err != nil {
				return fmt.Errorf("backup failed: %w", err)
			}
			fmt.Printf("backup complete: %s  size: %d bytes", backup.Name, backup.SizeBytes)
		}
	}
	return nil
}

func (s *SpannerBackup) Drop(ctx context.Context) error {
	fmt.Printf("dropping database %s", s.targetDb)
	req := &adminpb.DropDatabaseRequest{
		Database: s.targetConnectionString,
	}
	err := s.admin.DropDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.DropDatabase()")
	}
	fmt.Printf("database %s dropped\n", s.targetDb)

	return nil
}

func (s *SpannerBackup) Restore(ctx context.Context, targetDatabase string) error {
	if err := s.Drop(ctx); err != nil {
		return errors.Wrap(err, "s.Drop()")
	}
	req := &adminpb.RestoreDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s/instances/%s", s.projectID, s.instanceID), // Spanner Instance
		DatabaseId: targetDatabase,                                                     // Target Database to restore TO
		Source: &adminpb.RestoreDatabaseRequest_Backup{
			Backup: s.sourceConnectionString, // Restore FROM
		},
	}

	op, err := s.admin.RestoreDatabase(ctx, req)
	if err != nil {
		return errors.Wrap(err, "s.admin.RestoreDatabase()")
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return errors.Wrap(err, "op.Wait()")
	}

	fmt.Printf("database %s restored successfully\n", resp.Name)

	return nil
}

func (s *SpannerBackup) Close() error {
	s.client.Close()
	if err := s.admin.Close(); err != nil {
		return err
	}

	return nil
}
