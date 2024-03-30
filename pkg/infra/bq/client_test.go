package bq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/bq"
)

func TestInsert(t *testing.T) {
	projectID, ok := os.LookupEnv("TEST_BIGQUERY_PROJECT_ID")
	if !ok {
		t.Skip("TEST_BIGQUERY_PROJECT_ID is not set")
	}

	sDatasetID, ok := os.LookupEnv("TEST_BIGQUERY_DATASET_ID")
	if !ok {
		t.Skip("TEST_BIGQUERY_DATASET_ID is not set")
	}

	datasetID := types.BQDatasetID(sDatasetID)
	tableID := types.BQTableID(time.Now().Format("insert_20060102_150405"))

	ctx := context.Background()
	bqClient := gt.R1(bigquery.NewClient(ctx, projectID)).NoError(t)
	gt.NoError(t, bqClient.
		Dataset(datasetID.String()).
		Table(tableID.String()).
		Create(ctx, &bigquery.TableMetadata{}))

	d1 := map[string]any{
		"red":  uuid.NewString(),
		"blue": uuid.NewString(),
	}
	d2 := map[string]any{
		"red":    uuid.NewString(),
		"orange": uuid.NewString(),
	}
	d3 := map[string]any{
		"black": uuid.NewString(),
	}
	log1 := model.LogRecord{ID: "p1", Timestamp: time.Now(), Data: d1}
	log2 := model.LogRecord{ID: "p2", Timestamp: time.Now(), Data: d2}
	log3 := model.LogRecord{ID: "p3", Timestamp: time.Now(), Data: d3}

	client := gt.R1(bq.New(ctx, types.GoogleProjectID(projectID))).NoError(t)

	var merged bigquery.Schema
	t.Run("Insert first data", func(t *testing.T) {
		merged = gt.R1(bqs.Merge(merged, gt.R1(bqs.Infer(log1)).NoError(t))).NoError(t)
		merged = gt.R1(bqs.Merge(merged, gt.R1(bqs.Infer(log2)).NoError(t))).NoError(t)

		md := gt.R1(
			client.GetMetadata(ctx, datasetID, tableID),
		).NoError(t)

		updateMD := bigquery.TableMetadataToUpdate{
			Schema: merged,
		}
		gt.NoError(t, client.UpdateTable(ctx, datasetID, tableID, updateMD, md.ETag))

		gt.NoError(t, client.Insert(ctx,
			datasetID,
			tableID,
			merged,
			[]any{
				log1.Raw(),
				log2.Raw(),
			},
		))
	})

	t.Run("Update schema and insert data", func(t *testing.T) {
		var md *bigquery.TableMetadata
		for i := 0; i < 10; i++ {
			md = gt.R1(
				client.GetMetadata(ctx, datasetID, tableID),
			).NoError(t)

			if bqs.Equal(md.Schema, merged) {
				break
			}
			t.Log("retry to get schema")
		}

		merged = gt.R1(bqs.Merge(merged, gt.R1(bqs.Infer(log3)).NoError(t))).NoError(t)

		updateMD := bigquery.TableMetadataToUpdate{
			Schema: merged,
		}
		gt.NoError(t, client.UpdateTable(ctx, datasetID, tableID, updateMD, md.ETag))

		gt.NoError(t, client.Insert(ctx,
			datasetID,
			tableID,
			merged,
			[]any{
				log3.Raw(),
			},
		))
	})
}
