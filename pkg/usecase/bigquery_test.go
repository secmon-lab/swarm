package usecase_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
)

func TestCreateOrUpdateTable(t *testing.T) {
	bqProject := utils.LoadEnv(t, "TEST_BIGQUERY_PROJECT_ID")
	bqDataset := utils.LoadEnv(t, "TEST_BIGQUERY_DATASET_ID")

	ctx := context.Background()
	bqClient := gt.R1(bq.New(ctx, types.GoogleProjectID(bqProject))).NoError(t)

	tableID := time.Now().Format("create_test_20060102_150405")

	// Create table
	gt.R1(usecase.CreateOrUpdateTable(ctx,
		bqClient,
		types.BQDatasetID(bqDataset),
		types.BQTableID(tableID),
		&bigquery.TableMetadata{
			Schema: []*bigquery.FieldSchema{
				{Name: "name", Type: bigquery.StringFieldType},
				{Name: "age", Type: bigquery.IntegerFieldType},
			},
		})).NoError(t)

	// Update table
	gt.R1(usecase.CreateOrUpdateTable(ctx,
		bqClient,
		types.BQDatasetID(bqDataset),
		types.BQTableID(tableID),
		&bigquery.TableMetadata{
			Schema: []*bigquery.FieldSchema{
				{Name: "age", Type: bigquery.IntegerFieldType},
				{Name: "address", Type: bigquery.StringFieldType},
			},
		})).NoError(t)
}
