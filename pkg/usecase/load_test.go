package usecase_test

import (
	"context"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/infra/bq"
	"github.com/m-mizutani/swarm/pkg/infra/cs"
	"github.com/m-mizutani/swarm/pkg/infra/policy"
	"github.com/m-mizutani/swarm/pkg/usecase"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func TestLoadDataByObject(t *testing.T) {
	bqProject := utils.LoadEnv(t, "TEST_BIGQUERY_PROJECT_ID")
	bqDataset := utils.LoadEnv(t, "TEST_BIGQUERY_DATASET_ID")
	tableID := utils.LoadEnv(t, "TEST_BIGQUERY_TABLE_ID")
	gcsURL := utils.LoadEnv(t, "TEST_GCS_OBJECT_URL")
	policyDir := utils.LoadEnv(t, "TEST_POLICY_DIR")

	ctx := context.Background()
	bqClient := gt.R1(bq.New(ctx, bqProject)).NoError(t)
	csClient := gt.R1(cs.New(ctx)).NoError(t)
	pClient := gt.R1(policy.New(policy.WithDir(policyDir))).NoError(t)
	meta := model.NewMetadataConfig(types.BQDatasetID(bqDataset), types.BQTableID(tableID))

	uc := usecase.New(
		infra.New(
			infra.WithBigQuery(bqClient),
			infra.WithCloudStorage(csClient),
			infra.WithPolicy(pClient),
		),
		usecase.WithMetadata(meta),
	)

	gt.NoError(t, uc.LoadDataByObject(ctx, types.CSUrl(gcsURL)))
}
