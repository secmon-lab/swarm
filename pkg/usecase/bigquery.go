package usecase

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func createOrUpdateTable(ctx context.Context, bq interfaces.BigQuery, datasetID types.BQDatasetID, tableID types.BQTableID, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	old, err := bq.GetMetadata(ctx, datasetID, tableID)
	if err != nil {
		return nil, goerr.Wrap(err, "Failed to get metadata").With("datasetID", datasetID).With("tableID", tableID)
	}

	if old == nil {
		utils.CtxLogger(ctx).Info("creating new table", "datasetID", datasetID, "tableID", tableID)
		return md.Schema, bq.CreateTable(ctx, datasetID, tableID, md)
	}

	merged, err := bqs.Merge(old.Schema, md.Schema)
	if err != nil {
		return nil, goerr.Wrap(err, "Failed to merge schema").With("old", old.Schema).With("new", md.Schema)
	}

	// If schema is not changed, do nothing
	if bqs.Equal(old.Schema, merged) {
		return merged, nil
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: merged,
	}
	utils.CtxLogger(ctx).Info("updating table schema", "datasetID", datasetID, "tableID", tableID)

	if err := bq.UpdateTable(ctx, datasetID, tableID, update, old.ETag); err != nil {
		return nil, goerr.Wrap(err, "Failed to update table").With("datasetID", datasetID).With("tableID", tableID)
	}
	return merged, nil
}

func inferSchema[T any](data []T) (bigquery.Schema, error) {
	var merged bigquery.Schema
	for _, d := range data {
		schema, err := bqs.Infer(d)
		if err != nil {
			return nil, goerr.Wrap(err, "Failed to infer schema").With("data", d)
		}

		merged, err = bqs.Merge(merged, schema)
		if err != nil {
			return nil, goerr.Wrap(err, "Failed to merge schema")
		}
	}

	return merged, nil
}

func setupLoadLogTable(ctx context.Context, bq interfaces.BigQuery, meta *model.MetadataConfig) (bigquery.Schema, error) {
	schema, err := bqs.Infer(&model.LoadLog{
		Sources: []*model.SourceLog{
			{
				CS:     &model.CloudStorageObject{},
				Source: model.Source{},
			},
		},
		Ingests: []*model.IngestLog{{}},
	})
	if err != nil {
		return nil, goerr.Wrap(err, "failed to infer schema")
	}
	md := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "started_at",
			Type:  bigquery.MonthPartitioningType,
		},
	}
	if _, err := createOrUpdateTable(ctx, bq, meta.Dataset(), meta.Table(), md); err != nil {
		return nil, goerr.Wrap(err, "failed to create or update table")
	}

	return schema, nil
}
