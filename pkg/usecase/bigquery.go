package usecase

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func (x *UseCase) CreateOrUpdateTable(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	old, err := x.clients.BigQuery().GetMetadata(ctx, datasetID, tableID)
	if err != nil {
		return nil, goerr.Wrap(err, "Failed to get metadata").With("datasetID", datasetID).With("tableID", tableID)
	}

	if old == nil {
		utils.CtxLogger(ctx).Info("creating new table", "datasetID", datasetID, "tableID", tableID)
		return nil, x.clients.BigQuery().CreateTable(ctx, datasetID, tableID, md)
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
	return merged, x.clients.BigQuery().UpdateTable(ctx, datasetID, tableID, update, old.ETag)
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
			return nil, goerr.Wrap(err, "Failed to merge schema").With("merged", merged).With("schema", schema)
		}
	}

	return merged, nil
}
