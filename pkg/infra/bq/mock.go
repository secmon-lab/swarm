package bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type Mock struct {
	MockQuery       func(ctx context.Context, query string) (interfaces.BigQueryIterator, error)
	MockInsert      (func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, data []any) error)
	MockGetMetadata (func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID) (*bigquery.TableMetadata, error))
	MockUpdateTable (func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error)
	MockCreateTable (func(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error)
}

// CreateTable implements interfaces.BigQuery.
func (x *Mock) CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
	if x.MockCreateTable != nil {
		return x.MockCreateTable(ctx, dataset, table, md)
	}
	return nil
}

func NewMock() *Mock {
	return &Mock{}
}

var _ interfaces.BigQuery = &Mock{}

func (x *Mock) Query(ctx context.Context, query string) (interfaces.BigQueryIterator, error) {
	if x.MockQuery != nil {
		return x.MockQuery(ctx, query)
	}
	return nil, nil
}

func (x *Mock) Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error {
	if x.MockInsert != nil {
		return x.MockInsert(ctx, datasetID, tableID, data)
	}
	return nil
}

func (x *Mock) GetMetadata(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID) (*bigquery.TableMetadata, error) {
	if x.MockGetMetadata != nil {
		return x.MockGetMetadata(ctx, datasetID, tableID)
	}
	return nil, nil
}

func (x *Mock) UpdateTable(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error {
	if x.MockUpdateTable != nil {
		return x.MockUpdateTable(ctx, datasetID, tableID, md, eTag)
	}
	return nil
}
