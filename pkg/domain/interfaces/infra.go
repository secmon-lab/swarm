package interfaces

import (
	"context"
	"io"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type BigQueryIterator interface {
	Next(dst interface{}) error
}

type BigQuery interface {
	Query(ctx context.Context, query string) (BigQueryIterator, error)
	Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error
	GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error)
	UpdateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error
	CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error
}

type CSObjectIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

type CloudStorage interface {
	Open(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (io.ReadCloser, error)
	Attrs(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (*storage.ObjectAttrs, error)
	List(ctx context.Context, bucket types.CSBucket, query *storage.Query) CSObjectIterator
}
