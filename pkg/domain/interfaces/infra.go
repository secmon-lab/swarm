package interfaces

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type BigQueryIterator interface {
	Next(dst interface{}) error
}

type BigQuery interface {
	Query(ctx context.Context, query string) (BigQueryIterator, error)
	NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (BigQueryStream, error)

	GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error)
	UpdateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error
	CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error
}

type BigQueryStream interface {
	Insert(ctx context.Context, data []any) error
	Close() error
}

type PubSubTopic interface {
	Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error)
}

type PubSubSubscription interface {
	Pull(ctx context.Context, subName string) ([]*pubsubpb.ReceivedMessage, error)
	ModifyAckDeadline(ctx context.Context, subName string, ackID string, deadline time.Duration) error
	Acknowledge(ctx context.Context, subName string, ackID string) error
	Close() error
}

type CSObjectIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

type CloudStorage interface {
	Open(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error)
	Attrs(ctx context.Context, obj model.CloudStorageObject) (*storage.ObjectAttrs, error)
	List(ctx context.Context, bucket types.CSBucket, query *storage.Query) CSObjectIterator
}

type Database interface {
	GetOrCreateState(ctx context.Context, msgType types.MsgType, input *model.State) (*model.State, bool, error)
	GetState(ctx context.Context, msgType types.MsgType, id string) (*model.State, error)
	UpdateState(ctx context.Context, msgType types.MsgType, id string, state types.MsgState, now time.Time) error
}
