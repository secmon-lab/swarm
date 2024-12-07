package bq

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Client struct {
	mwClient  *mw.Client
	bqClient  *bigquery.Client
	projectID types.GoogleProjectID
}

var _ interfaces.BigQuery = &Client{}

func New(ctx context.Context, projectID types.GoogleProjectID) (*Client, error) {
	mwClient, err := mw.NewClient(ctx, projectID.String())
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create bigquery client").With("projectID", projectID)
	}

	bqClient, err := bigquery.NewClient(ctx, projectID.String(),
		mw.WithMultiplexing(),
		mw.WithMultiplexPoolLimit(32),
	)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create bigquery client").With("projectID", projectID)
	}

	return &Client{
		mwClient:  mwClient,
		bqClient:  bqClient,
		projectID: projectID,
	}, nil
}

// Query implements interfaces.BigQuery.
func (x *Client) Query(ctx context.Context, query string) (interfaces.BigQueryIterator, error) {
	q := x.bqClient.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to read query result")
	}

	return it, nil
}

func backoff(ctx context.Context, callback func(n int) (done bool, err error)) error {
	// Retry with exponential backoff
	backoff := 10 * time.Millisecond
	waitMax := 30 * time.Second

	for i := 0; ; i++ {
		done, err := callback(i)
		if done {
			return err
		}

		// Exponential backoff
		backoff *= 2
		if backoff == 0 {
			backoff = 1 * time.Second
		} else if backoff > waitMax {
			backoff = waitMax
		}

		select {
		case <-ctx.Done():
			return goerr.Wrap(ctx.Err(), "context is canceled")
		case <-time.After(backoff):
		}
	}
}

func (x *Client) NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
	return newStream(ctx, x.mwClient, x.projectID, datasetID, tableID, schema)
}

func convertDataToBytes(md protoreflect.MessageDescriptor, data []any) ([][]byte, error) {
	var rows [][]byte
	for _, v := range data {
		message := dynamicpb.NewMessage(md)

		raw, err := json.Marshal(v)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Marshal json message").With("v", v)
		}

		// First, json->proto message
		err = protojson.Unmarshal(raw, message)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Unmarshal json message").With("raw", string(raw))
		}
		// Then, proto message -> bytes.
		b, err := proto.Marshal(message)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Marshal proto message")
		}

		rows = append(rows, b)
	}

	return rows, nil
}

var errAppendCountMismatch = goerr.New("append count mismatch")
var errSchemaMismatch = goerr.New("schema mismatch")

func (x *Client) Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema")
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema to descriptor")
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return goerr.Wrap(err, "adapted descriptor is not a message descriptor")
	}
	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return goerr.Wrap(err, "failed to normalize descriptor")
	}

	// After updating BigQuery schema, there is a delay for propagation of the schema change. According to the following document, it takes about 10 minutes.
	// https://issuetracker.google.com/issues/64329577#comment3
	// Then, we wait for 15 minutes to avoid the schema propagation delay.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	tableParent := mw.TableParentFromParts(
		x.projectID.String(),
		datasetID.String(),
		tableID.String(),
	)

	if err := backoff(ctx, func(n int) (bool, error) {
		if err := insert(ctx, x.mwClient, tableParent, data, descriptorProto, messageDescriptor); err != nil {
			if err == errAppendCountMismatch {
				utils.CtxLogger(ctx).Warn("append count mismatch, retry", "n", n)
				return false, nil
			}
			if err == errSchemaMismatch {
				utils.CtxLogger(ctx).Warn("schema mismatch, retry", "n", n)
				return false, nil
			}
			return true, err
		}
		return true, nil
	}); err != nil {
		return goerr.Wrap(err, "failed to insert data")
	}
	return nil
}

func isSchemaMismatchError(err error) bool {
	if apiErr, ok := apierror.FromError(err); ok {
		storageErr := &storagepb.StorageError{}
		if e := apiErr.Details().ExtractProtoMessage(storageErr); e == nil && storageErr.Code == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
			return true
		}
	}

	return false
}

func insert(ctx context.Context, mwClient *mw.Client, tableParent string, data []any, dp *descriptorpb.DescriptorProto, md protoreflect.MessageDescriptor) error {
	logger := utils.CtxLogger(ctx)

	logger.Info("starting data ingestion", "count", len(data))
	ms, err := mwClient.NewManagedStream(ctx,
		mw.WithDestinationTable(tableParent),
		mw.WithType(mw.PendingStream),
		mw.WithSchemaDescriptor(dp),
	)
	if err != nil {
		return goerr.Wrap(err, "failed to create managed stream")
	}
	defer utils.SafeClose(ms)

	logger.Info("created managed stream", "stream_name", ms.StreamName())

	const maxRows = 256
	var respSet []*mw.AppendResult

	// TODO: Remove this struct after debugging
	type appendPerfLog struct {
		Count    int
		Duration time.Duration
	}
	var perfLogs []appendPerfLog

	logger.Info("converting data to bytes", "count", len(data))
	for s := 0; s < len(data); s += maxRows {
		e := min(s+maxRows, len(data))
		rows, err := convertDataToBytes(md, data[s:e])
		if err != nil {
			return goerr.Wrap(err, "failed to convert data to bytes")
		}

		ts := time.Now()
		resp, err := ms.AppendRows(ctx, rows)
		if err != nil {
			return goerr.Wrap(err, "failed to append rows")
		}
		perfLogs = append(perfLogs, appendPerfLog{Count: len(rows), Duration: time.Since(ts)})
		respSet = append(respSet, resp)
	}
	logger.Info("append performance", "logs", perfLogs)

	for _, resp := range respSet {
		if _, err := resp.GetResult(ctx); err != nil {
			if isSchemaMismatchError(err) {
				return errSchemaMismatch
			}
			return goerr.Wrap(err, "failed to get append result")
		}
	}

	n, err := ms.Finalize(ctx)
	if err != nil {
		return goerr.Wrap(err, "failed to finalize stream")
	}

	if n != int64(len(data)) {
		logger.Warn("append count mismatch", "expected", len(data), "actual", n)
	}

	logger.Info("append rows", "count", n)

	req := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       mw.TableParentFromStreamName(ms.StreamName()),
		WriteStreams: []string{ms.StreamName()},
	}
	logger.Debug("commit write streams", "req", req)

	resp, err := mwClient.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		return goerr.Wrap(err, "failed to commit write streams")
	}
	if errs := resp.GetStreamErrors(); len(errs) > 0 {
		return goerr.Wrap(err, "failed to commit write streams").With("errors", errs)
	}

	return nil
}

// GetMetadata implements interfaces.BigQuery. If the table does not exist, it returns nil.
func (x *Client) GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error) {
	md, err := x.bqClient.Dataset(dataset.String()).Table(table.String()).Metadata(ctx)
	if err != nil {
		if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
			return nil, nil
		}
		return nil, goerr.Wrap(err, "failed to get table metadata")
	}

	return md, nil
}

// UpdateSchema implements interfaces.BigQuery.
func (x *Client) UpdateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error {
	if _, err := x.bqClient.Dataset(dataset.String()).Table(table.String()).Update(ctx, md, eTag); err != nil {
		return goerr.Wrap(err, "failed to update table schema").With("dataset", dataset).With("table", table)
	}

	return nil
}

// CreateTable implements interfaces.BigQuery.
func (x *Client) CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
	if err := x.bqClient.Dataset(dataset.String()).Table(table.String()).Create(ctx, md); err != nil {
		return goerr.Wrap(err, "failed to create table").With("dataset", dataset).With("table", table)
	}

	return nil
}
