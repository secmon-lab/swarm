package bq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/bq/writer"

	"cloud.google.com/go/bigquery"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/m-mizutani/goerr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Stream struct {
	datasetID     types.BQDatasetID
	tableID       types.BQTableID
	schema        bigquery.Schema
	msgDescriptor protoreflect.MessageDescriptor

	mgr *writer.Manager
}

func newStream(ctx context.Context, mwClient *mw.Client, projectID types.GoogleProjectID, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (*Stream, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to convert schema")
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, goerr.Wrap(err, "failed to convert schema to descriptor")
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, goerr.Wrap(err, "adapted descriptor is not a message descriptor")
	}
	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to normalize descriptor")
	}

	mgr, err := writer.NewManger(ctx, mwClient, descriptorProto, projectID, datasetID, tableID)
	if err != nil {
		return nil, err
	}

	return &Stream{
		datasetID:     datasetID,
		tableID:       tableID,
		schema:        schema,
		msgDescriptor: messageDescriptor,
		mgr:           mgr,
	}, nil
}

func (x *Stream) Insert(ctx context.Context, data []any) error {
	var rows [][]byte
	for _, v := range data {
		message := dynamicpb.NewMessage(x.msgDescriptor)

		raw, err := json.Marshal(v)
		if err != nil {
			return goerr.Wrap(err, "failed to Marshal json message").With("v", v)
		}

		// First, json->proto message
		err = protojson.Unmarshal(raw, message)
		if err != nil {
			return goerr.Wrap(err, "failed to Unmarshal json message").With("raw", string(raw))
		}
		// Then, proto message -> bytes.
		b, err := proto.Marshal(message)
		if err != nil {
			return goerr.Wrap(err, "failed to Marshal proto message")
		}

		rows = append(rows, b)
	}

	// After updating BigQuery schema, there is a delay for propagation of the schema change. According to the following document, it takes about 10 minutes.
	// https://issuetracker.google.com/issues/64329577#comment3
	// Then, we wait for 15 minutes to avoid the schema propagation delay.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	if err := backoff(ctx, func(c int) (bool, error) {
		w := x.mgr.Writer(ctx)
		defer w.Release()

		if err := w.Append(ctx, rows); err != nil {
			if err == types.ErrSchemaNotMatched {
				// If schema does not matched, it seems reconnection of stream is required
				if err := x.mgr.Renew(ctx); err != nil {
					return true, err // failed to renew stream, abort
				}
				return false, nil // retry
			}
		}

		return true, nil // done without error
	}); err != nil {
		return err
	}

	return nil
}

func (x *Stream) Close() error {
	return x.mgr.Close()
}
