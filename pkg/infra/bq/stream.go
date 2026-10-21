package bq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra/bq/writer"
	"github.com/secmon-lab/swarm/pkg/utils"

	"cloud.google.com/go/bigquery"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/m-mizutani/goerr/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxAppendFailures is the maximum number of failed appends in one InsertSerializedRows call, except failures by a schema mismatch. A schema mismatch is retried until the timeout of InsertSerializedRows because it continues while a schema change of the table propagates.
const maxAppendFailures = 5

type Stream struct {
	msgDescriptor protoreflect.MessageDescriptor

	mgr *writer.Manager

	// appendMutex serializes sending appends to the managed stream. The managedwriter documentation of the
	// bigquery module in use does not state that ManagedStream.AppendRows is safe for concurrent use,
	// and its implementation reads the request template without holding a lock. Waiting for the response
	// is done without the lock so that appends of other callers can be sent in the meantime.
	appendMutex sync.Mutex
}

func newStream(ctx context.Context, mwClient *mw.Client, projectID types.GoogleProjectID, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (*Stream, error) {
	messageDescriptor, descriptorProto, err := schemaToDescriptor(schema)
	if err != nil {
		return nil, err
	}

	return newStreamWithOpener(ctx, messageDescriptor, writer.DefaultStreamOpener(mwClient, descriptorProto, projectID, datasetID, tableID))
}

func newStreamWithOpener(ctx context.Context, messageDescriptor protoreflect.MessageDescriptor, open writer.OpenFunc) (*Stream, error) {
	mgr, err := writer.NewManager(ctx, open)
	if err != nil {
		return nil, err
	}

	return &Stream{
		msgDescriptor: messageDescriptor,
		mgr:           mgr,
	}, nil
}

// Insert serializes data and writes all of them in one AppendRows request. See InsertSerializedRows for retries.
func (x *Stream) Insert(ctx context.Context, data []any) error {
	rows, err := x.SerializeRows(data)
	if err != nil {
		return err
	}
	return x.InsertSerializedRows(ctx, rows)
}

// SerializeRows converts data to protocol buffer rows of the schema that the stream was created with.
func (x *Stream) SerializeRows(data []any) ([][]byte, error) {
	return convertDataToBytes(x.msgDescriptor, data)
}

// InsertSerializedRows writes all rows in one AppendRows request and returns after BigQuery accepts the request. When the append fails, it renews the managed stream and sends the same request again: a schema mismatch is retried until the timeout, an error that a new managed stream can not fix (see isRetryableAppendError) is returned at once, and other errors are retried up to maxAppendFailures times. The last error is returned when it gives up.
func (x *Stream) InsertSerializedRows(ctx context.Context, rows [][]byte) error {
	// After updating BigQuery schema, there is a delay for propagation of the schema change. According to the following document, it takes about 10 minutes.
	// https://issuetracker.google.com/issues/64329577#comment3
	// Then, we wait for 15 minutes to avoid the schema propagation delay.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	failures := 0
	var lastErr error
	if err := backoff(ctx, func(c int) (bool, error) {
		w := x.mgr.Writer(ctx)
		err := x.appendRows(ctx, w, rows)
		if err == nil {
			return true, nil // done without error
		}
		if ctx.Err() != nil {
			// Canceled or timed out while appending. lastErr keeps the error of the previous attempt, such as a schema mismatch that lasted until the timeout.
			return true, goerr.Wrap(err, "context is done while appending rows")
		}
		lastErr = err

		if errors.Is(err, types.ErrSchemaNotMatched) {
			utils.CtxLogger(ctx).Warn("schema mismatch, renew stream and retry", "n", c)
		} else {
			if !isRetryableAppendError(err) {
				return true, err
			}
			failures++
			if failures >= maxAppendFailures {
				return true, goerr.Wrap(err, "gave up appending rows", goerr.V("failures", failures))
			}
			utils.CtxLogger(ctx).Warn("failed to append rows, renew stream and retry", "n", c, "error", err.Error())
		}

		if err := x.mgr.Renew(ctx, w); err != nil {
			return true, err // failed to renew stream, abort
		}
		return false, nil // retry
	}); err != nil {
		// Report the schema mismatch to the caller so that it can discard the schema it used.
		if errors.Is(lastErr, types.ErrSchemaNotMatched) && !errors.Is(err, types.ErrSchemaNotMatched) {
			return goerr.Wrap(lastErr, "failed to insert data after schema mismatch", goerr.V("error", err.Error()))
		}
		return err
	}

	return nil
}

// appendRows sends rows with w and waits for the response. It releases w after the response, so that w is not closed by a renewal while the request is in flight.
func (x *Stream) appendRows(ctx context.Context, w *writer.Writer, rows [][]byte) error {
	defer w.Release()

	x.appendMutex.Lock()
	result, err := w.Append(ctx, rows)
	x.appendMutex.Unlock()
	if err != nil {
		return err
	}

	return result.Wait(ctx)
}

// isRetryableAppendError returns false for errors that sending the same rows through a new managed stream can not fix. The codes follow retryPredicate of managedwriter in cloud.google.com/go/bigquery v1.85.0 (storage/managedwriter/retry.go), which also treats them as non-retryable:
//   - InvalidArgument: the request itself is rejected, such as a request that is too large (documented on ManagedStream.AppendRows).
//   - NotFound: the table does not exist.
//   - PermissionDenied, Unauthenticated: the credential is not allowed to write the table.
//
// Other errors are retried, which is broader than retryPredicate. retryPredicate decides whether the library sends a request again on the same managed stream, while InsertSerializedRows opens a new managed stream before sending again. A new managed stream fixes errors that retryPredicate does not retry, such as the context error that a managed stream keeps after its own context is done, and io.EOF that a closed managed stream returns. Errors that a new managed stream does not fix are retried at most maxAppendFailures times.
func isRetryableAppendError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch s.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated:
		return false
	}
	return true
}

func (x *Stream) Close() error {
	return x.mgr.Close()
}
