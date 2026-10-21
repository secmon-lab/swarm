package writer

import (
	"context"
	"io"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
	"google.golang.org/protobuf/types/descriptorpb"
)

// RowAppender sends serialized rows to a BigQuery write stream.
type RowAppender interface {
	// AppendRows sends all rows in one AppendRows request. It returns before the server responds to the request; Wait of the returned AppendResult returns the response.
	AppendRows(ctx context.Context, rows [][]byte) (AppendResult, error)
	Close() error
}

// AppendResult is the result of one AppendRows request.
type AppendResult interface {
	// Wait waits until the server responds to the request. It returns nil if the server accepted all rows, and types.ErrSchemaNotMatched if the rows have fields that the table does not have.
	Wait(ctx context.Context) error
}

// OpenFunc opens a new RowAppender. Manager calls it at creation and at every renewal.
type OpenFunc func(ctx context.Context) (RowAppender, error)

type Manager struct {
	open          OpenFunc
	currentWriter *Writer
	mutex         sync.Mutex
	closeWG       sync.WaitGroup
}

// DefaultStreamOpener returns OpenFunc that opens a managed stream to the _default stream of the table. Rows are serialized with the descriptor.
func DefaultStreamOpener(mwClient *mw.Client, proto *descriptorpb.DescriptorProto, projectID types.GoogleProjectID, datasetID types.BQDatasetID, tableID types.BQTableID) OpenFunc {
	return func(ctx context.Context) (RowAppender, error) {
		ms, err := mwClient.NewManagedStream(ctx,
			mw.WithDestinationTable(
				mw.TableParentFromParts(
					projectID.String(),
					datasetID.String(),
					tableID.String(),
				),
			),
			mw.WithSchemaDescriptor(proto),
		)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to create managed stream",
				goerr.V("datasetID", datasetID), goerr.V("tableID", tableID))
		}
		return &managedStreamAppender{s: ms}, nil
	}
}

type managedStreamAppender struct {
	s *mw.ManagedStream
}

func (x *managedStreamAppender) AppendRows(ctx context.Context, rows [][]byte) (AppendResult, error) {
	arResult, err := x.s.AppendRows(ctx, rows)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to append rows", goerr.V("rows", len(rows)))
	}
	return &managedStreamResult{r: arResult, rows: len(rows)}, nil
}

// Close closes the managed stream. ManagedStream.Close returns io.EOF when the stream had no error, so io.EOF is not reported as a failure.
func (x *managedStreamAppender) Close() error {
	if err := x.s.Close(); err != nil && err != io.EOF {
		return goerr.Wrap(err, "failed to close managed stream", goerr.V("stream_name", x.s.StreamName()))
	}
	return nil
}

type managedStreamResult struct {
	r    *mw.AppendResult
	rows int
}

func (x *managedStreamResult) Wait(ctx context.Context) error {
	if _, err := x.r.FullResponse(ctx); err != nil {
		if apiErr, ok := apierror.FromError(err); ok {
			storageErr := &storagepb.StorageError{}
			if e := apiErr.Details().ExtractProtoMessage(storageErr); e == nil && storageErr.Code == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
				utils.CtxLogger(ctx).Debug("schema does not matched, should retry")
				return types.ErrSchemaNotMatched
			}
		}
		return goerr.Wrap(err, "failed to get append result", goerr.V("rows", x.rows))
	}
	return nil
}

func newWriter(ctx context.Context, open OpenFunc) (*Writer, error) {
	a, err := open(ctx)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		id: uuid.NewString(),
		a:  a,
	}
	utils.CtxLogger(ctx).Debug("created new writer", "writer_id", w.id)
	return w, nil
}

func NewManager(ctx context.Context, open OpenFunc) (*Manager, error) {
	w, err := newWriter(ctx, open)
	if err != nil {
		return nil, err
	}

	return &Manager{
		open:          open,
		currentWriter: w,
	}, nil
}

// Writer is a RowAppender in use by Manager. Manager closes it after it is replaced and all users have released it.
type Writer struct {
	id string
	a  RowAppender
	wg sync.WaitGroup
}

// Renew replaces failed with a new writer. If failed has already been replaced by another call, it does nothing, so that appends that failed on the same writer at the same time open only one new writer. The failed writer is closed after all appends using it are released.
func (x *Manager) Renew(ctx context.Context, failed *Writer) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	if x.currentWriter != failed {
		return nil
	}

	// The managed stream keeps the context passed at creation for its whole lifetime. The new writer is shared by later appends, so it must not be canceled together with the context of the append that triggered the renewal.
	newWriter, err := newWriter(context.WithoutCancel(ctx), x.open)
	if err != nil {
		return err
	}

	oldWriter := x.currentWriter
	utils.CtxLogger(ctx).Debug("renewing writer", "writer_id", oldWriter.id)
	x.closeWG.Add(1)
	go func() {
		defer x.closeWG.Done()
		oldWriter.wg.Wait()
		utils.SafeClose(oldWriter.a)
		utils.CtxLogger(ctx).Debug("closed writer", "writer_id", oldWriter.id)
	}()

	x.currentWriter = newWriter

	return nil
}

func (x *Manager) Close() error {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	if err := x.currentWriter.a.Close(); err != nil {
		return goerr.Wrap(err, "failed to close writer", goerr.V("writer_id", x.currentWriter.id))
	}
	x.currentWriter = nil
	x.closeWG.Wait()
	return nil
}

// Writer returns the current writer. The caller must call Release of the writer when it no longer uses the writer.
func (x *Manager) Writer(ctx context.Context) *Writer {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.currentWriter.wg.Add(1)
	return x.currentWriter
}

// Append sends all rows in one AppendRows request.
func (x *Writer) Append(ctx context.Context, rows [][]byte) (AppendResult, error) {
	return x.a.AppendRows(ctx, rows)
}

func (x *Writer) Release() {
	x.wg.Done()
}
