package writer

import (
	"context"
	"io"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
	"google.golang.org/protobuf/types/descriptorpb"
)

type Manager struct {
	fac           *factory
	currentWriter *writer
	mutex         sync.Mutex
	closeWG       sync.WaitGroup
}

type factory struct {
	mwClient *mw.Client
	proto    *descriptorpb.DescriptorProto

	projectID types.GoogleProjectID
	datasetID types.BQDatasetID
	tableID   types.BQTableID
}

func (x *factory) newWriter(ctx context.Context) (*writer, error) {
	ms, err := x.mwClient.NewManagedStream(ctx,
		mw.WithDestinationTable(
			mw.TableParentFromParts(
				x.projectID.String(),
				x.datasetID.String(),
				x.tableID.String(),
			),
		),
		// mw.WithType(mw.CommittedStream),
		mw.WithSchemaDescriptor(x.proto),
	)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create managed stream")
	}

	w := &writer{
		id: uuid.NewString(),
		s:  ms,
	}
	utils.CtxLogger(ctx).Debug("created new writer", "writer_id", w.id)
	return w, nil
}

func NewManger(ctx context.Context, mwClient *mw.Client, proto *descriptorpb.DescriptorProto, projectID types.GoogleProjectID, datasetID types.BQDatasetID, tableID types.BQTableID) (*Manager, error) {
	f := &factory{
		mwClient: mwClient,
		proto:    proto,

		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}

	writer, err := f.newWriter(ctx)
	if err != nil {
		return nil, err
	}

	return &Manager{
		fac:           f,
		currentWriter: writer,
	}, nil
}

type writer struct {
	id string
	s  *mw.ManagedStream
	wg sync.WaitGroup
}

func (x *Manager) Renew(ctx context.Context) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	newWriter, err := x.fac.newWriter(ctx)
	if err != nil {
		return err
	}

	oldWriter := x.currentWriter
	utils.CtxLogger(ctx).Debug("renewing writer", "writer_id", oldWriter.id)
	x.closeWG.Add(1)
	go func() {
		defer x.closeWG.Done()
		oldWriter.wg.Wait()
		utils.SafeClose(oldWriter.s)
		utils.CtxLogger(ctx).Debug("closed writer", "writer_id", oldWriter.id)
	}()

	x.currentWriter = newWriter

	return nil
}

func (x *Manager) Close() error {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	if err := x.currentWriter.s.Close(); err != nil && err != io.EOF {
		return goerr.Wrap(err, "failed to close managed stream").With("writer_id", x.currentWriter.id)
	}
	x.currentWriter = nil
	x.closeWG.Wait()
	return nil
}

func (x *Manager) Writer(ctx context.Context) *writer {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.currentWriter.wg.Add(1)
	return x.currentWriter
}

func (x *writer) Append(ctx context.Context, rows [][]byte) error {
	arResult, err := x.s.AppendRows(ctx, rows)
	if err != nil {
		return goerr.Wrap(err, "failed to append rows")
	}

	if _, err := arResult.FullResponse(ctx); err != nil {
		if apiErr, ok := apierror.FromError(err); ok {
			storageErr := &storagepb.StorageError{}
			if e := apiErr.Details().ExtractProtoMessage(storageErr); e == nil && storageErr.Code == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
				utils.CtxLogger(ctx).Debug("schema does not matched, should retry")
				return types.ErrSchemaNotMatched
			}
		}
		return goerr.Wrap(err, "failed to get append result")
	}

	return nil
}

func (x *writer) Release() {
	x.wg.Done()
}
