package bq

import (
	"context"
	"encoding/json"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type Mock struct {
	MockQuery       func(ctx context.Context, query string) (interfaces.BigQueryIterator, error)
	MockNewStream   (func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error))
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

// MockStream records rows written to it. SerializeRows converts each data to JSON, so the size of a row is the size of its JSON.
type MockStream struct {
	mutex sync.Mutex
	// Inserted has the rows of each successful InsertSerializedRows call.
	Inserted [][][]byte
	Closed   bool

	// MockSerializeRows replaces the JSON conversion of SerializeRows if set.
	MockSerializeRows func(data []any) ([][]byte, error)
	// MockInsertSerializedRows is called by InsertSerializedRows without holding the lock of MockStream. If it returns an error, the rows are not recorded.
	MockInsertSerializedRows func(ctx context.Context, rows [][]byte) error
}

func (x *MockStream) Insert(ctx context.Context, data []any) error {
	rows, err := x.SerializeRows(data)
	if err != nil {
		return err
	}
	return x.InsertSerializedRows(ctx, rows)
}

func (x *MockStream) SerializeRows(data []any) ([][]byte, error) {
	if x.MockSerializeRows != nil {
		return x.MockSerializeRows(data)
	}
	rows := make([][]byte, len(data))
	for i, v := range data {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to marshal data", goerr.V("index", i))
		}
		rows[i] = raw
	}
	return rows, nil
}

func (x *MockStream) InsertSerializedRows(ctx context.Context, rows [][]byte) error {
	if x.MockInsertSerializedRows != nil {
		if err := x.MockInsertSerializedRows(ctx, rows); err != nil {
			return err
		}
	}

	x.mutex.Lock()
	defer x.mutex.Unlock()
	x.Inserted = append(x.Inserted, rows)
	return nil
}

func (x *MockStream) Close() error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.Closed = true
	return nil
}

// IsClosed returns Closed with holding the lock, for reading it while other goroutines use the stream.
func (x *MockStream) IsClosed() bool {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return x.Closed
}

// InsertedRows returns the number of rows written by successful InsertSerializedRows calls.
func (x *MockStream) InsertedRows() int {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	n := 0
	for _, rows := range x.Inserted {
		n += len(rows)
	}
	return n
}

func (x *Mock) NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
	if x.MockNewStream != nil {
		return x.MockNewStream(ctx, datasetID, tableID, schema)
	}
	return &MockStream{}, nil
}

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

func NewGeneralMock() *GeneralMock {
	return &GeneralMock{}
}

type GeneralMock struct {
	Metadata []*bigquery.TableMetadata

	OpenedStream []struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		Schema  bigquery.Schema
	}
	Streams []*MockStream

	CreatedTable []struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		MD      *bigquery.TableMetadata
	}
	UpdatedTable []struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		MD      bigquery.TableMetadataToUpdate
		ETag    string
	}

	Queries []string

	mutex sync.Mutex
}

// CreateTable implements interfaces.BigQuery.
func (x *GeneralMock) CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.CreatedTable = append(x.CreatedTable, struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		MD      *bigquery.TableMetadata
	}{Dataset: dataset, Table: table, MD: md})

	return nil
}

// GetMetadata implements interfaces.BigQuery.
func (x *GeneralMock) GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error) {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	if len(x.Metadata) == 0 {
		return nil, nil
	}
	md := x.Metadata[0]
	x.Metadata = x.Metadata[1:]
	return md, nil
}

func (x *GeneralMock) NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.OpenedStream = append(x.OpenedStream, struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		Schema  bigquery.Schema
	}{Dataset: datasetID, Table: tableID, Schema: schema})

	s := &MockStream{}
	x.Streams = append(x.Streams, s)
	return s, nil
}

// Query implements interfaces.BigQuery.
func (x *GeneralMock) Query(ctx context.Context, query string) (interfaces.BigQueryIterator, error) {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.Queries = append(x.Queries, query)
	return nil, nil
}

// UpdateTable implements interfaces.BigQuery.
func (x *GeneralMock) UpdateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.UpdatedTable = append(x.UpdatedTable, struct {
		Dataset types.BQDatasetID
		Table   types.BQTableID
		MD      bigquery.TableMetadataToUpdate
		ETag    string
	}{Dataset: dataset, Table: table, MD: md, ETag: eTag})

	return nil
}

func (x *GeneralMock) Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error {
	return nil
}

type MockInsertedData struct {
	DatasetID types.BQDatasetID
	TableID   types.BQTableID
	Schema    bigquery.Schema
	Data      []any
}

var _ interfaces.BigQuery = &GeneralMock{}
