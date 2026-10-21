package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/m-mizutani/goerr/v2"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
)

func TestCreateOrUpdateTable(t *testing.T) {
	bqProject := utils.LoadEnv(t, "TEST_BIGQUERY_PROJECT_ID")
	bqDataset := utils.LoadEnv(t, "TEST_BIGQUERY_DATASET_ID")

	ctx := context.Background()
	bqClient := gt.R1(bq.New(ctx, types.GoogleProjectID(bqProject))).NoError(t)

	tableID := time.Now().Format("create_test_20060102_150405")

	// Create table
	gt.R1(usecase.CreateOrUpdateTable(ctx,
		bqClient,
		types.BQDatasetID(bqDataset),
		types.BQTableID(tableID),
		&bigquery.TableMetadata{
			Schema: []*bigquery.FieldSchema{
				{Name: "name", Type: bigquery.StringFieldType},
				{Name: "age", Type: bigquery.IntegerFieldType},
			},
		})).NoError(t)

	// Update table
	gt.R1(usecase.CreateOrUpdateTable(ctx,
		bqClient,
		types.BQDatasetID(bqDataset),
		types.BQTableID(tableID),
		&bigquery.TableMetadata{
			Schema: []*bigquery.FieldSchema{
				{Name: "age", Type: bigquery.IntegerFieldType},
				{Name: "address", Type: bigquery.StringFieldType},
			},
		})).NoError(t)
}

// fakeTable is a BigQuery table on memory. It counts GetMetadata and UpdateTable calls, and UpdateTable fails with ErrTableETagMismatch as many times as failUpdates.
type fakeTable struct {
	mutex       sync.Mutex
	md          *bigquery.TableMetadata
	version     int
	getCount    int
	updateCount int
	failUpdates int
	failInserts int
}

func (x *fakeTable) mock() *bq.Mock {
	return &bq.Mock{
		MockGetMetadata: func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID) (*bigquery.TableMetadata, error) {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			x.getCount++
			if x.md == nil {
				return nil, nil
			}
			return &bigquery.TableMetadata{Schema: x.md.Schema, ETag: x.md.ETag}, nil
		},
		MockCreateTable: func(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			x.version++
			x.md = &bigquery.TableMetadata{Schema: md.Schema, ETag: fmt.Sprintf("v%d", x.version)}
			return nil
		},
		MockUpdateTable: func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			x.updateCount++
			if x.failUpdates > 0 {
				x.failUpdates--
				// Another writer updated the table after the caller read it.
				x.version++
				x.md.ETag = fmt.Sprintf("v%d", x.version)
				return goerr.Wrap(types.ErrTableETagMismatch, "table was updated by another writer")
			}
			if eTag != x.md.ETag {
				return goerr.New("unexpected etag", goerr.V("expected", x.md.ETag), goerr.V("actual", eTag))
			}
			x.version++
			x.md = &bigquery.TableMetadata{Schema: md.Schema, ETag: fmt.Sprintf("v%d", x.version)}
			return nil
		},
		MockInsert: func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, data []any) error {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			if x.failInserts > 0 {
				x.failInserts--
				return goerr.Wrap(types.ErrSchemaNotMatched, "table schema does not match")
			}
			return nil
		},
	}
}

func (x *fakeTable) counts() (get, update int) {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return x.getCount, x.updateCount
}

func newRecords(data map[string]any) []*model.LogRecord {
	return []*model.LogRecord{
		{
			ID:         types.LogID(uuid.NewString()),
			Timestamp:  time.Now(),
			IngestedAt: time.Now(),
			Data:       data,
		},
	}
}

func hasDataField(schema bigquery.Schema, name string) bool {
	for _, f := range schema {
		if f.Name != "data" {
			continue
		}
		for _, sub := range f.Schema {
			if sub.Name == name {
				return true
			}
		}
	}
	return false
}

var testDst = model.BigQueryDest{Dataset: "test-dataset", Table: "test-table"}

func TestIngestRecordsUpdatesTableForNewField(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()

	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)
	get, update := table.counts()
	gt.Equal(t, get, 1)
	gt.Equal(t, update, 0)

	// A record with a new field makes the table fetched and updated.
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x", "b": "y"}), 1)).NoError(t)
	get, update = table.counts()
	gt.Equal(t, get, 2)
	gt.Equal(t, update, 1)
	gt.True(t, hasDataField(table.md.Schema, "b"))

	// Fields that are already in the table do not cause any metadata call.
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"b": "y"}), 1)).NoError(t)
	get, update = table.counts()
	gt.Equal(t, get, 2)
	gt.Equal(t, update, 1)
}

func TestIngestRecordsRetriesOnETagMismatch(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()

	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)

	table.failUpdates = 1
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x", "b": "y"}), 1)).NoError(t)

	// GetMetadata: create (1) + first update (1) + retry (1), UpdateTable: failed (1) + retry (1)
	get, update := table.counts()
	gt.Equal(t, get, 3)
	gt.Equal(t, update, 2)
	gt.True(t, hasDataField(table.md.Schema, "b"))
}

func TestIngestRecordsGivesUpETagMismatchRetries(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()

	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)

	table.failUpdates = 100
	_, err := usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x", "b": "y"}), 1)
	gt.Error(t, err)
	gt.True(t, errors.Is(err, types.ErrTableETagMismatch))

	_, update := table.counts()
	gt.Equal(t, update, 5)
}

func TestIngestRecordsRefetchesSchemaAfterSchemaMismatch(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()

	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)

	table.failInserts = 1
	_, err := usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)
	gt.True(t, errors.Is(err, types.ErrSchemaNotMatched))
	get, _ := table.counts()
	gt.Equal(t, get, 1)

	// The next attempt fetches the table metadata again instead of using the discarded schema.
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)
	get, _ = table.counts()
	gt.Equal(t, get, 2)
}
