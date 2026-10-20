package usecase

import (
	"context"
	"errors"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
)

// maxTableUpdateAttempts is the maximum number of attempts to update a table schema when the update fails because another writer updated the table at the same time.
const maxTableUpdateAttempts = 5

type tableKey struct {
	dataset types.BQDatasetID
	table   types.BQTableID
}

// tableSchemaCache keeps the latest known schema of each table to skip GetMetadata and UpdateTable when the schema of records is already included in it. The cached schema is used only to decide whether the table needs to be fetched again, so a stale schema that lacks fields added by another process is still safe: such records are not included in it and cause a fetch.
type tableSchemaCache struct {
	mutex  sync.Mutex
	tables map[tableKey]*tableSchemaEntry
}

type tableSchemaEntry struct {
	// mutex is held while the table is fetched and updated, so that concurrent writers of the same table in this process do not create or update the table at the same time.
	mutex  sync.Mutex
	schema bigquery.Schema
}

func newTableSchemaCache() *tableSchemaCache {
	return &tableSchemaCache{
		tables: make(map[tableKey]*tableSchemaEntry),
	}
}

func (x *tableSchemaCache) entry(key tableKey) *tableSchemaEntry {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	e, ok := x.tables[key]
	if !ok {
		e = &tableSchemaEntry{}
		x.tables[key] = e
	}
	return e
}

// ensureTableSchema makes sure that the table has all fields of md.Schema and returns the table schema to write records with. It calls GetMetadata and UpdateTable only when md.Schema has a field that the cached schema does not have.
func (x *tableSchemaCache) ensureTableSchema(ctx context.Context, bq interfaces.BigQuery, datasetID types.BQDatasetID, tableID types.BQTableID, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	e := x.entry(tableKey{dataset: datasetID, table: tableID})
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.schema != nil && schemaIncludes(e.schema, md.Schema) {
		return e.schema, nil
	}

	schema, err := createOrUpdateTable(ctx, bq, datasetID, tableID, md)
	if err != nil {
		return nil, err
	}
	e.schema = schema
	return schema, nil
}

// invalidate discards the cached schema of the table. It is called when writing records fails with a schema mismatch, which means the cached schema is different from the actual table schema.
func (x *tableSchemaCache) invalidate(datasetID types.BQDatasetID, tableID types.BQTableID) {
	e := x.entry(tableKey{dataset: datasetID, table: tableID})
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.schema = nil
}

// schemaIncludes returns true if all fields of sub are included in base with compatible types.
func schemaIncludes(base, sub bigquery.Schema) bool {
	merged, err := bqs.Merge(base, sub)
	if err != nil {
		return false
	}
	return bqs.Equal(base, merged)
}

// createOrUpdateTable creates the table or adds fields of md.Schema to the table, and returns the resulting table schema. When the update fails because the table was updated after GetMetadata, it fetches the metadata again and retries up to maxTableUpdateAttempts times.
func createOrUpdateTable(ctx context.Context, bq interfaces.BigQuery, datasetID types.BQDatasetID, tableID types.BQTableID, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	for attempt := 1; ; attempt++ {
		schema, err := createOrUpdateTableOnce(ctx, bq, datasetID, tableID, md)
		if err == nil {
			return schema, nil
		}
		if !errors.Is(err, types.ErrTableETagMismatch) || attempt >= maxTableUpdateAttempts {
			return nil, goerr.Wrap(err, "failed to create or update table", goerr.V("attempt", attempt))
		}
		utils.CtxLogger(ctx).Warn("table was updated by another writer, retrying",
			"datasetID", datasetID, "tableID", tableID, "attempt", attempt)
	}
}

func createOrUpdateTableOnce(ctx context.Context, bq interfaces.BigQuery, datasetID types.BQDatasetID, tableID types.BQTableID, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	old, err := bq.GetMetadata(ctx, datasetID, tableID)
	if err != nil {
		return nil, goerr.Wrap(err, "Failed to get metadata", goerr.V("datasetID", datasetID), goerr.V("tableID", tableID))
	}

	if old == nil {
		utils.CtxLogger(ctx).Info("creating new table", "datasetID", datasetID, "tableID", tableID)
		return md.Schema, bq.CreateTable(ctx, datasetID, tableID, md)
	}

	merged, err := bqs.Merge(old.Schema, md.Schema)
	if err != nil {
		return nil, goerr.Wrap(err, "Failed to merge schema", goerr.V("old", old.Schema), goerr.V("new", md.Schema))
	}

	// If schema is not changed, do nothing
	if bqs.Equal(old.Schema, merged) {
		return merged, nil
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: merged,
	}
	utils.CtxLogger(ctx).Info("updating table schema", "datasetID", datasetID, "tableID", tableID)

	if err := bq.UpdateTable(ctx, datasetID, tableID, update, old.ETag); err != nil {
		return nil, goerr.Wrap(err, "Failed to update table", goerr.V("datasetID", datasetID), goerr.V("tableID", tableID))
	}
	return merged, nil
}

func inferSchema[T any](data []T) (bigquery.Schema, error) {
	var merged bigquery.Schema
	for _, d := range data {
		schema, err := bqs.Infer(d)
		if err != nil {
			return nil, goerr.Wrap(err, "Failed to infer schema", goerr.V("data", d))
		}

		merged, err = bqs.Merge(merged, schema)
		if err != nil {
			return nil, goerr.Wrap(err, "Failed to merge schema")
		}
	}

	return merged, nil
}

func setupLoadLogTable(ctx context.Context, bq interfaces.BigQuery, meta *model.MetadataConfig) (bigquery.Schema, error) {
	schema, err := bqs.Infer(&model.LoadLog{
		Sources: []*model.SourceLog{
			{
				CS:     &model.CloudStorageObject{},
				Source: model.Source{},
			},
		},
		Ingests: []*model.IngestLog{{}},
	})
	if err != nil {
		return nil, goerr.Wrap(err, "failed to infer schema")
	}
	md := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "started_at",
			Type:  bigquery.MonthPartitioningType,
		},
	}
	if _, err := createOrUpdateTable(ctx, bq, meta.Dataset(), meta.Table(), md); err != nil {
		return nil, goerr.Wrap(err, "failed to create or update table")
	}

	return schema, nil
}
