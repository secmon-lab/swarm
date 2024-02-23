package usecase

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/hashicorp/go-multierror"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/policy"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func (x *UseCase) LoadDataByObject(ctx context.Context, url types.CSUrl) error {
	bucket, objID, err := url.Parse()
	if err != nil {
		return goerr.Wrap(err, "failed to parse CloudStorage URL").With("url", url)
	}

	attrs, err := x.clients.CloudStorage().Attrs(ctx, bucket, objID)
	if err != nil {
		return goerr.Wrap(err, "failed to get object attributes").With("bucket", bucket).With("objID", objID)
	}

	req := &model.LoadDataRequest{
		CSEvent: &model.CloudStorageEvent{
			Bucket:       bucket,
			Name:         objID,
			Size:         fmt.Sprintf("%d", attrs.Size),
			Etag:         attrs.Etag,
			ContentType:  attrs.ContentType,
			Generation:   fmt.Sprintf("%d", attrs.Generation),
			Kind:         "storage#object",
			Md5Hash:      string(attrs.MD5),
			MediaLink:    attrs.MediaLink,
			StorageClass: attrs.StorageClass,
			TimeCreated:  attrs.Created.Format("2006-01-02T15:04:05.999Z"),
			Updated:      attrs.Updated.Format("2006-01-02T15:04:05.999Z"),
		},
	}

	return x.LoadData(ctx, req)
}

func (x *UseCase) LoadData(ctx context.Context, req *model.LoadDataRequest) (e error) {
	if req.CSEvent == nil {
		return goerr.Wrap(types.ErrAssertion, "CSEvent is nil").With("req", req)
	}

	reqID, ctx := utils.CtxRequestID(ctx)
	startedAt := time.Now()

	eventLog := &model.EventLog{
		ID:         reqID,
		CSBucket:   req.CSEvent.Bucket,
		CSObjectID: req.CSEvent.Name,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
	}

	if x.metadata != nil {
		schema, err := bqs.Infer(&model.EventLog{
			Ingests: []*model.IngestLog{{}},
		})
		if err != nil {
			return goerr.Wrap(err, "failed to infer schema").With("req", req)
		}
		md := &bigquery.TableMetadata{
			Schema: schema,
		}
		if _, err := x.CreateOrUpdateTable(ctx, x.metadata.Dataset(), x.metadata.Table(), md); err != nil {
			return goerr.Wrap(err, "failed to create or update table").With("req", req)
		}

		defer func() {
			eventLog.FinishedAt = time.Now()
			if err := x.clients.BigQuery().Insert(ctx, x.metadata.Dataset(), x.metadata.Table(), schema, []any{eventLog.Raw()}); err != nil {
				utils.HandleError(ctx, "failed to insert request log", err)
				e = err
			}
		}()
	}

	sLogs, err := x.handleEvent(ctx, req)
	eventLog.Ingests = sLogs
	eventLog.Success = err == nil
	if err != nil {
		eventLog.Error = err.Error()
		return goerr.Wrap(err, "failed to handle request").With("req", req)
	}

	return nil
}

func (x *UseCase) handleEvent(ctx context.Context, req *model.LoadDataRequest) ([]*model.IngestLog, error) {
	if req.CSEvent == nil {
		return nil, goerr.Wrap(types.ErrAssertion, "CSEvent is nil").With("req", req)
	}

	var event model.EventPolicyOutput
	if err := x.clients.Policy().Query(ctx, "data.event", req.CSEvent, &event); err != nil {
		return nil, err
	}
	if len(event.Sources) == 0 {
		return nil, goerr.Wrap(types.ErrNoPolicyResult, "no source in event").With("req", req)
	}

	var errors *multierror.Error
	var ingestLogs []*model.IngestLog

	for _, s := range event.Sources {
		logs, err := x.handleSource(ctx, req, s)
		if err != nil {
			utils.HandleError(ctx, "failed to handle stream", err)
			errors = multierror.Append(errors, err)
		}
		ingestLogs = append(ingestLogs, logs...)
	}

	return ingestLogs, errors.ErrorOrNil()
}

func (x *UseCase) handleSource(ctx context.Context, req *model.LoadDataRequest, s *model.Source) ([]*model.IngestLog, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	rawRecords, err := downloadCloudStorageObject(ctx,
		x.clients.CloudStorage(),
		req.CSEvent.Bucket,
		req.CSEvent.Name,
		s,
	)
	if err != nil {
		return nil, err
	}

	logs, err := parseRawRecords(ctx, rawRecords, x.clients.Policy(), s.Schema)
	if err != nil {
		return nil, err
	}

	dstMap := map[model.BigQueryDest][]*model.LogRecord{}
	for idx, log := range logs {
		if err := log.Validate(); err != nil {
			return nil, err
		}
		if log.ID == "" {
			log.ID = types.NewLogID(req.CSEvent.Bucket, req.CSEvent.Name, idx)
		}

		tsNano := math.Mod(log.Timestamp, 1.0) * 1000 * 1000 * 1000
		dstMap[log.BigQueryDest] = append(dstMap[log.BigQueryDest], &model.LogRecord{
			ID:         log.ID,
			Timestamp:  time.Unix(int64(log.Timestamp), int64(tsNano)),
			InsertedAt: time.Now(),

			// If there is a field that has nil value in the log.Data, the field can not be estimated field type by bqs.Infer. It will cause an error when inserting data to BigQuery. So, remove nil value from log.Data.
			Data: cloneWithoutNil(log.Data),
		})
	}

	var ingestLogs []*model.IngestLog
	for dst, records := range dstMap {
		log, err := x.ingestRecords(ctx, dst, records)

		log.DatasetID = dst.Dataset
		log.TableID = dst.Table
		log.ObjectSchema = s.Schema
		ingestLogs = append(ingestLogs, log)
		if err != nil {
			return ingestLogs, err
		}
	}

	return ingestLogs, nil
}

func (x *UseCase) ingestRecords(ctx context.Context, bqDst model.BigQueryDest, records []*model.LogRecord) (*model.IngestLog, error) {
	ingestID, ctx := utils.CtxIngestID(ctx)

	result := &model.IngestLog{
		ID:        ingestID,
		StartedAt: time.Now(),
		DatasetID: bqDst.Dataset,
		TableID:   bqDst.Table,
		LogCount:  len(records),
	}

	defer func() {
		result.FinishedAt = time.Now()
	}()

	schema, err := inferSchema(records)
	if err != nil {
		return result, err
	}

	md := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "Timestamp",
			Type:  bigquery.DayPartitioningType,
		},
	}

	finalized, err := x.CreateOrUpdateTable(ctx, bqDst.Dataset, bqDst.Table, md)
	if err != nil {
		return result, goerr.Wrap(err, "failed to update schema").With("dst", bqDst)
	}
	jsonSchema, err := schema.ToJSONFields()
	if err != nil {
		return result, goerr.Wrap(err, "failed to convert schema to JSON").With("schema", schema)
	}
	result.TableSchema = string(jsonSchema)

	data := make([]any, len(records))
	for i := range records {
		data[i] = records[i].Raw()
	}

	if err := x.clients.BigQuery().Insert(ctx, bqDst.Dataset, bqDst.Table, finalized, data); err != nil {
		return result, goerr.Wrap(err, "failed to insert data").With("dst", bqDst)
	}

	result.Success = true
	return result, nil
}

func downloadCloudStorageObject(ctx context.Context, csClient interfaces.CloudStorage, bucket types.CSBucket, objID types.CSObjectID, s *model.Source) ([]any, error) {
	var records []any
	reader, err := csClient.Open(ctx, bucket, objID)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to open object").With("bucket", bucket).With("objID", objID)
	}
	defer reader.Close()

	if s.Comp == types.GZIPComp {
		r, err := gzip.NewReader(reader)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to create gzip reader").With("bucket", bucket).With("objID", objID)
		}
		defer r.Close()
		reader = r
	}

	decoder := json.NewDecoder(reader)
	for decoder.More() {
		var record any
		if err := decoder.Decode(&record); err != nil {
			return nil, goerr.Wrap(err, "failed to decode JSON").With("bucket", bucket).With("objID", objID)
		}

		records = append(records, record)
	}

	return records, nil
}

func parseRawRecords(ctx context.Context, rawLogs []any, p *policy.Client, schema types.ObjectSchema) ([]*model.Log, error) {
	logs := make([]*model.Log, 0, len(rawLogs))
	for _, r := range rawLogs {
		var output model.SchemaPolicyOutput
		if err := p.Query(ctx, schema.Query(), r, &output); err != nil {
			return nil, err
		}

		if len(output.Logs) == 0 {
			utils.CtxLogger(ctx).Warn("No log data in schema policy", "schema", schema, "record", r)
			continue
		}

		logs = append(logs, output.Logs...)
	}

	return logs, nil
}
