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

	rLog := &model.RequestLog{
		ID:         reqID,
		CSBucket:   req.CSEvent.Bucket,
		CSObjectID: req.CSEvent.Name,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
	}

	if x.metadata != nil {
		schema, err := bqs.Infer(&model.RequestLog{
			Streams: []*model.StreamLog{{}},
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
			rLog.FinishedAt = time.Now()
			if err := x.clients.BigQuery().Insert(ctx, x.metadata.Dataset(), x.metadata.Table(), schema, []any{rLog.Raw()}); err != nil {
				utils.HandleError(ctx, "failed to insert request log", err)
				e = err
			}
		}()
	}

	sLogs, err := x.handleRequest(ctx, req)
	rLog.Streams = sLogs
	rLog.Success = err == nil
	if err != nil {
		rLog.Error = err.Error()
		return goerr.Wrap(err, "failed to handle request").With("req", req)
	}

	return nil
}

func (x *UseCase) handleRequest(ctx context.Context, req *model.LoadDataRequest) ([]*model.StreamLog, error) {
	if req.CSEvent == nil {
		return nil, goerr.Wrap(types.ErrAssertion, "CSEvent is nil").With("req", req)
	}

	var pipeline model.PipelinePolicyOutput
	if err := x.clients.Policy().Query(ctx, "data.pipeline", req.CSEvent, &pipeline); err != nil {
		return nil, err
	}
	if len(pipeline.Streams) == 0 {
		return nil, goerr.Wrap(types.ErrNoPolicyResult, "no stream in pipeline").With("req", req)
	}

	var errors *multierror.Error
	streamLogs := make([]*model.StreamLog, len(pipeline.Streams))

	for i, s := range pipeline.Streams {
		log, err := x.handleStream(ctx, req, s)
		if err != nil {
			utils.HandleError(ctx, "failed to handle stream", err)
			log.Error = err.Error()
			errors = multierror.Append(errors, err)
		}
		streamLogs[i] = log
	}

	return streamLogs, errors.ErrorOrNil()
}

func (x *UseCase) handleStream(ctx context.Context, req *model.LoadDataRequest, s model.Stream) (*model.StreamLog, error) {
	streamID, ctx := utils.CtxStreamID(ctx)
	sLog := &model.StreamLog{
		ID:           streamID,
		StartedAt:    time.Now(),
		ObjectSchema: s.Schema,
		DatasetID:    s.Dataset,
		TableID:      s.Table,
	}

	defer func() {
		sLog.FinishedAt = time.Now()
	}()

	if err := s.Validate(); err != nil {
		return sLog, err
	}

	records, err := downloadCloudStorageObject(ctx,
		x.clients.CloudStorage(),
		req.CSEvent.Bucket,
		req.CSEvent.Name,
		s,
	)
	if err != nil {
		return sLog, err
	}

	logs, err := parseRecords(ctx, records, x.clients.Policy(), s.Schema)
	if err != nil {
		return sLog, err
	}

	logRecords := make([]*model.LogRecord, len(logs))
	for i, log := range logs {
		if err := log.Validate(); err != nil {
			return sLog, err
		}
		if log.ID == "" {
			log.ID = types.NewLogID()
		}

		nanoSec := math.Mod(log.Timestamp, 1.0) * 1000 * 1000 * 1000

		logRecords[i] = &model.LogRecord{
			ID:         log.ID,
			StreamID:   streamID,
			Timestamp:  time.Unix(int64(log.Timestamp), int64(nanoSec)),
			InsertedAt: time.Now(),

			// If there is a field that has nil value in the log.Data, the field can not be estimated field type by bqs.Infer. It will cause an error when inserting data to BigQuery. So, remove nil value from log.Data.
			Data: cloneWithoutNil(log.Data),
		}
	}

	schema, err := inferSchema(logRecords)
	if err != nil {
		return sLog, err
	}

	md := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "Timestamp",
			Type:  bigquery.DayPartitioningType,
		},
	}

	finalized, err := x.CreateOrUpdateTable(ctx, s.Dataset, s.Table, md)
	if err != nil {
		return sLog, goerr.Wrap(err, "failed to update schema").With("dataset", s.Dataset).With("table", s.Table)
	}
	jsonSchema, err := schema.ToJSONFields()
	if err != nil {
		return sLog, goerr.Wrap(err, "failed to convert schema to JSON").With("schema", schema)
	}
	sLog.TableSchema = string(jsonSchema)

	data := make([]any, len(logRecords))
	for i := range logRecords {
		data[i] = logRecords[i].Raw()
	}

	if err := x.clients.BigQuery().Insert(ctx, s.Dataset, s.Table, finalized, data); err != nil {
		return sLog, goerr.Wrap(err, "failed to insert data").With("dataset", s.Dataset).With("table", s.Table)
	}

	sLog.Success = true
	return sLog, nil
}

func downloadCloudStorageObject(ctx context.Context, csClient interfaces.CloudStorage, bucket types.CSBucket, objID types.CSObjectID, s model.Stream) ([]any, error) {
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

func parseRecords(ctx context.Context, record []any, p *policy.Client, schema types.ObjectSchema) ([]*model.LogOutput, error) {
	logs := make([]*model.LogOutput, 0, len(record))
	for _, r := range record {
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
