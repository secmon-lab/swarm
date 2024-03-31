package usecase

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func (x *UseCase) LoadDataByObject(ctx context.Context, url types.CSUrl) error {
	bucket, objName, err := url.Parse()
	if err != nil {
		return goerr.Wrap(err, "failed to parse CloudStorage URL").With("url", url)
	}

	csObj := model.CloudStorageObject{
		Bucket: bucket,
		Name:   objName,
	}

	attrs, err := x.clients.CloudStorage().Attrs(ctx, csObj)
	if err != nil {
		return goerr.Wrap(err, "failed to get object attributes").With("obj", csObj)
	}

	obj := model.NewObjectFromCloudStorageAttrs(attrs)
	sources, err := x.ObjectToSources(ctx, obj)
	if err != nil {
		return goerr.Wrap(err, "failed to convert event to sources")
	}

	var loadReq []*model.LoadRequest
	for _, src := range sources {
		loadReq = append(loadReq, &model.LoadRequest{
			Object: obj,
			Source: *src,
		})
	}

	return x.Load(ctx, loadReq)
}

type ingestRequest struct {
	dst     model.BigQueryDest
	records []*model.LogRecord
}

func (x *UseCase) Load(ctx context.Context, requests []*model.LoadRequest) error {
	reqID, ctx := utils.CtxRequestID(ctx)

	loadLog := model.LoadLog{
		ID:        reqID,
		StartedAt: time.Now(),
	}

	if x.metadata != nil {
		schema, err := setupLoadLogTable(ctx, x.clients.BigQuery(), x.metadata)
		if err != nil {
			return err
		}
		s, err := x.clients.BigQuery().NewStream(ctx, x.metadata.Dataset(), x.metadata.Table(), schema)
		if err != nil {
			return err
		}

		defer func() {
			if err := s.Insert(ctx, []any{loadLog.Raw()}); err != nil {
				utils.HandleError(ctx, "failed to insert request log", err)
			}
		}()
	}
	defer func() {
		loadLog.FinishedAt = time.Now()
		utils.CtxLogger(ctx).Info("request handled", "req", requests, "proc.log", loadLog)
	}()

	logRecords, srcLogs, err := importLogRecords(ctx, x.clients, requests, x.readObjectConcurrency)
	loadLog.Sources = srcLogs
	if err != nil {
		loadLog.Error = err.Error()
		return err
	}

	reqCh := make(chan ingestRequest, len(logRecords))
	for dst := range logRecords {
		reqCh <- ingestRequest{dst: dst, records: logRecords[dst]}
	}
	close(reqCh)

	errCh := make(chan error, len(logRecords))
	logCh := make(chan *model.IngestLog, len(logRecords))
	var wg sync.WaitGroup
	for i := 0; i < x.ingestTableConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for req := range reqCh {
				log, err := ingestRecords(ctx, x.clients.BigQuery(), req.dst, req.records, x.ingestRecordConcurrency)
				logCh <- log
				if err != nil {
					log.Error = err.Error()
					errCh <- err
				}
			}
		}()
	}

	wg.Wait()

	close(logCh)
	for log := range logCh {
		loadLog.Ingests = append(loadLog.Ingests, log)
	}

	close(errCh)
	for err := range errCh {
		loadLog.Error = err.Error()
		return err
	}

	loadLog.Success = true
	return nil
}

type importSourceResponse struct {
	dstMap model.LogRecordSet
	log    *model.SourceLog
}

func importLogRecords(ctx context.Context, clients *infra.Clients, requests []*model.LoadRequest, concurrency int) (model.LogRecordSet, []*model.SourceLog, *multierror.Error) {
	var logs []*model.SourceLog
	dstMap := model.LogRecordSet{}

	var wg sync.WaitGroup
	reqCh := make(chan *model.LoadRequest, len(requests))
	respCh := make(chan *importSourceResponse, len(requests))
	errCh := make(chan error, len(requests))

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqCh {
				result, err := importSource(ctx, clients, req)
				if err != nil {
					utils.HandleError(ctx, "failed to import source", err)
					errCh <- err
				}
				respCh <- result
			}
		}()
	}

	for i := 0; i < len(requests); i++ {
		reqCh <- requests[i]
	}
	close(reqCh)
	wg.Wait()
	close(respCh)
	close(errCh)

	for req := range respCh {
		logs = append(logs, req.log)
		dstMap.Merge(req.dstMap)
	}

	var mErr *multierror.Error
	for err := range errCh {
		mErr = multierror.Append(mErr, err)
	}

	return dstMap, logs, mErr
}

func importSource(ctx context.Context, clients *infra.Clients, req *model.LoadRequest) (*importSourceResponse, error) {
	result := &importSourceResponse{
		dstMap: model.LogRecordSet{},
		log: &model.SourceLog{
			CS:        req.Object.CS,
			RowCount:  0,
			Source:    req.Source,
			StartedAt: time.Now(),
		},
	}
	defer func() {
		result.log.FinishedAt = time.Now()
	}()

	rows, err := downloadCloudStorageObject(ctx, clients.CloudStorage(), req)
	if err != nil {
		return result, err
	}

	for _, row := range rows {
		result.log.RowCount++

		var output model.SchemaPolicyOutput
		if err := clients.Policy().Query(ctx, req.Source.Schema.Query(), row, &output); err != nil {
			return result, err
		}

		if len(output.Logs) == 0 {
			utils.CtxLogger(ctx).Warn("No log data in schema policy", "req", req, "record", row)
			continue
		}

		for _, log := range output.Logs {
			if err := log.Validate(); err != nil {
				return result, err
			}

			newData := cloneWithoutNil(log.Data)

			if log.ID == "" {
				// TODO: Fix this when adding another object storage service, such as S3
				log.ID, err = types.NewLogID(newData)
				if err != nil {
					return result, err
				}
			}

			tsNano := math.Mod(log.Timestamp, 1.0) * 1000 * 1000 * 1000
			record := &model.LogRecord{
				ID:         log.ID,
				Timestamp:  time.Unix(int64(log.Timestamp), int64(tsNano)),
				IngestedAt: time.Now(),

				// If there is a field that has nil value in the log.Data, the field can not be estimated field type by bqs.Infer. It will cause an error when inserting data to BigQuery. So, remove nil value from log.Data.
				Data: newData,
			}

			result.dstMap[log.BigQueryDest] = append(result.dstMap[log.BigQueryDest], record)
		}
	}

	result.log.Success = true
	return result, nil
}

func downloadCloudStorageObject(ctx context.Context, csClient interfaces.CloudStorage, req *model.LoadRequest) ([]any, error) {
	var records []any
	reader, err := csClient.Open(ctx, *req.Object.CS)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to open object").With("req", req)
	}
	defer reader.Close()

	if req.Source.Compress == types.GZIPComp {
		r, err := gzip.NewReader(reader)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to create gzip reader").With("req", req)
		}
		defer r.Close()
		reader = r
	}

	decoder := json.NewDecoder(reader)
	for decoder.More() {
		var record any
		if err := decoder.Decode(&record); err != nil {
			return nil, goerr.Wrap(err, "failed to decode JSON").With("req", req)
		}

		records = append(records, record)
	}

	return records, nil
}

const maxIngestLogCount = 256

func ingestRecords(ctx context.Context, bq interfaces.BigQuery, bqDst model.BigQueryDest, records []*model.LogRecord, concurrency int) (*model.IngestLog, error) {
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

	md, err := buildBQMetadata(schema, bqDst.Partition)
	if err != nil {
		return result, err
	}

	finalized, err := createOrUpdateTable(ctx, bq, bqDst.Dataset, bqDst.Table, md)
	if err != nil {
		return result, goerr.Wrap(err, "failed to update schema").With("dst", bqDst)
	}

	jsonSchema, err := schemaToJSON(schema)
	if err != nil {
		return result, err
	}
	result.TableSchema = string(jsonSchema)

	recordsCh := make(chan []*model.LogRecord, len(records)/maxIngestLogCount+1)
	for i := 0; i < len(records); i += maxIngestLogCount {
		end := min(i+maxIngestLogCount, len(records))
		subRecords := records[i:end]
		recordsCh <- subRecords
	}
	close(recordsCh)

	var wg sync.WaitGroup
	stream, err := bq.NewStream(ctx, bqDst.Dataset, bqDst.Table, finalized)
	if err != nil {
		return result, err
	}
	defer utils.SafeClose(stream)

	errCh := make(chan error)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for subRecords := range recordsCh {
				data := make([]any, len(subRecords))
				for i := range subRecords {
					subRecords[i].IngestID = ingestID
					data[i] = subRecords[i].Raw()
				}

				startedAt := time.Now()
				if err := stream.Insert(ctx, data); err != nil {
					errCh <- goerr.Wrap(err, "failed to insert data").With("dst", bqDst)
				}
				utils.CtxLogger(ctx).Debug("inserted data", "dst", bqDst, "count", len(data), "duration", time.Since(startedAt))
			}
		}()
	}

	wg.Wait()
	close(errCh)

	var mErr *multierror.Error
	for err := range errCh {
		utils.HandleError(ctx, "failed to insert data", err)
		mErr = multierror.Append(mErr, err)
	}
	if mErr != nil {
		return result, mErr
	}

	result.Success = true
	return result, nil
}
