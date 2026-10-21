package usecase_test

import (
	"bytes"
	"context"
	_ "embed"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/policy"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
)

func TestLoadDataByObject(t *testing.T) {
	bqProject := utils.LoadEnv(t, "TEST_BIGQUERY_PROJECT_ID")
	bqDataset := utils.LoadEnv(t, "TEST_BIGQUERY_DATASET_ID")
	tableID := utils.LoadEnv(t, "TEST_BIGQUERY_TABLE_ID")
	gcsURL := utils.LoadEnv(t, "TEST_GCS_OBJECT_URL")
	policyDir := utils.LoadEnv(t, "TEST_POLICY_DIR")

	ctx := context.Background()
	bqClient := gt.R1(bq.New(ctx, types.GoogleProjectID(bqProject))).NoError(t)
	csClient := gt.R1(cs.New(ctx)).NoError(t)
	pClient := gt.R1(policy.New(policy.WithDir(policyDir))).NoError(t)
	meta := model.NewMetadataConfig(types.BQDatasetID(bqDataset), types.BQTableID(tableID))

	uc := usecase.New(
		infra.New(
			infra.WithBigQuery(bqClient),
			infra.WithCloudStorage(csClient),
			infra.WithPolicy(pClient),
		),
		usecase.WithMetadata(meta),
	)

	gt.NoError(t, uc.LoadDataByObject(ctx, types.CSUrl(gcsURL)))
}

//go:embed testdata/object/cloudtrail_example.json
var cloudTrailExampleRaw []byte

//go:embed testdata/object/cloudtrail_example.json.gz
var cloudTrailExampleGzip []byte

func TestLoadData(t *testing.T) {
	testCases := map[string]struct {
		objectName types.CSObjectID
		objectData []byte
		model.Source
	}{
		"cloudtrail_example.json": {
			objectName: "cloudtrail_example.log",
			objectData: cloudTrailExampleRaw,
			Source: model.Source{
				Parser:   types.JSONParser,
				Schema:   "cloudtrail",
				Compress: types.NoCompress,
			},
		},
		"cloudtrail_example.json.gz": {
			objectName: "cloudtrail_example.log.gz",
			objectData: cloudTrailExampleGzip,
			Source: model.Source{
				Parser:   types.JSONParser,
				Schema:   "cloudtrail",
				Compress: types.GZIPComp,
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			ctx := context.Background()
			bqClient := bq.NewGeneralMock()
			csClient := &cs.Mock{
				MockOpen: func(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader([]byte(tc.objectData))), nil
				},
			}
			pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
			meta := model.NewMetadataConfig("test-dataset", "test-table")

			uc := usecase.New(
				infra.New(
					infra.WithBigQuery(bqClient),
					infra.WithCloudStorage(csClient),
					infra.WithPolicy(pClient),
				),
				usecase.WithMetadata(meta),
			)

			req := &model.LoadRequest{
				Source: tc.Source,
				Object: model.Object{
					CS: &model.CloudStorageObject{
						Bucket: "test-bucket",
						Name:   tc.objectName,
					},
				},
			}

			gt.NoError(t, uc.Load(ctx, []*model.LoadRequest{req}))

			/*
				ids := []types.LogID{
					"ac3cfd93-435d-41cc-bbd7-aad0340ec668",
					"18e67b09-94a3-4b5c-9b3a-cd549b3341fb",
					"dbb28938-5ed4-4774-8bb6-82ea916b21bb",
					"d4dacb9d-9822-4217-b88d-d334bde89755",
				}
					gt.A(t, bqClient.Streams).Length(2)
					gt.A(t, bqClient.Streams[0].Inserted[0]).Length(1) // first stream is metadata
					gt.A(t, bqClient.Streams[1].Inserted[0]).Length(4) // second stream is data
					for i, id := range ids {
						r := gt.Cast[*model.LogRecordRaw](t, bqClient.Streams[1].Inserted[0][i])
						gt.Equal(t, r.ID, id)
					}
			*/
		})
	}
}

func newCloudTrailLoadRequest() *model.LoadRequest {
	return &model.LoadRequest{
		Source: model.Source{
			Parser:   types.JSONParser,
			Schema:   "cloudtrail",
			Compress: types.NoCompress,
		},
		Object: model.Object{
			CS: &model.CloudStorageObject{
				Bucket: "test-bucket",
				Name:   "cloudtrail_example.log",
			},
		},
	}
}

func newCloudTrailCSMock() *cs.Mock {
	return &cs.Mock{
		MockOpen: func(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(cloudTrailExampleRaw)), nil
		},
	}
}

func TestLoadReusesTableMetadata(t *testing.T) {
	ctx := context.Background()
	var mutex sync.Mutex
	getMetadataCount := map[types.BQTableID]int{}
	bqClient := &bq.Mock{
		MockGetMetadata: func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID) (*bigquery.TableMetadata, error) {
			mutex.Lock()
			defer mutex.Unlock()
			getMetadataCount[tableID]++
			return nil, nil
		},
	}
	pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
	uc := usecase.New(infra.New(
		infra.WithBigQuery(bqClient),
		infra.WithCloudStorage(newCloudTrailCSMock()),
		infra.WithPolicy(pClient),
	))

	for i := 0; i < 3; i++ {
		gt.NoError(t, uc.Load(ctx, []*model.LoadRequest{newCloudTrailLoadRequest()}))
	}

	gt.Equal(t, getMetadataCount, map[types.BQTableID]int{"cloudtrail": 1})
}

func TestLoadReusesLoadLogStream(t *testing.T) {
	ctx := context.Background()
	bqClient := bq.NewGeneralMock()
	pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
	meta := model.NewMetadataConfig("test-dataset", "test-load-logs")

	uc := usecase.New(
		infra.New(
			infra.WithBigQuery(bqClient),
			infra.WithCloudStorage(newCloudTrailCSMock()),
			infra.WithPolicy(pClient),
		),
		usecase.WithMetadata(meta),
	)

	const loadCount = 4
	var wg sync.WaitGroup
	for i := 0; i < loadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gt.NoError(t, uc.Load(ctx, []*model.LoadRequest{newCloudTrailLoadRequest()}))
		}()
	}
	wg.Wait()

	var loadLogStreams []*bq.MockStream
	for i, opened := range bqClient.OpenedStream {
		if opened.Table == "test-load-logs" {
			loadLogStreams = append(loadLogStreams, bqClient.Streams[i])
		}
	}
	gt.A(t, loadLogStreams).Length(1)
	createdLoadLogTable := 0
	for _, created := range bqClient.CreatedTable {
		if created.Table == "test-load-logs" {
			createdLoadLogTable++
		}
	}
	gt.Equal(t, createdLoadLogTable, 1)

	stream := loadLogStreams[0]
	// One row is written for each Load call.
	gt.A(t, stream.Inserted).Length(loadCount)
	for _, rows := range stream.Inserted {
		gt.A(t, rows).Length(1)
	}
	gt.False(t, stream.Closed)

	gt.NoError(t, uc.Close())
	gt.True(t, stream.Closed)
}

func TestLoadReusesTableStream(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
	uc := usecase.New(infra.New(
		infra.WithBigQuery(table.mock()),
		infra.WithCloudStorage(newCloudTrailCSMock()),
		infra.WithPolicy(pClient),
	))

	const loadCount = 4
	var wg sync.WaitGroup
	for i := 0; i < loadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gt.NoError(t, uc.Load(ctx, []*model.LoadRequest{newCloudTrailLoadRequest()}))
		}()
	}
	wg.Wait()

	// All messages write their rows through one stream of the table, each in one request.
	streams := table.openedStreams()
	gt.A(t, streams).Length(1)
	gt.A(t, streams[0].stream.Inserted).Length(loadCount)
	for _, rows := range streams[0].stream.Inserted {
		gt.A(t, rows).Length(4) // cloudtrail_example.json has 4 records
	}
	// No pending stream is created.
	gt.Equal(t, table.pendingInserts, 0)
}

func TestIngestRecordsUsesPendingStreamForLargeRows(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	cache := usecase.NewTableSchemaCache()
	streams := usecase.NewTableStreamCache()

	// A small message is written through the stream.
	gt.R1(usecase.IngestRecords(ctx, table.mock(), cache, streams, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)
	gt.Equal(t, table.pendingInserts, 0)

	// The rows of this message exceed the limit of one request together, but not one by one.
	large := strings.Repeat("x", usecase.MaxDefaultStreamAppendBytes/2)
	records := append(newRecords(map[string]any{"a": large}), newRecords(map[string]any{"a": large})...)
	gt.R1(usecase.IngestRecords(ctx, table.mock(), cache, streams, testDst, records, 1)).NoError(t)

	gt.Equal(t, table.pendingInserts, 1)
	opened := table.openedStreams()
	gt.A(t, opened).Length(1)
	gt.A(t, opened[0].stream.Inserted).Length(1) // only the small message
}

func TestIngestRecordsLimitsDefaultStreamBySerializedSize(t *testing.T) {
	const limit = usecase.MaxDefaultStreamAppendBytes
	testCases := map[string]struct {
		sizes          []int
		pendingInserts int
	}{
		"total equals the limit":  {sizes: []int{limit / 2, limit - limit/2}, pendingInserts: 0},
		"total exceeds the limit": {sizes: []int{limit / 2, limit - limit/2 + 1}, pendingInserts: 1},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			ctx := context.Background()
			table := &fakeTable{serializedSizes: tc.sizes}
			records := append(newRecords(map[string]any{"a": "x"}), newRecords(map[string]any{"a": "y"})...)

			gt.R1(usecase.IngestRecords(ctx, table.mock(), usecase.NewTableSchemaCache(), usecase.NewTableStreamCache(), testDst, records, 1)).NoError(t)

			gt.Equal(t, table.pendingInserts, tc.pendingInserts)
			gt.A(t, table.openedStreams()[0].stream.Inserted).Length(1 - tc.pendingInserts)
		})
	}
}

func TestIngestRecordsReplacesStreamForNewField(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()
	streams := usecase.NewTableStreamCache()

	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, streams, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, streams, testDst, newRecords(map[string]any{"a": "x", "b": "y"}), 1)).NoError(t)

	opened := table.openedStreams()
	gt.A(t, opened).Length(2)
	gt.False(t, hasDataField(opened[0].schema, "b"))
	gt.True(t, hasDataField(opened[1].schema, "b"))
	gt.True(t, opened[0].stream.IsClosed())
	gt.False(t, opened[1].stream.IsClosed())
	gt.A(t, opened[1].stream.Inserted).Length(1)

	// A message without the new field keeps using the new stream because the table schema has not changed.
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, streams, testDst, newRecords(map[string]any{"a": "x"}), 1)).NoError(t)
	gt.A(t, table.openedStreams()).Length(2)
	gt.A(t, opened[1].stream.Inserted).Length(2)
}

func TestIngestRecordsClosesReplacedStreamAfterInsertInProgress(t *testing.T) {
	ctx := context.Background()
	table := &fakeTable{}
	bqClient := table.mock()
	cache := usecase.NewTableSchemaCache()
	streams := usecase.NewTableStreamCache()

	// The first write through the first stream waits until finish is closed.
	started := make(chan struct{})
	finish := make(chan struct{})
	var once sync.Once
	table.onInsert = func(stream int) error {
		if stream == 0 {
			once.Do(func() {
				close(started)
				<-finish
			})
		}
		return nil
	}

	done := make(chan error)
	go func() {
		_, err := usecase.IngestRecords(ctx, bqClient, cache, streams, testDst, newRecords(map[string]any{"a": "x"}), 1)
		done <- err
	}()
	<-started

	// Another message with a new field replaces the stream while the first message is still writing.
	gt.R1(usecase.IngestRecords(ctx, bqClient, cache, streams, testDst, newRecords(map[string]any{"a": "x", "b": "y"}), 1)).NoError(t)
	opened := table.openedStreams()
	gt.A(t, opened).Length(2)
	gt.False(t, opened[0].stream.IsClosed())

	close(finish)
	gt.NoError(t, <-done)
	gt.True(t, opened[0].stream.IsClosed())
	gt.False(t, opened[1].stream.IsClosed())
}

func TestUseCaseCloseClosesAllStreams(t *testing.T) {
	ctx := context.Background()
	bqClient := bq.NewGeneralMock()
	pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
	uc := usecase.New(
		infra.New(
			infra.WithBigQuery(bqClient),
			infra.WithCloudStorage(newCloudTrailCSMock()),
			infra.WithPolicy(pClient),
		),
		usecase.WithMetadata(model.NewMetadataConfig("test-dataset", "test-load-logs")),
	)

	gt.NoError(t, uc.Load(ctx, []*model.LoadRequest{newCloudTrailLoadRequest()}))

	// One stream for the data table and one for load_logs.
	var tables []types.BQTableID
	for _, opened := range bqClient.OpenedStream {
		tables = append(tables, opened.Table)
	}
	gt.A(t, tables).UnorderedEqual([]types.BQTableID{"cloudtrail", "test-load-logs"})
	for _, s := range bqClient.Streams {
		gt.False(t, s.IsClosed())
	}

	gt.NoError(t, uc.Close())
	for _, s := range bqClient.Streams {
		gt.True(t, s.IsClosed())
	}
}

func TestIngestRecordBigNum(t *testing.T) {
	bqMock := bq.NewGeneralMock()
	ctx := context.Background()
	dst := model.BigQueryDest{
		Dataset: "test-dataset",
		Table:   "test-table",
	}

	const dataSize = 32000
	var records []*model.LogRecord
	for i := 0; i < dataSize; i++ {
		records = append(records, &model.LogRecord{
			ID:        types.LogID(uuid.NewString()),
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"key": "value",
			},
			IngestedAt: time.Now(),
		})
	}

	resp := gt.R1(usecase.IngestRecords(ctx, bqMock, usecase.NewTableSchemaCache(), usecase.NewTableStreamCache(), dst, records, 32)).NoError(t)
	gt.True(t, resp.Success)

	/*
		gt.A(t, bqMock.Streams).Length(1).At(0, func(t testing.TB, stream *bq.MockStream) {
			total := 0
			for _, r := range stream.Inserted {
				total += len(r)
			}
			gt.Equal(t, total, dataSize)
		})
	*/
}
