package model

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type LoadLog struct {
	ID         types.RequestID `json:"id" bigquery:"id"`
	StartedAt  time.Time       `json:"started_at" bigquery:"started_at"`
	FinishedAt time.Time       `json:"finished_at" bigquery:"finished_at"`
	Success    bool            `json:"success" bigquery:"success"`
	Sources    []*SourceLog    `json:"sources" bigquery:"sources"`
	Ingests    []*IngestLog    `json:"ingests" bigquery:"ingests"`
	Error      string          `json:"error" bigquery:"error"`
}

type SourceLog struct {
	CS         *CloudStorageObject `json:"cs" bigquery:"cs"`
	Source     Source              `json:"source" bigquery:"source"`
	RowCount   int                 `json:"row_count" bigquery:"row_count"`
	StartedAt  time.Time           `json:"started_at" bigquery:"started_at"`
	FinishedAt time.Time           `json:"finished_at" bigquery:"finished_at"`
	Success    bool                `json:"success" bigquery:"success"`
}

type IngestLog struct {
	ID           types.IngestID     `json:"id" bigquery:"id"`
	StartedAt    time.Time          `json:"started_at" bigquery:"started_at"`
	FinishedAt   time.Time          `json:"finished_at" bigquery:"finished_at"`
	ObjectSchema types.ObjectSchema `json:"object_schema" bigquery:"object_schema"`
	DatasetID    types.BQDatasetID  `json:"dataset_id" bigquery:"dataset_id"`
	TableID      types.BQTableID    `json:"table_id" bigquery:"table_id"`
	TableSchema  string             `json:"table_schema" bigquery:"table_schema"`
	LogCount     int                `json:"log_count" bigquery:"log_count"`
	Success      bool               `json:"success" bigquery:"success"`
	Error        string             `json:"error" bigquery:"error"`
}

type LoadLogRaw struct {
	LoadLog
	StartedAt  int64           `json:"started_at" bigquery:"started_at"`
	FinishedAt int64           `json:"finished_at" bigquery:"finished_at"`
	Ingests    []*IngestLogRaw `json:"ingests" bigquery:"ingests"`
	Sources    []*SourceLogRaw `json:"sources" bigquery:"sources"`
}

type SourceLogRaw struct {
	SourceLog
	StartedAt  int64 `json:"started_at" bigquery:"started_at"`
	FinishedAt int64 `json:"finished_at" bigquery:"finished_at"`
}

func (x *SourceLog) Raw() *SourceLogRaw {
	return &SourceLogRaw{
		SourceLog:  *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
	}
}

type IngestLogRaw struct {
	IngestLog
	StartedAt  int64 `json:"started_at" bigquery:"started_at"`
	FinishedAt int64 `json:"finished_at" bigquery:"finished_at"`
}

func (x *IngestLog) Raw() *IngestLogRaw {
	return &IngestLogRaw{
		IngestLog:  *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
	}
}

func (x *LoadLog) Raw() *LoadLogRaw {
	resp := &LoadLogRaw{
		LoadLog:    *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),

		Sources: make([]*SourceLogRaw, len(x.Sources)),
		Ingests: make([]*IngestLogRaw, len(x.Ingests)),
	}

	for i, source := range x.Sources {
		resp.Sources[i] = source.Raw()
	}

	for i, route := range x.Ingests {
		resp.Ingests[i] = route.Raw()
	}

	return resp
}

type LogRecord struct {
	// NOTICE: Must update LogRecordRaw also when adding new fields to LogRecord
	ID         types.LogID    `json:"id" bigquery:"id"`
	IngestID   types.IngestID `json:"ingest_id" bigquery:"ingest_id"`
	Timestamp  time.Time      `json:"timestamp" bigquery:"timestamp"`
	IngestedAt time.Time      `json:"ingested_at" bigquery:"ingested_at"`
	Data       any            `json:"data" bigquery:"data"`
}

func (x LogRecord) Raw() *LogRecordRaw {
	return &LogRecordRaw{
		LogRecord:  x,
		Timestamp:  x.Timestamp.UnixMicro(),
		IngestedAt: x.IngestedAt.UnixMicro(),
	}
}

// LogRecordRaw is replaced LogRecord with Timestamp from time.Time to int64. BigQuery Storage Write API requires converting data to protocol buffer. But adapt.StorageSchemaToProto2Descriptor is not supported for time.Time. It uses int64 for timestamp instead of time.Time. So, LogRecordRaw is used for only insertion by BigQuery Storage Write API.
type LogRecordRaw struct {
	LogRecord
	Timestamp  int64 `json:"timestamp" bigquery:"timestamp"`
	IngestedAt int64 `json:"ingested_at" bigquery:"ingested_at"`
}

type LogRecordSet map[BigQueryDest][]*LogRecord

func (x LogRecordSet) Merge(src LogRecordSet) {
	for srcKey, srcRecords := range src {
		x[srcKey] = append(x[srcKey], srcRecords...)
	}
}
