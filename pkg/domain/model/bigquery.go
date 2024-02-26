package model

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type LoadLog struct {
	ID         types.RequestID
	StartedAt  time.Time
	FinishedAt time.Time
	Success    bool
	Sources    []*SourceLog
	Ingests    []*IngestLog
	Error      string
}

type SourceLog struct {
	CS *CloudStorageObject
	Source
	RowCount   int
	StartedAt  time.Time
	FinishedAt time.Time
	Success    bool
}

type IngestLog struct {
	ID           types.IngestID
	StartedAt    time.Time
	FinishedAt   time.Time
	ObjectSchema types.ObjectSchema
	DatasetID    types.BQDatasetID
	TableID      types.BQTableID
	TableSchema  string
	LogCount     int
	Success      bool
}

type LoadLogRaw struct {
	LoadLog
	StartedAt  int64
	FinishedAt int64
	Ingests    []*IngestLogRaw
	Sources    []*SourceLogRaw
}

type SourceLogRaw struct {
	SourceLog
	StartedAt  int64
	FinishedAt int64
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
	StartedAt  int64
	FinishedAt int64
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
	ID         types.LogID
	IngestID   types.IngestID
	Timestamp  time.Time
	InsertedAt time.Time
	Data       any
}

func (x LogRecord) Raw() *LogRecordRaw {
	return &LogRecordRaw{
		LogRecord:  x,
		Timestamp:  x.Timestamp.UnixMicro(),
		InsertedAt: x.InsertedAt.UnixMicro(),
	}
}

// LogRecordRaw is replaced LogRecord with Timestamp from time.Time to int64. BigQuery Storage Write API requires converting data to protocol buffer. But adapt.StorageSchemaToProto2Descriptor is not supported for time.Time. It uses int64 for timestamp instead of time.Time. So, LogRecordRaw is used for only insertion by BigQuery Storage Write API.
type LogRecordRaw struct {
	LogRecord
	Timestamp  int64
	InsertedAt int64
}

type LogRecordSet map[BigQueryDest][]*LogRecord

func (x LogRecordSet) Merge(src LogRecordSet) {
	for srcKey, srcRecords := range src {
		x[srcKey] = append(x[srcKey], srcRecords...)
	}
}
