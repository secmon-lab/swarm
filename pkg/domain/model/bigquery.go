package model

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type EventLog struct {
	ID         types.RequestID
	CSBucket   types.CSBucket
	CSObjectID types.CSObjectID
	StartedAt  time.Time
	FinishedAt time.Time
	Success    bool
	Ingests    []*IngestLog
	Error      string
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

type EventLogRaw struct {
	EventLog
	StartedAt  int64
	FinishedAt int64
	Ingests    []*IngestLogRaw
}

type IngestLogRaw struct {
	IngestLog
	StartedAt  int64
	FinishedAt int64
}

func (x *EventLog) Raw() *EventLogRaw {
	resp := &EventLogRaw{
		EventLog:   *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
		Ingests:    make([]*IngestLogRaw, len(x.Ingests)),
	}

	for i, route := range x.Ingests {
		resp.Ingests[i] = route.Raw()
	}

	return resp
}

func (x *IngestLog) Raw() *IngestLogRaw {
	return &IngestLogRaw{
		IngestLog:  *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
	}
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
