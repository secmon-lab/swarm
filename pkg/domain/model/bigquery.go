package model

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type RequestLog struct {
	ID         types.RequestID
	CSBucket   types.CSBucket
	CSObjectID types.CSObjectID
	StartedAt  time.Time
	FinishedAt time.Time
	Success    bool
	Streams    []*StreamLog
	Error      string
}

type StreamLog struct {
	ID           types.StreamID
	StartedAt    time.Time
	FinishedAt   time.Time
	ObjectSchema types.ObjectSchema
	TableSchema  string
	DatasetID    types.BQDatasetID
	TableID      types.BQTableID
	LogCount     int
	Success      bool
	Error        string
}

type RequestLogRaw struct {
	RequestLog
	StartedAt  int64
	FinishedAt int64
	Streams    []*StreamLogRaw
}

type StreamLogRaw struct {
	StreamLog
	StartedAt  int64
	FinishedAt int64
}

func (x *RequestLog) Raw() *RequestLogRaw {
	resp := &RequestLogRaw{
		RequestLog: *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
		Streams:    make([]*StreamLogRaw, len(x.Streams)),
	}

	for i, stream := range x.Streams {
		resp.Streams[i] = stream.Raw()
	}

	return resp
}

func (x *StreamLog) Raw() *StreamLogRaw {
	return &StreamLogRaw{
		StreamLog:  *x,
		StartedAt:  x.StartedAt.UnixMicro(),
		FinishedAt: x.FinishedAt.UnixMicro(),
	}
}

type LogRecord struct {
	// NOTICE: Must update LogRecordRaw also when adding new fields to LogRecord
	ID         types.LogID
	StreamID   types.StreamID
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
