package types

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/m-mizutani/goerr"
)

// RequestID is a unique identifier for each request
type RequestID string

func NewRequestID() RequestID      { return RequestID(uuid.NewString()) }
func (x RequestID) Empty() bool    { return x == "" }
func (x RequestID) String() string { return string(x) }

// Ingestion metadata
type IngestID string
type LogID string

func NewIngestID() IngestID { return IngestID(uuid.NewString()) }

func NewLogID(data any) (LogID, error) {
	h := md5.New()
	if err := json.NewEncoder(h).Encode(data); err != nil {
		return "", goerr.Wrap(err, "failed to encode data for new ID").With("data", data)
	}

	return LogID(hex.EncodeToString(h.Sum(nil))), nil
}

// Google Cloud Platform
type GoogleProjectID string
type PubSubTopicID string
type PubSubMessageID string

func (x GoogleProjectID) String() string { return string(x) }
func (x PubSubTopicID) String() string   { return string(x) }

type BQDatasetID string
type BQTableID string
type BQPartition string

func (x BQDatasetID) String() string { return string(x) }
func (x BQTableID) String() string   { return string(x) }

const (
	BQPartitionNone  BQPartition = ""
	BQPartitionHour  BQPartition = "hour"
	BQPartitionDay   BQPartition = "day"
	BQPartitionMonth BQPartition = "month"
	BQPartitionYear  BQPartition = "year"
)

func (x BQPartition) Type() bigquery.TimePartitioningType {
	mapping := map[BQPartition]bigquery.TimePartitioningType{
		BQPartitionHour:  bigquery.HourPartitioningType,
		BQPartitionDay:   bigquery.DayPartitioningType,
		BQPartitionMonth: bigquery.MonthPartitioningType,
		BQPartitionYear:  bigquery.YearPartitioningType,
	}

	if v, ok := mapping[x]; ok {
		return v
	}
	return ""
}

type CSBucket string
type CSObjectID string
type CSUrl string

func (x CSBucket) String() string   { return string(x) }
func (x CSObjectID) String() string { return string(x) }
func (x CSUrl) String() string      { return string(x) }

func (x CSUrl) Parse() (CSBucket, CSObjectID, error) {
	// convert gs://bucket/object to (bucket, object)

	if !strings.HasPrefix(string(x), "gs://") {
		return "", "", goerr.Wrap(ErrInvalidOption, "CSUrl has invalid prefix").With("url", x)
	}

	parts := strings.Split(string(x), "/")
	if len(parts) < 4 {
		return "", "", goerr.Wrap(ErrInvalidOption, "CSUrl is invalid").With("url", x)
	}

	if parts[0] != "gs:" || parts[1] != "" {
		return "", "", goerr.Wrap(ErrInvalidOption, "CSUrl is invalid").With("url", x)
	}

	if parts[2] == "" {
		return "", "", goerr.Wrap(ErrInvalidOption, "CSUrl has empty bucket").With("url", x)
	}

	bucket := CSBucket(parts[2])
	object := CSObjectID(strings.Join(parts[3:], "/"))

	return bucket, object, nil
}

type ObjectURL string
type ObjectType string

const (
	UnknownObject      ObjectType = ""
	CloudStorageObject ObjectType = "cs"
)

func (x ObjectURL) Type() ObjectType {
	if strings.HasPrefix(string(x), "gs://") {
		return CloudStorageObject
	}

	return UnknownObject
}

func (x ObjectURL) ParseAsCloudStorage() (CSBucket, CSObjectID, error) {
	if x.Type() != CloudStorageObject {
		return "", "", goerr.Wrap(ErrInvalidOption, "ObjectURL is not CloudStorage").With("url", x)
	}

	return CSUrl(x).Parse()
}

// Object information
type ObjectParser string

const (
	JSONParser ObjectParser = "json"
)

type ObjectCompress string

const (
	NoCompress ObjectCompress = ""
	GZIPComp   ObjectCompress = "gzip"
)

type ObjectSchema string

func (x ObjectSchema) Query() string { return "data.schema." + string(x) }

// EventSchema presents schema of event data that is received from HTTP request.
type EventSchema string

const (
	CloudStorageEventSchema EventSchema = "cs"
	SwarmEventSchema        EventSchema = "swarm"
)

func (x EventSchema) Query() string { return "data.event." + string(x) }

type (
	MsgType  string
	MsgState string
)

const (
	MsgPubSub MsgType = "pubsub"

	MsgFailed    MsgState = "failed"
	MsgRunning   MsgState = "running"
	MsgCompleted MsgState = "completed"
)
