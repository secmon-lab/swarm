package types

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"strings"

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

func NewLogID(bucket CSBucket, objID CSObjectID, idx int) LogID {
	h := md5.New()
	h.Write([]byte(bucket))
	h.Write([]byte{0x00})
	h.Write([]byte(objID))
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(idx))
	h.Write([]byte(bytes))

	return LogID(hex.EncodeToString(h.Sum(nil)))
}

// Google Cloud Platform
type GoogleProjectID string

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
