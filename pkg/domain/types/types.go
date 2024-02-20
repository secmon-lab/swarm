package types

import (
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
type StreamID string
type LogID string

func NewStreamID() StreamID { return StreamID(uuid.NewString()) }
func NewLogID() LogID       { return LogID(uuid.NewString()) }

// Google Cloud Platform
type GoogleProjectID string

type BQDatasetID string
type BQTableID string

func (x BQDatasetID) String() string { return string(x) }
func (x BQTableID) String() string   { return string(x) }

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
type ObjectFormat string

const (
	JSONFormat ObjectFormat = "json"
)

type ObjectComp string

const (
	GZIPComp ObjectComp = "gzip"
)

type ObjectSchema string

func (x ObjectSchema) Query() string { return "data.schema." + string(x) }
