package model

import (
	"encoding/hex"
	"time"

	"cloud.google.com/go/storage"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type LoadDataRequest struct {
	CSEvent *CloudStorageEvent
}

type LoadRequest struct {
	Source Source
	Object Object
}

type EnqueueRequest struct {
	URLs []types.ObjectURL
}

type EnqueueResponse struct {
	Elapsed time.Duration
	Count   int64
	Size    int64
}

type Object struct {
	CS        *CloudStorageObject `json:"cs,omitempty" bigquery:"cs"`
	Size      *int64              `json:"size,omitempty" bigquery:"size"`
	CreatedAt *int64              `json:"created_at" bigquery:"created_at"`
	Digests   []Digest            `json:"digests" bigquery:"digests"`

	// Data is original notification data, such as CloudStorageEvent
	Data any `json:"data" bigquery:"-"`
}

type CloudStorageObject struct {
	Bucket types.CSBucket   `json:"bucket" bigquery:"bucket"`
	Name   types.CSObjectID `json:"name" bigquery:"name"`
}

type Digest struct {
	Alg   string `json:"alg" bigquery:"alg"`
	Value string `json:"value" bigquery:"value"`
}

func NewObjectFromCloudStorageAttrs(attrs *storage.ObjectAttrs) Object {
	return Object{
		CS: &CloudStorageObject{
			Bucket: types.CSBucket(attrs.Bucket),
			Name:   types.CSObjectID(attrs.Name),
		},
		Size:      &attrs.Size,
		CreatedAt: toPtr(attrs.Created.Unix()),
		Digests: []Digest{
			{
				Alg:   "md5",
				Value: hex.EncodeToString(attrs.MD5),
			},
		},
	}
}

func toPtr[T any](v T) *T {
	return &v
}
