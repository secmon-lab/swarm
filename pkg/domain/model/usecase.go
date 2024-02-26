package model

import (
	"encoding/hex"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type LoadDataRequest struct {
	CSEvent *CloudStorageEvent
}

type LoadRequest struct {
	Source Source
	Object Object
}

type Object struct {
	CS        *CloudStorageObject `json:"cs,omitempty"`
	Size      *int64              `json:"size,omitempty"`
	CreatedAt *int64              `json:"created_at"`
	Digests   []Digest            `json:"digests"`

	// Data is original notification data, such as CloudStorageEvent
	Data any `json:"data"`
}

type CloudStorageObject struct {
	Bucket types.CSBucket   `json:"bucket"`
	Name   types.CSObjectID `json:"name"`
}

type Digest struct {
	Alg   string `json:"alg"`
	Value string `json:"value"`
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
