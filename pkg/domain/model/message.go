package model

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type EventarcDirectEvent struct {
	Bucket                  types.CSBucket   `json:"bucket"`
	ContentType             string           `json:"contentType"`
	Crc32c                  string           `json:"crc32c"`
	Etag                    string           `json:"etag"`
	Generation              string           `json:"generation"`
	ID                      string           `json:"id"`
	Kind                    string           `json:"kind"`
	Md5Hash                 string           `json:"md5Hash"`
	MediaLink               string           `json:"mediaLink"`
	Metageneration          string           `json:"metageneration"`
	Name                    types.CSObjectID `json:"name"`
	SelfLink                string           `json:"selfLink"`
	Size                    string           `json:"size"`
	StorageClass            string           `json:"storageClass"`
	TimeCreated             string           `json:"timeCreated"`
	TimeStorageClassUpdated string           `json:"timeStorageClassUpdated"`
	Updated                 string           `json:"updated"`
}

type PubSubBody struct {
	Message      PubSubMessage `json:"message"`
	Subscription string        `json:"subscription"`
}

type PubSubMessage struct {
	Attributes  map[string]string `json:"attributes"`
	Data        string            `json:"data"`
	MessageID   string            `json:"message_id"`
	PublishTime string            `json:"publish_time"`
}

type CloudStorageEvent struct {
	Bucket                  types.CSBucket   `json:"bucket"`
	ContentType             string           `json:"contentType"`
	Crc32c                  string           `json:"crc32c"`
	Etag                    string           `json:"etag"`
	Generation              string           `json:"generation"`
	ID                      string           `json:"id"`
	Kind                    string           `json:"kind"`
	Md5Hash                 string           `json:"md5Hash"`
	MediaLink               string           `json:"mediaLink"`
	Metageneration          string           `json:"metageneration"`
	Name                    types.CSObjectID `json:"name"`
	SelfLink                string           `json:"selfLink"`
	Size                    string           `json:"size"`
	StorageClass            string           `json:"storageClass"`
	TimeCreated             string           `json:"timeCreated"`
	TimeStorageClassUpdated string           `json:"timeStorageClassUpdated"`
	Updated                 string           `json:"updated"`
}

func (x CloudStorageEvent) ToObject() Object {
	var size *int64
	{
		raw, err := strconv.ParseInt(x.Size, 10, 64)
		if err == nil {
			size = &raw
		}
	}

	var createdAt *int64
	{
		t, err := time.Parse("2006-01-02T15:04:05.999Z", x.TimeCreated)
		if err == nil {
			raw := t.Unix()
			createdAt = &raw
		}
	}

	var digests []Digest
	{
		v, err := base64.StdEncoding.DecodeString(x.Md5Hash)
		if err == nil {
			digests = append(digests, Digest{
				Alg:   "md5",
				Value: hex.EncodeToString(v),
			})
		}
	}

	return Object{
		CS: &CloudStorageObject{
			Bucket: x.Bucket,
			Name:   x.Name,
		},
		Size:      size,
		CreatedAt: createdAt,
		Digests:   digests,

		Data: x,
	}
}

// SwarmMessage is a struct for the event from swarm. It's abstracted event structure for multiple event sources.
type SwarmMessage struct {
	Objects []*Object `json:"objects"`
}
