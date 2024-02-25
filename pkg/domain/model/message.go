package model

import "github.com/m-mizutani/swarm/pkg/domain/types"

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
