package model

import (
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type CSObject struct {
	bucket types.CSBucket
	object types.CSObjectID
}

func (x *CSObject) Bucket() types.CSBucket   { return x.bucket }
func (x *CSObject) Object() types.CSObjectID { return x.object }

func NewCSObject(bucket types.CSBucket, object types.CSObjectID) CSObject {
	return CSObject{
		bucket: bucket,
		object: object,
	}
}
