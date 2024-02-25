package model

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type RawRecord struct {
	data any
	hash string
}

type CSObject struct {
	bucket types.CSBucket
	object types.CSObjectID
}

func NewCSObject(bucket types.CSBucket, object types.CSObjectID) *CSObject {
	return &CSObject{
		bucket: bucket,
		object: object,
	}
}

func NewRawRecord(obj *CSObject, idx int, data any) *RawRecord {
	h := md5.New()
	h.Write([]byte(obj.bucket))
	h.Write([]byte{0x00})
	h.Write([]byte(obj.object))
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(idx))
	h.Write([]byte(bytes))

	return &RawRecord{
		data: data,
		hash: hex.EncodeToString(h.Sum(nil)),
	}
}
