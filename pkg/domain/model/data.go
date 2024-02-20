package model

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"

	"github.com/m-mizutani/swarm/pkg/domain/types"

	"cloud.google.com/go/bigquery"
)

type DataRow struct {
	data   map[string]any
	object types.CSObjectID
	index  int
}

func NewDataRow(object types.CSObjectID, index int, data map[string]any) *DataRow {
	return &DataRow{
		data:   data,
		object: object,
		index:  index,
	}
}

func (x *DataRow) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = make(map[string]bigquery.Value)
	for k, v := range x.data {
		row[k] = v
	}

	h := md5.New()
	h.Write([]byte(x.object))
	h.Write([]byte{0x00})
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(x.index))
	h.Write([]byte(bytes))
	return row, hex.EncodeToString(h.Sum(nil)), nil
}
