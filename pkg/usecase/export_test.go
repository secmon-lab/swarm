package usecase

import "time"

var (
	CloneWithoutNil     = cloneWithoutNil
	CreateOrUpdateTable = createOrUpdateTable
	IngestRecords       = ingestRecords
	NewTableSchemaCache = newTableSchemaCache
)

type TableSchemaCache = tableSchemaCache

func (x *UseCase) SetAckDeadlineExtendInterval(d time.Duration) {
	x.ackDeadlineExtendInterval = d
}
