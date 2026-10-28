package usecase

import "time"

var (
	CloneWithoutNil     = cloneWithoutNil
	CreateOrUpdateTable = createOrUpdateTable
	IngestRecords       = ingestRecords
	NewTableSchemaCache = newTableSchemaCache
	NewTableStreamCache = newTableStreamCache
)

const MaxDefaultStreamAppendBytes = maxDefaultStreamAppendBytes

type TableSchemaCache = tableSchemaCache
type TableStreamCache = tableStreamCache

func (x *UseCase) SetAckDeadlineExtendInterval(d time.Duration) {
	x.ackDeadlineExtendInterval = d
}

func (x *UseCase) SetNow(now func() time.Time) {
	x.now = now
}
