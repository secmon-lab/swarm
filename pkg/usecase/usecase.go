package usecase

import (
	"sync"
	"time"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/infra"
)

type UseCase struct {
	clients  *infra.Clients
	metadata *model.MetadataConfig

	readObjectConcurrency   int
	ingestTableConcurrency  int
	ingestRecordConcurrency int
	enqueueCountLimit       int
	enqueueSizeLimit        int

	// stateTimeout is a duration to wait for state transition. Even if the state is not changed, other process can acquire the state after this duration.
	stateTimeout time.Duration

	// stateTTL is a duration to keep the state. After this duration, the state is deleted from database. This is used to avoid re-process of the same message.
	stateTTL time.Duration

	// stateCheckInterval is a duration to check state transition. This is used in WaitState method.
	stateCheckInterval time.Duration

	// stateWaitTimeout is a duration to wait for state transition. This is used in WaitState method.
	stateWaitTimeout time.Duration

	// messageConcurrency is the maximum number of Pub/Sub messages processed at the same time in RunWithSubscriptions.
	messageConcurrency int

	// ackDeadlineExtendInterval is the interval of extending the ack deadline of a Pub/Sub message in process.
	ackDeadlineExtendInterval time.Duration
	// ackDeadlineExtension is the ack deadline set by each extension.
	ackDeadlineExtension time.Duration

	tableSchemas *tableSchemaCache

	// loadLogStream is the stream to write load_logs. It is opened by the first Load call and closed by Close.
	loadLogStream      interfaces.BigQueryStream
	loadLogStreamMutex sync.Mutex
}

const (
	defaultReadObjectConcurrency     = 32
	defaultEnqueueCountLimit         = 128
	defaultEnqueueSizeLimit          = 4 // MiB
	defaultIngestTableConcurrency    = 8
	defaultIngestRecordConcurrency   = 8
	defaultStateTimeout              = 30 * time.Minute
	defaultStateTTL                  = 7 * 24 * time.Hour
	defaultStateCheckInterval        = 10 * time.Second
	defaultStateWaitTimeout          = 2 * time.Minute
	defaultMessageConcurrency        = 1
	defaultAckDeadlineExtendInterval = 60 * time.Second
	defaultAckDeadlineExtension      = 90 * time.Second
)

func New(clients *infra.Clients, options ...Option) *UseCase {
	uc := &UseCase{
		clients:                   clients,
		readObjectConcurrency:     defaultReadObjectConcurrency,
		ingestTableConcurrency:    defaultIngestTableConcurrency,
		ingestRecordConcurrency:   defaultIngestRecordConcurrency,
		enqueueCountLimit:         defaultEnqueueCountLimit,
		enqueueSizeLimit:          defaultEnqueueSizeLimit,
		stateTimeout:              defaultStateTimeout,
		stateTTL:                  defaultStateTTL,
		stateCheckInterval:        defaultStateCheckInterval,
		stateWaitTimeout:          defaultStateWaitTimeout,
		messageConcurrency:        defaultMessageConcurrency,
		ackDeadlineExtendInterval: defaultAckDeadlineExtendInterval,
		ackDeadlineExtension:      defaultAckDeadlineExtension,
		tableSchemas:              newTableSchemaCache(),
	}

	for _, option := range options {
		option(uc)
	}

	return uc
}

// Close releases resources held by UseCase. Currently it closes the stream to write load_logs.
func (x *UseCase) Close() error {
	x.loadLogStreamMutex.Lock()
	defer x.loadLogStreamMutex.Unlock()

	if x.loadLogStream == nil {
		return nil
	}

	s := x.loadLogStream
	x.loadLogStream = nil
	if err := s.Close(); err != nil {
		return goerr.Wrap(err, "failed to close load log stream")
	}
	return nil
}

type Option func(*UseCase)

func WithMetadata(metadata *model.MetadataConfig) Option {
	return func(uc *UseCase) {
		uc.metadata = metadata
	}
}

func WithReadObjectConcurrency(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(uc *UseCase) {
		uc.readObjectConcurrency = n
	}
}

func WithEnqueueCountLimit(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(uc *UseCase) {
		uc.enqueueCountLimit = n
	}
}

func WithEnqueueSizeLimit(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(uc *UseCase) {
		uc.enqueueSizeLimit = n
	}
}

func WithIngestTableConcurrency(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(uc *UseCase) {
		uc.ingestTableConcurrency = n
	}
}

func WithIngestRecordConcurrency(n int) Option {
	if n < 1 {
		n = 1
	}
	return func(uc *UseCase) {
		uc.ingestRecordConcurrency = n
	}
}

// WithMessageConcurrency sets the maximum number of Pub/Sub messages processed at the same time. Unlike other concurrency options, a value less than 1 is kept as is so that RunWithSubscriptions rejects it instead of silently changing it.
func WithMessageConcurrency(n int) Option {
	return func(uc *UseCase) {
		uc.messageConcurrency = n
	}
}

func WithStateTimeout(d time.Duration) Option {
	return func(uc *UseCase) {
		uc.stateTimeout = d
	}
}

func WithStateTTL(d time.Duration) Option {
	return func(uc *UseCase) {
		uc.stateTTL = d
	}
}

func WithStateCheckInterval(d time.Duration) Option {
	return func(uc *UseCase) {
		uc.stateCheckInterval = d
	}
}
