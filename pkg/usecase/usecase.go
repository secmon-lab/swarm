package usecase

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/infra"
)

type UseCase struct {
	clients  *infra.Clients
	metadata *model.MetadataConfig

	readObjectConcurrency int
	ingestConcurrency     int
	enqueueCountLimit     int
	enqueueSizeLimit      int
	stateTimeout          time.Duration
}

const (
	defaultReadObjectConcurrency = 32
	defaultEnqueueCountLimit     = 128
	defaultEnqueueSizeLimit      = 4 // MiB
	defaultIngestConcurrency     = 64
)

func New(clients *infra.Clients, options ...Option) *UseCase {
	uc := &UseCase{
		clients:               clients,
		readObjectConcurrency: defaultReadObjectConcurrency,
		ingestConcurrency:     defaultIngestConcurrency,
		enqueueCountLimit:     defaultEnqueueCountLimit,
		enqueueSizeLimit:      defaultEnqueueSizeLimit,
	}

	for _, option := range options {
		option(uc)
	}

	return uc
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
