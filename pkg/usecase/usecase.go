package usecase

import (
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/infra"
)

type UseCase struct {
	clients  *infra.Clients
	metadata *model.MetadataConfig

	readObjectConcurrency int
}

const (
	defaultReadObjectConcurrency = 32
)

func New(clients *infra.Clients, options ...Option) *UseCase {
	uc := &UseCase{
		clients:               clients,
		readObjectConcurrency: defaultReadObjectConcurrency,
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
