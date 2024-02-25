package usecase

import (
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/infra"
)

type UseCase struct {
	clients  *infra.Clients
	metadata *model.MetadataConfig
}

func New(clients *infra.Clients, options ...Option) *UseCase {
	uc := &UseCase{
		clients: clients,
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
