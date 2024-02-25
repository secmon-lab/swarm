package usecase

import (
	"context"

	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

func (x *UseCase) EventToSources(ctx context.Context, input any) ([]*model.Source, error) {
	var event model.EventPolicyOutput
	if err := x.clients.Policy().Query(ctx, "data.event", input, &event); err != nil {
		return nil, err
	}
	if len(event.Sources) == 0 {
		return nil, goerr.Wrap(types.ErrNoPolicyResult, "no source in event").With("input", input)
	}

	return event.Sources, nil
}
