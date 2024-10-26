package usecase

import (
	"context"

	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

func (x *UseCase) ObjectToSources(ctx context.Context, obj model.Object) ([]*model.Source, error) {
	var event model.EventPolicyOutput
	if err := x.clients.Policy().Query(ctx, "data.event", obj, &event); err != nil {
		return nil, err
	}
	if len(event.Sources) == 0 {
		return nil, goerr.Wrap(types.ErrNoPolicyResult, "no source in event").With("input", obj)
	}

	return event.Sources, nil
}
