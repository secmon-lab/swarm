package usecase

import (
	"context"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
)

// ObjectToSources returns the sources of the object defined by the event rule. It returns no source and no error if the event rule ignores the object.
func (x *UseCase) ObjectToSources(ctx context.Context, obj model.Object) ([]*model.Source, error) {
	var event model.EventPolicyOutput
	if err := x.clients.Policy().Query(ctx, "data.event", obj, &event); err != nil {
		return nil, err
	}
	if event.Ignore {
		utils.CtxLogger(ctx).Info("object is ignored by event rule", "object", obj)
		return nil, nil
	}
	if len(event.Sources) == 0 {
		return nil, goerr.Wrap(types.ErrNoPolicyResult, "no source in event", goerr.V("input", obj))
	}

	return event.Sources, nil
}
