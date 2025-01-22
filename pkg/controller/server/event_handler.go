package server

import (
	"context"
	"encoding/json"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
)

func handleSwarmEvent(ctx context.Context, uc interfaces.UseCase, data []byte) error {
	var event model.SwarmMessage
	if err := json.Unmarshal(data, &event); err != nil {
		return goerr.Wrap(err, "failed to unmarshal data", goerr.V("data", string(data)))
	}

	var loadReq []*model.LoadRequest
	for _, obj := range event.Objects {
		sources, err := uc.ObjectToSources(ctx, *obj)
		if err != nil {
			return goerr.Wrap(err, "failed to convert object to sources", goerr.V("object", obj))
		}

		for _, src := range sources {
			loadReq = append(loadReq, &model.LoadRequest{
				Object: *obj,
				Source: *src,
			})
		}
	}

	if err := uc.Load(ctx, loadReq); err != nil {
		return goerr.Wrap(err, "failed to handle swarm event", goerr.V("event", event))
	}

	return nil
}

func handleCloudStorageEvent(ctx context.Context, uc interfaces.UseCase, data []byte) error {
	var event model.CloudStorageEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return goerr.Wrap(err, "failed to unmarshal data", goerr.V("data", string(data)))
	}

	obj := event.ToObject()
	sources, err := uc.ObjectToSources(ctx, obj)
	if err != nil {
		return goerr.Wrap(err, "failed to convert event to sources", goerr.V("event", event))
	}

	loadReq := make([]*model.LoadRequest, len(sources))
	for i := range sources {
		loadReq[i] = &model.LoadRequest{
			Object: event.ToObject(),
			Source: *sources[i],
		}
	}

	if err := uc.Load(ctx, loadReq); err != nil {
		return goerr.Wrap(err, "failed to load", goerr.V("event", event))
	}

	return nil
}
