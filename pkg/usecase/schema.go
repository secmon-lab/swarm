package usecase

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

func (x *UseCase) ApplyInferredSchema(ctx context.Context, urls []types.CSUrl) error {
	var requests []*model.LoadRequest

	for _, url := range urls {
		bucket, objID, err := url.Parse()
		if err != nil {
			return err
		}

		csObj := model.CloudStorageObject{
			Bucket: bucket,
			Name:   objID,
		}

		attr, err := x.clients.CloudStorage().Attrs(ctx, csObj)
		if err != nil {
			return err
		}

		obj := model.NewObjectFromCloudStorageAttrs(attr)
		sources, err := x.ObjectToSources(ctx, obj)
		if err != nil {
			return err
		}

		for _, src := range sources {
			requests = append(requests, &model.LoadRequest{
				Object: obj,
				Source: *src,
			})
		}
	}

	return nil
}
