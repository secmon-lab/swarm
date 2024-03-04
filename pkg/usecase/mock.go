package usecase

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
)

type Mock struct {
	MockLoadData        func(ctx context.Context, req []*model.LoadRequest) error
	MockAuthorize       func(ctx context.Context, input *model.AuthPolicyInput) error
	MockObjectToSources func(ctx context.Context, obj model.Object) ([]*model.Source, error)
	MockEnqueue         func(ctx context.Context, req *model.EnqueueRequest) (*model.EnqueueResponse, error)
}

func (x *Mock) Load(ctx context.Context, req []*model.LoadRequest) error {
	if x.MockLoadData != nil {
		return x.MockLoadData(ctx, req)
	}
	return nil
}

func (x Mock) Authorize(ctx context.Context, input *model.AuthPolicyInput) error {
	if x.MockAuthorize != nil {
		return x.MockAuthorize(ctx, input)
	}
	return nil
}

func (x Mock) ObjectToSources(ctx context.Context, obj model.Object) ([]*model.Source, error) {
	if x.MockObjectToSources != nil {
		return x.MockObjectToSources(ctx, obj)
	}
	return nil, nil
}

func (x Mock) Enqueue(ctx context.Context, req *model.EnqueueRequest) (*model.EnqueueResponse, error) {
	return x.MockEnqueue(ctx, req)
}
