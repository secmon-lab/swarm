package usecase

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
)

type Mock struct {
	MockLoadData       func(ctx context.Context, req []*model.LoadRequest) error
	MockAuthorize      func(ctx context.Context, input *model.AuthPolicyInput) error
	MockEventToSources func(ctx context.Context, input any) ([]*model.Source, error)
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

func (x Mock) EventToSources(ctx context.Context, input any) ([]*model.Source, error) {
	if x.MockEventToSources != nil {
		return x.MockEventToSources(ctx, input)
	}
	return nil, nil
}
