package usecase

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
)

type Mock struct {
	MockLoadData  func(ctx context.Context, req *model.LoadDataRequest) error
	MockAuthorize func(ctx context.Context, input *model.AuthPolicyInput) error
}

func (x *Mock) LoadData(ctx context.Context, req *model.LoadDataRequest) error {
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

/*
func (x *Mock) Authorize(ctx context.Context, token []byte) (*model.AuthContext, error) {
	if x.MockAuthorize != nil {
		return x.MockAuthorize(ctx, token)
	}
	return &model.AuthContext{}, nil
}
*/
