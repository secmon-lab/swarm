package usecase

import (
	"context"
	"time"

	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type Mock struct {
	MockLoadData         func(ctx context.Context, req []*model.LoadRequest) error
	MockAuthorize        func(ctx context.Context, input *model.AuthPolicyInput) error
	MockObjectToSources  func(ctx context.Context, obj model.Object) ([]*model.Source, error)
	MockEnqueue          func(ctx context.Context, req *model.EnqueueRequest) (*model.EnqueueResponse, error)
	MockGetOrCreateState func(ctx context.Context, msgType types.MsgType, id string) (*model.State, bool, error)
	MockUpdateState      func(ctx context.Context, msgType types.MsgType, id string, state types.MsgState) error
	MockWaitState        func(ctx context.Context, msgType types.MsgType, id string, expiresAt time.Time) error
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

func (x Mock) GetOrCreateState(ctx context.Context, msgType types.MsgType, id string) (*model.State, bool, error) {
	if x.MockGetOrCreateState == nil {
		return &model.State{}, true, nil
	}
	return x.MockGetOrCreateState(ctx, msgType, id)
}

func (x Mock) UpdateState(ctx context.Context, msgType types.MsgType, id string, state types.MsgState) error {
	if x.MockUpdateState == nil {
		return nil
	}
	return x.MockUpdateState(ctx, msgType, id, state)
}

func (x Mock) WaitState(ctx context.Context, msgType types.MsgType, id string, expiresAt time.Time) error {
	if x.MockWaitState == nil {
		return nil
	}
	return x.MockWaitState(ctx, msgType, id, expiresAt)
}
