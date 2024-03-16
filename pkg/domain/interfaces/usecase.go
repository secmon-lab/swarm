package interfaces

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type UseCase interface {
	ObjectToSources(ctx context.Context, obj model.Object) ([]*model.Source, error)
	Load(ctx context.Context, requests []*model.LoadRequest) error
	Enqueue(ctx context.Context, req *model.EnqueueRequest) (*model.EnqueueResponse, error)
	Authorize(ctx context.Context, input *model.AuthPolicyInput) error

	GetOrCreateState(ctx context.Context, msgType types.MsgType, id string) (*model.State, bool, error)
	UpdateState(ctx context.Context, msgType types.MsgType, id string, state types.MsgState) error
}
