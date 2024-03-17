package usecase

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func (x *UseCase) GetOrCreateState(ctx context.Context, msgType types.MsgType, id string) (*model.State, bool, error) {
	now := utils.CtxTime(ctx)
	reqID, _ := utils.CtxRequestID(ctx)

	state := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: reqID,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(x.stateTimeout),
		TTL:       now.Add(x.stateTTL),
	}

	// If database is not available, return acquired state always
	db := x.clients.Database()
	if db == nil {
		return state, true, nil
	}

	return db.GetOrCreateState(ctx, msgType, state)
}

func (x *UseCase) UpdateState(ctx context.Context, msgType types.MsgType, id string, state types.MsgState) error {
	// If database is not available, return nil
	if x.clients.Database() == nil {
		return nil
	}

	now := utils.CtxTime(ctx)
	return x.clients.Database().UpdateState(ctx, msgType, id, state, now)
}
