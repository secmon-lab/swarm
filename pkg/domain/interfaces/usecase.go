package interfaces

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/model"
)

type UseCase interface {
	LoadData(ctx context.Context, req *model.LoadDataRequest) error
	Authorize(ctx context.Context, input *model.AuthPolicyInput) error
}
