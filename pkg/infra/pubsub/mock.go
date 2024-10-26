package pubsub

import (
	"context"

	"github.com/google/uuid"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type Mock struct {
	MockPublish func(ctx context.Context, data []byte) (types.PubSubMessageID, error)
	Results     []*MockResult
}

type MockResult struct {
	ID   types.PubSubMessageID
	Data []byte
}

func NewMock() *Mock {
	mock := &Mock{}
	mock.MockPublish = func(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
		mock.Results = append(mock.Results, &MockResult{
			ID:   types.PubSubMessageID(uuid.NewString()),
			Data: data,
		})
		return mock.Results[len(mock.Results)-1].ID, nil
	}
	return mock
}

func (x *Mock) Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
	return x.MockPublish(ctx, data)
}
