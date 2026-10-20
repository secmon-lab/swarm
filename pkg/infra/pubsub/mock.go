package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
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

type SubscriptionMock struct {
	MockPull              func(ctx context.Context, subName string, maxMessages int) ([]*pubsubpb.ReceivedMessage, error)
	MockModifyAckDeadline func(ctx context.Context, subName string, ackID string, deadline time.Duration) error
	MockAcknowledge       func(ctx context.Context, subName string, ackID string) error
}

var _ interfaces.PubSubSubscription = &SubscriptionMock{}

func (x *SubscriptionMock) Pull(ctx context.Context, subName string, maxMessages int) ([]*pubsubpb.ReceivedMessage, error) {
	if x.MockPull != nil {
		return x.MockPull(ctx, subName, maxMessages)
	}
	return nil, nil
}

func (x *SubscriptionMock) ModifyAckDeadline(ctx context.Context, subName string, ackID string, deadline time.Duration) error {
	if x.MockModifyAckDeadline != nil {
		return x.MockModifyAckDeadline(ctx, subName, ackID, deadline)
	}
	return nil
}

func (x *SubscriptionMock) Acknowledge(ctx context.Context, subName string, ackID string) error {
	if x.MockAcknowledge != nil {
		return x.MockAcknowledge(ctx, subName, ackID)
	}
	return nil
}

func (x *SubscriptionMock) Close() error {
	return nil
}
