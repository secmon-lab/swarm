package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type TopicClient struct {
	client    *pubsub.Client
	publisher *pubsub.Publisher
}

func NewTopic(ctx context.Context, projectID types.GoogleProjectID, topicID types.PubSubTopicID) (*TopicClient, error) {
	client, err := pubsub.NewClient(ctx, projectID.String())
	if err != nil {
		return nil, err
	}

	publisher := client.Publisher(topicID.String())
	return &TopicClient{
		client:    client,
		publisher: publisher,
	}, nil
}

func (x *TopicClient) Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
	msgID, err := x.publisher.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return types.PubSubMessageID(msgID), err
}

func (x *TopicClient) Close() error {
	x.publisher.Stop()
	return x.client.Close()
}

// messageWrapper wraps *pubsub.Message to implement interfaces.PubSubMessage.
type messageWrapper struct {
	msg *pubsub.Message
}

func (m *messageWrapper) Data() []byte { return m.msg.Data }
func (m *messageWrapper) ID() string   { return m.msg.ID }
func (m *messageWrapper) Ack()         { m.msg.Ack() }
func (m *messageWrapper) Nack()        { m.msg.Nack() }

type SubscriptionClient struct {
	client *pubsub.Client
}

func NewSubscriptionClient(ctx context.Context, projectID types.GoogleProjectID) (*SubscriptionClient, error) {
	client, err := pubsub.NewClient(ctx, projectID.String())
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create pubsub client")
	}

	return &SubscriptionClient{
		client: client,
	}, nil
}

func (x *SubscriptionClient) Receive(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
	sub := x.client.Subscriber(subName)
	if err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		f(ctx, &messageWrapper{msg: msg})
	}); err != nil {
		return goerr.Wrap(err, "failed to receive messages", goerr.V("subName", subName))
	}
	return nil
}

func (x *SubscriptionClient) Close() error {
	return x.client.Close()
}
