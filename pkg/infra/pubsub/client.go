package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	apiv1 "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type TopicClient struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

func NewTopic(ctx context.Context, projectID types.GoogleProjectID, topicID types.PubSubTopicID) (*TopicClient, error) {
	client, err := pubsub.NewClient(ctx, projectID.String())
	if err != nil {
		return nil, err
	}

	topic := client.Topic(topicID.String())
	return &TopicClient{
		client: client,
		topic:  topic,
	}, nil
}

func (x *TopicClient) Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
	msgID, err := x.topic.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return types.PubSubMessageID(msgID), err
}

func (x *TopicClient) Close() {
	x.topic.Stop()
	x.client.Close()
}

type SubscriptionClient struct {
	client *apiv1.SubscriberClient
}

func NewSubscriptionClient(ctx context.Context) (*SubscriptionClient, error) {
	client, err := apiv1.NewSubscriberClient(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create subscriber client")
	}

	return &SubscriptionClient{
		client: client,
	}, nil
}

func (x *SubscriptionClient) Pull(ctx context.Context, subName string) ([]*pubsubpb.ReceivedMessage, error) {
	req := pubsubpb.PullRequest{
		Subscription:      subName,
		MaxMessages:       1,
		ReturnImmediately: true,
	}

	res, err := x.client.Pull(ctx, &req)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to pull message", goerr.V("subName", subName))
	}

	return res.ReceivedMessages, nil
}

func (x *SubscriptionClient) Acknowledge(ctx context.Context, subName string, ackID string) error {
	req := pubsubpb.AcknowledgeRequest{
		Subscription: subName,
		AckIds:       []string{ackID},
	}

	if err := x.client.Acknowledge(ctx, &req); err != nil {
		return goerr.Wrap(err, "failed to acknowledge message", goerr.V("subName", subName), goerr.V("ackID", ackID))
	}
	return nil
}

func (x *SubscriptionClient) ModifyAckDeadline(ctx context.Context, subName string, ackID string, deadline time.Duration) error {
	req := pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subName,
		AckIds:             []string{ackID},
		AckDeadlineSeconds: int32(deadline.Seconds()),
	}

	if err := x.client.ModifyAckDeadline(ctx, &req); err != nil {
		return goerr.Wrap(err, "failed to modify ack deadline",
			goerr.V("subName", subName),
			goerr.V("ackID", ackID),
			goerr.V("deadline", deadline),
		)
	}
	return nil
}

func (x *SubscriptionClient) Close() error {
	return x.client.Close()
}
