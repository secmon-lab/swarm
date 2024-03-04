package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type Client struct {
	topic *pubsub.Topic
}

func New(ctx context.Context, projectID types.GoogleProjectID, topicID types.PubSubTopicID) (*Client, error) {
	client, err := pubsub.NewClient(ctx, projectID.String())
	if err != nil {
		return nil, err
	}

	topic := client.Topic(topicID.String())
	return &Client{topic: topic}, nil
}

func (x *Client) Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
	msgID, err := x.topic.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return types.PubSubMessageID(msgID), err
}
