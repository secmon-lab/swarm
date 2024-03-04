package config

import (
	"context"

	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/pubsub"
	"github.com/urfave/cli/v2"
)

type PubSub struct {
	projectID types.GoogleProjectID
	topicID   types.PubSubTopicID
}

func (x *PubSub) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "pubsub-project-id",
			Usage:       "Google Cloud Project ID for Pub/Sub",
			EnvVars:     []string{"SWARM_PUBSUB_PROJECT_ID"},
			Destination: (*string)(&x.projectID),
		},
		&cli.StringFlag{
			Name:        "pubsub-topic-id",
			Usage:       "Pub/Sub topic ID",
			EnvVars:     []string{"SWARM_PUBSUB_TOPIC_ID"},
			Destination: (*string)(&x.topicID),
		},
	}
}

func (x *PubSub) Configure(ctx context.Context) (*pubsub.Client, error) {
	client, err := pubsub.New(ctx, x.projectID, x.topicID)
	if err != nil {
		return nil, err
	}

	return client, nil
}
