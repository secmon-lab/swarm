package usecase

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/utils"
)

func (x *UseCase) RunWithSubscriptions(ctx context.Context, subscriptions []string) error {
	utils.CtxLogger(ctx).Info("starting job", "subscriptions", subscriptions)

	for _, subName := range subscriptions {
		if err := x.runWithSubscription(ctx, subName); err != nil {
			return err
		}
	}

	return nil
}

func (x *UseCase) runWithSubscription(ctx context.Context, subName string) error {
	utils.CtxLogger(ctx).Info("starting job", "subscription", subName)

	pullClient := x.clients.PubSubSubscription()
	for {
		resp, err := pullClient.Pull(ctx, subName)
		if err != nil {
			return err
		}
		if len(resp) == 0 {
			utils.CtxLogger(ctx).Info("no message in subscription", "subscription", subName)
			return nil
		}

		for _, msg := range resp {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			go func() {
				if err := loopExtendPubSubMessageDeadline(ctx, pullClient, subName, msg.AckId); err != nil {
					utils.CtxLogger(ctx).Error("failed to extend deadline", "error", err)
				}
			}()

			if err := x.processPubSubMessage(ctx, msg); err != nil {
				return err
			}

			if err := pullClient.Acknowledge(ctx, subName, msg.AckId); err != nil {
				return err
			}
		}
	}
}

func loopExtendPubSubMessageDeadline(ctx context.Context, client interfaces.PubSubSubscription, subName string, ackID string) error {
	tickInterval := 60 * time.Second
	extendDuration := 90 * time.Second

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()

		case <-tick.C:
			utils.CtxLogger(ctx).Info("extend deadline", "subscription", subName, "ackID", ackID)
			if err := client.ModifyAckDeadline(ctx, subName, ackID, extendDuration); err != nil {
				return err
			}
		}
	}
}

func (x *UseCase) processPubSubMessage(ctx context.Context, msg *pubsubpb.ReceivedMessage) error {
	logger := utils.CtxLogger(ctx)
	logger.Info("processing message", "message", msg)

	// Decode message
	var event model.CloudStorageEvent
	if err := json.Unmarshal(msg.Message.Data, &event); err != nil {
		return err
	}
	logger.Info("decoded message", "event", event)

	obj := event.ToObject()
	sources, err := x.ObjectToSources(ctx, obj)
	if err != nil {
		return goerr.Wrap(err, "failed to convert event to sources", goerr.V("event", event))
	}

	loadReq := make([]*model.LoadRequest, len(sources))
	for i := range sources {
		loadReq[i] = &model.LoadRequest{
			Object: event.ToObject(),
			Source: *sources[i],
		}
	}

	if err := x.Load(ctx, loadReq); err != nil {
		return goerr.Wrap(err, "failed to load", goerr.V("event", event))
	}

	return nil
}
