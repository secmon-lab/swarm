package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/utils"
	"golang.org/x/sync/errgroup"
)

func (x *UseCase) RunWithSubscriptions(ctx context.Context, subscriptions []string) error {
	utils.CtxLogger(ctx).Info("starting job", "subscriptions", subscriptions)

	eg, ctx := errgroup.WithContext(ctx)
	for _, subName := range subscriptions {
		eg.Go(func() error {
			return x.runWithSubscription(ctx, subName)
		})
	}

	return eg.Wait()
}

func (x *UseCase) runWithSubscription(ctx context.Context, subName string) error {
	utils.CtxLogger(ctx).Info("starting job", "subscription", subName)

	cctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	idleTimer := time.AfterFunc(x.idleTimeout, func() {
		utils.CtxLogger(ctx).Info("idle timeout reached, stopping", "subscription", subName)
		cancel(nil)
	})
	defer idleTimer.Stop()

	err := x.clients.PubSubSubscription().Receive(cctx, subName, func(ctx context.Context, msg interfaces.PubSubMessage) {
		idleTimer.Reset(x.idleTimeout)

		if err := x.processPubSubMessage(ctx, msg); err != nil {
			utils.CtxLogger(ctx).Error("failed to process message", "error", err)
			msg.Nack()
			cancel(err)
			return
		}
		msg.Ack()
	})

	if err != nil {
		return err
	}

	// cancel(nil) sets cause to context.Canceled, so filter that out.
	// Only return cause if it's a real processing error from the callback.
	if cause := context.Cause(cctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}

	return nil
}

func (x *UseCase) processPubSubMessage(ctx context.Context, msg interfaces.PubSubMessage) error {
	logger := utils.CtxLogger(ctx)
	logger.Info("processing message", "messageID", msg.ID())

	var event model.CloudStorageEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
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
