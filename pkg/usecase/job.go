package usecase

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
)

func (x *UseCase) RunWithSubscriptions(ctx context.Context, subscriptions []string) error {
	if x.messageConcurrency < 1 {
		return goerr.Wrap(types.ErrInvalidOption, "message concurrency must be 1 or more", goerr.V("messageConcurrency", x.messageConcurrency))
	}
	if x.stopPullingAfter < 0 {
		return goerr.Wrap(types.ErrInvalidOption, "stop pulling after must be 0 or more", goerr.V("stopPullingAfter", x.stopPullingAfter))
	}

	utils.CtxLogger(ctx).Info("starting job", "subscriptions", subscriptions, "messageConcurrency", x.messageConcurrency, "stopPullingAfter", x.stopPullingAfter)

	// A zero time means that pulling never stops by time.
	var pullDeadline time.Time
	if x.stopPullingAfter > 0 {
		pullDeadline = x.now().Add(x.stopPullingAfter)
	}

	for _, subName := range subscriptions {
		if err := x.runWithSubscription(ctx, subName, pullDeadline); err != nil {
			return err
		}
		if x.pullDeadlinePassed(pullDeadline) {
			utils.CtxLogger(ctx).Info("stop pulling because the pull deadline has passed", "pullDeadline", pullDeadline, "lastSubscription", subName)
			return nil
		}
	}

	return nil
}

func (x *UseCase) pullDeadlinePassed(pullDeadline time.Time) bool {
	return !pullDeadline.IsZero() && !x.now().Before(pullDeadline)
}

// runWithSubscription processes messages of the subscription until the subscription has no message or pullDeadline passes. It processes at most x.messageConcurrency messages at the same time. After pullDeadline, it pulls no new message and waits for the messages in process, so that the job can finish before the task timeout of the platform kills it and the messages in process are redelivered. When processing a message fails, it stops pulling new messages, waits for the messages in process, and returns the first error.
func (x *UseCase) runWithSubscription(ctx context.Context, subName string, pullDeadline time.Time) error {
	utils.CtxLogger(ctx).Info("starting job", "subscription", subName)

	pullClient := x.clients.PubSubSubscription()
	results := make(chan error, x.messageConcurrency)
	inProcess := 0
	var firstErr error

	receiveResult := func(err error) {
		inProcess--
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// receiveAvailableResults takes results that are already available without waiting, so that a failure stops pulling as soon as possible.
	receiveAvailableResults := func() {
		for inProcess > 0 {
			select {
			case err := <-results:
				receiveResult(err)
			default:
				return
			}
		}
	}

	for {
		receiveAvailableResults()
		if firstErr != nil {
			break
		}

		if x.pullDeadlinePassed(pullDeadline) {
			break
		}

		if inProcess == x.messageConcurrency {
			receiveResult(<-results)
			continue
		}

		msgs, err := pullClient.Pull(ctx, subName, x.messageConcurrency-inProcess)

		// Messages already received are processed and acknowledged on success, even if another message failed during Pull. Leaving them unprocessed would delay their redelivery until their ack deadline expires.
		for _, msg := range msgs {
			inProcess++
			go func() {
				results <- x.handlePubSubMessage(ctx, pullClient, subName, msg)
			}()
		}

		if err != nil {
			// A processing failure that finished before Pull failed is the first error.
			receiveAvailableResults()
			if firstErr == nil {
				firstErr = err
			}
			break
		}

		if len(msgs) == 0 {
			if inProcess == 0 {
				utils.CtxLogger(ctx).Info("no message in subscription", "subscription", subName)
				return nil
			}
			// Wait for a message in process before pulling again, so that an empty subscription is not pulled repeatedly.
			receiveResult(<-results)
		}
	}

	for inProcess > 0 {
		receiveResult(<-results)
	}

	return firstErr
}

// handlePubSubMessage processes the message and acknowledges it if processing succeeds. The ack deadline of the message is extended while it is processed.
func (x *UseCase) handlePubSubMessage(ctx context.Context, client interfaces.PubSubSubscription, subName string, msg *pubsubpb.ReceivedMessage) error {
	msgCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	extenderDone := make(chan struct{})
	go func() {
		defer close(extenderDone)
		if err := loopExtendPubSubMessageDeadline(msgCtx, client, subName, msg.AckId, x.ackDeadlineExtendInterval, x.ackDeadlineExtension); err != nil {
			utils.CtxLogger(msgCtx).Error("failed to extend deadline", "error", err)
		}
	}()

	err := x.processPubSubMessage(msgCtx, msg)

	// Stop the extender and wait for it before Acknowledge, not after it, so that ModifyAckDeadline is never called for an acknowledged message. The last extension leaves enough time to acknowledge.
	cancel()
	<-extenderDone

	if err != nil {
		return err
	}

	if err := client.Acknowledge(ctx, subName, msg.AckId); err != nil {
		return err
	}
	return nil
}

func loopExtendPubSubMessageDeadline(ctx context.Context, client interfaces.PubSubSubscription, subName string, ackID string, tickInterval, extendDuration time.Duration) error {
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
	if len(sources) == 0 {
		return nil
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
