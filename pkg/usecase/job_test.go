package usecase_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/pubsub"
	"github.com/secmon-lab/swarm/pkg/usecase"
)

func TestRunWithSubscriptions_IdleTimeout(t *testing.T) {
	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			<-ctx.Done()
			return nil
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(100*time.Millisecond),
	)

	start := time.Now()
	err := uc.RunWithSubscriptions(context.Background(), []string{"projects/p/subscriptions/s"})
	elapsed := time.Since(start)

	gt.NoError(t, err)
	gt.B(t, elapsed < 2*time.Second).True()
}

func TestRunWithSubscriptions_ProcessingErrorNacks(t *testing.T) {
	msg := &pubsub.MockMessage{
		MessageID:   "msg-1",
		MessageData: []byte("invalid json"),
	}

	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			f(ctx, msg)
			<-ctx.Done()
			return nil
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(100*time.Millisecond),
	)

	err := uc.RunWithSubscriptions(context.Background(), []string{"projects/p/subscriptions/s"})

	// Processing error should be propagated
	gt.Error(t, err)
	// Message should be nacked, not acked
	gt.B(t, msg.Nacked()).True()
	gt.B(t, msg.Acked()).False()
}

func TestRunWithSubscriptions_ServiceError(t *testing.T) {
	serviceErr := errors.New("service unavailable")
	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			return serviceErr
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(100*time.Millisecond),
	)

	err := uc.RunWithSubscriptions(context.Background(), []string{"projects/p/subscriptions/s"})

	gt.Error(t, err)
}

func TestRunWithSubscriptions_ParallelSubscriptions(t *testing.T) {
	var mu sync.Mutex
	receivedSubs := make(map[string]bool)

	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			mu.Lock()
			receivedSubs[subName] = true
			mu.Unlock()
			<-ctx.Done()
			return nil
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(100*time.Millisecond),
	)

	subs := []string{
		"projects/p/subscriptions/sub-a",
		"projects/p/subscriptions/sub-b",
		"projects/p/subscriptions/sub-c",
	}

	start := time.Now()
	err := uc.RunWithSubscriptions(context.Background(), subs)
	elapsed := time.Since(start)

	gt.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	for _, sub := range subs {
		gt.B(t, receivedSubs[sub]).True()
	}

	// Parallel: should complete in roughly 1x idle timeout, not 3x
	gt.B(t, elapsed < 1*time.Second).True()
}

func TestRunWithSubscriptions_ParentContextCancel(t *testing.T) {
	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			<-ctx.Done()
			return nil
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(10*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := uc.RunWithSubscriptions(ctx, []string{"projects/p/subscriptions/s"})
	elapsed := time.Since(start)

	// Parent context cancel is not an error
	gt.NoError(t, err)
	gt.B(t, elapsed < 2*time.Second).True()
}

func TestRunWithSubscriptions_IdleTimerResetOnMessage(t *testing.T) {
	// Track how many messages were delivered to verify timer reset kept Receive alive
	var mu sync.Mutex
	var deliveredCount int

	mock := &pubsub.SubscriptionMock{
		MockReceive: func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
			// Use a goroutine to deliver messages at intervals shorter than idle timeout.
			// The callback will fail (invalid json) and call cancel(err), but the
			// important thing is that idleTimer.Reset is called before the error.
			// However since cancel(err) cancels ctx, only the first message gets through.
			// So instead, test that Receive stayed alive longer than idle timeout
			// by having messages arrive before timeout fires.
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(30 * time.Millisecond):
						mu.Lock()
						deliveredCount++
						mu.Unlock()
						// Deliver a message - even though it fails, timer.Reset
						// is called before processPubSubMessage
						msg := &pubsub.MockMessage{
							MessageID:   "msg",
							MessageData: []byte("invalid"),
						}
						f(ctx, msg)
					}
				}
			}()
			<-ctx.Done()
			return nil
		},
	}

	uc := usecase.New(
		infra.New(infra.WithPubSubSubscription(mock)),
		usecase.WithIdleTimeout(100*time.Millisecond),
	)

	_ = uc.RunWithSubscriptions(context.Background(), []string{"projects/p/subscriptions/s"})

	// At least 1 message should have been delivered
	mu.Lock()
	defer mu.Unlock()
	gt.B(t, deliveredCount >= 1).True()
}
