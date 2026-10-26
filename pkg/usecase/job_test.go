package usecase_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/m-mizutani/goerr/v2"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/policy"
	"github.com/secmon-lab/swarm/pkg/infra/pubsub"
	"github.com/secmon-lab/swarm/pkg/usecase"
)

type subscriptionEvent struct {
	op          string // "pull", "modify" or "ack"
	ackID       string
	maxMessages int
	received    int
}

// fakeSubscription is a subscription that delivers queued messages and records calls to it.
type fakeSubscription struct {
	mutex  sync.Mutex
	queue  []*pubsubpb.ReceivedMessage
	events []subscriptionEvent
	pulls  int

	// messagesPerPull limits the number of messages returned by one Pull. 0 means no limit.
	messagesPerPull int
	// beforePull is called with the 1-based count of Pull calls. If it returns an error, Pull returns the error without delivering messages.
	beforePull func(call int) error
	// ackErr is returned by Acknowledge for the message whose ack ID is the key.
	ackErr map[string]error
}

func newFakeSubscription(t *testing.T, objectNames ...string) *fakeSubscription {
	sub := &fakeSubscription{}
	for _, name := range objectNames {
		data, err := json.Marshal(model.CloudStorageEvent{
			Kind:   "storage#object",
			Bucket: "cloudtrail-logs",
			Name:   types.CSObjectID(name),
		})
		gt.NoError(t, err)
		sub.queue = append(sub.queue, &pubsubpb.ReceivedMessage{
			AckId:   name,
			Message: &pubsubpb.PubsubMessage{Data: data},
		})
	}
	return sub
}

func (x *fakeSubscription) mock() *pubsub.SubscriptionMock {
	return &pubsub.SubscriptionMock{
		MockPull: func(ctx context.Context, subName string, maxMessages int) ([]*pubsubpb.ReceivedMessage, error) {
			x.mutex.Lock()
			x.pulls++
			call := x.pulls
			x.mutex.Unlock()

			if x.beforePull != nil {
				if err := x.beforePull(call); err != nil {
					return nil, err
				}
			}

			x.mutex.Lock()
			defer x.mutex.Unlock()

			n := min(maxMessages, len(x.queue))
			if x.messagesPerPull > 0 {
				n = min(n, x.messagesPerPull)
			}
			msgs := x.queue[:n]
			x.queue = x.queue[n:]
			x.events = append(x.events, subscriptionEvent{op: "pull", maxMessages: maxMessages, received: n})
			return msgs, nil
		},
		MockModifyAckDeadline: func(ctx context.Context, subName string, ackID string, deadline time.Duration) error {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			x.events = append(x.events, subscriptionEvent{op: "modify", ackID: ackID})
			return nil
		},
		MockAcknowledge: func(ctx context.Context, subName string, ackID string) error {
			x.mutex.Lock()
			defer x.mutex.Unlock()
			if err := x.ackErr[ackID]; err != nil {
				return err
			}
			x.events = append(x.events, subscriptionEvent{op: "ack", ackID: ackID})
			return nil
		},
	}
}

func (x *fakeSubscription) eventsOf(op string) []subscriptionEvent {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	var resp []subscriptionEvent
	for _, ev := range x.events {
		if ev.op == op {
			resp = append(resp, ev)
		}
	}
	return resp
}

func (x *fakeSubscription) ackedIDs() []string {
	var ids []string
	for _, ev := range x.eventsOf("ack") {
		ids = append(ids, ev.ackID)
	}
	return ids
}

var (
	errObjectNotFound = goerr.New("object not found")
	errPull           = goerr.New("pull failed")
)

// newJobUseCase returns UseCase that loads the CloudTrail example object for each message. Loading an object named "fail.log" fails after calling onFail. insert is called when records of a message are inserted.
func newJobUseCase(t *testing.T, sub *fakeSubscription, onFail func(), insert func(), options ...usecase.Option) *usecase.UseCase {
	return newJobUseCaseWithInsert(t, sub, onFail, func() error {
		if insert != nil {
			insert()
		}
		return nil
	}, options...)
}

// newJobUseCaseWithInsert is newJobUseCase whose insert returns the result of writing records of a message.
func newJobUseCaseWithInsert(t *testing.T, sub *fakeSubscription, onFail func(), insert func() error, options ...usecase.Option) *usecase.UseCase {
	csClient := &cs.Mock{
		MockOpen: func(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
			if obj.Name == "fail.log" {
				if onFail != nil {
					onFail()
				}
				return nil, errObjectNotFound
			}
			return io.NopCloser(bytes.NewReader(cloudTrailExampleRaw)), nil
		},
	}
	bqClient := &bq.Mock{
		MockNewStream: func(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
			return &bq.MockStream{
				MockInsertSerializedRows: func(ctx context.Context, rows [][]byte) error {
					return insert()
				},
			}, nil
		},
	}
	pClient := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)

	return usecase.New(
		infra.New(
			infra.WithBigQuery(bqClient),
			infra.WithCloudStorage(csClient),
			infra.WithPolicy(pClient),
			infra.WithPubSubSubscription(sub.mock()),
		),
		options...,
	)
}

func TestRunWithSubscriptionsLimitsConcurrency(t *testing.T) {
	const concurrency = 3
	var names []string
	for i := 0; i < 10; i++ {
		names = append(names, fmt.Sprintf("obj-%d.log", i))
	}
	sub := newFakeSubscription(t, names...)

	// The first inserts wait until `concurrency` messages are inserted at the same time. If messages were processed one by one, they would wait until the timeout.
	var mutex sync.Mutex
	active, maxActive := 0, 0
	reached := make(chan struct{})
	insert := func() {
		mutex.Lock()
		active++
		maxActive = max(maxActive, active)
		if active == concurrency {
			select {
			case <-reached:
			default:
				close(reached)
			}
		}
		mutex.Unlock()

		select {
		case <-reached:
		case <-time.After(5 * time.Second):
		}

		mutex.Lock()
		active--
		mutex.Unlock()
	}
	uc := newJobUseCase(t, sub, nil, insert, usecase.WithMessageConcurrency(concurrency))

	gt.NoError(t, uc.RunWithSubscriptions(context.Background(), []string{"sub"}))

	gt.Equal(t, maxActive, concurrency)
	for _, ev := range sub.eventsOf("pull") {
		gt.N(t, ev.maxMessages).Greater(0).LessOrEqual(concurrency)
	}
	gt.A(t, sub.ackedIDs()).UnorderedEqual(names)
}

func TestRunWithSubscriptionsSequentialWithConcurrencyOne(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log", "obj-1.log", "obj-2.log")
	uc := newJobUseCase(t, sub, nil, nil, usecase.WithMessageConcurrency(1))

	gt.NoError(t, uc.RunWithSubscriptions(context.Background(), []string{"sub"}))

	expected := []subscriptionEvent{
		{op: "pull", maxMessages: 1, received: 1},
		{op: "ack", ackID: "obj-0.log"},
		{op: "pull", maxMessages: 1, received: 1},
		{op: "ack", ackID: "obj-1.log"},
		{op: "pull", maxMessages: 1, received: 1},
		{op: "ack", ackID: "obj-2.log"},
		{op: "pull", maxMessages: 1, received: 0},
	}
	gt.Equal(t, sub.events, expected)
}

func TestRunWithSubscriptionsFailure(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log", "fail.log", "obj-2.log", "obj-3.log", "obj-4.log")

	// Successful messages are still in process when the failure happens. They finish after the failure has been reported, which takes much less than the wait below.
	failed := make(chan struct{})
	onFail := func() { close(failed) }
	insert := func() {
		<-failed
		time.Sleep(100 * time.Millisecond)
	}
	uc := newJobUseCase(t, sub, onFail, insert, usecase.WithMessageConcurrency(3))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.Error(t, err)
	gt.True(t, errors.Is(err, errObjectNotFound))

	// The messages in process are acknowledged, and the failed message is not.
	gt.A(t, sub.ackedIDs()).UnorderedEqual([]string{"obj-0.log", "obj-2.log"})
	// No message is pulled after the failure.
	gt.A(t, sub.eventsOf("pull")).Length(1)
}

func TestRunWithSubscriptionsPullFailure(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log", "obj-1.log")
	sub.messagesPerPull = 1

	// The second Pull fails while the first message is in process.
	pulledSecond := make(chan struct{})
	sub.beforePull = func(call int) error {
		if call == 2 {
			close(pulledSecond)
			return errPull
		}
		return nil
	}
	insert := func() { <-pulledSecond }
	uc := newJobUseCase(t, sub, nil, insert, usecase.WithMessageConcurrency(2))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.True(t, errors.Is(err, errPull))
	gt.A(t, sub.ackedIDs()).Equal([]string{"obj-0.log"})
}

func TestRunWithSubscriptionsReturnsProcessingErrorBeforePullError(t *testing.T) {
	sub := newFakeSubscription(t, "fail.log", "obj-1.log")
	sub.messagesPerPull = 1

	// The second Pull fails after processing the first message has failed.
	failed := make(chan struct{})
	onFail := func() { close(failed) }
	sub.beforePull = func(call int) error {
		if call == 2 {
			<-failed
			// Give the failed message time to report its result.
			time.Sleep(100 * time.Millisecond)
			return errPull
		}
		return nil
	}
	uc := newJobUseCase(t, sub, onFail, nil, usecase.WithMessageConcurrency(2))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.True(t, errors.Is(err, errObjectNotFound))
	gt.A(t, sub.ackedIDs()).Length(0)
}

func TestRunWithSubscriptionsAcknowledgeFailure(t *testing.T) {
	errAck := goerr.New("ack failed")
	sub := newFakeSubscription(t, "obj-0.log", "obj-1.log")
	sub.ackErr = map[string]error{"obj-0.log": errAck}
	uc := newJobUseCase(t, sub, nil, nil, usecase.WithMessageConcurrency(1))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.True(t, errors.Is(err, errAck))
	// No message is pulled after the failure.
	gt.A(t, sub.eventsOf("pull")).Length(1)
}

func TestRunWithSubscriptionsNoDeadlineExtensionAfterAck(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log", "obj-1.log", "obj-2.log", "obj-3.log")
	// Processing a message takes several extension intervals.
	insert := func() { time.Sleep(30 * time.Millisecond) }
	uc := newJobUseCase(t, sub, nil, insert, usecase.WithMessageConcurrency(2))
	uc.SetAckDeadlineExtendInterval(time.Millisecond)

	gt.NoError(t, uc.RunWithSubscriptions(context.Background(), []string{"sub"}))

	// The deadline is extended while messages are processed.
	gt.A(t, sub.eventsOf("modify")).Longer(0)

	// RunWithSubscriptions waits for the extenders to stop, so no event is added after it returns.
	acked := map[string]bool{}
	for _, ev := range sub.events {
		switch ev.op {
		case "ack":
			acked[ev.ackID] = true
		case "modify":
			gt.False(t, acked[ev.ackID])
		}
	}
	gt.A(t, sub.ackedIDs()).Length(4)
}

func TestRunWithSubscriptionsDoesNotAckFailedInsert(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log")
	// The stream returns an error when it gives up retrying appends.
	errInsert := goerr.New("gave up appending rows")
	uc := newJobUseCaseWithInsert(t, sub, nil, func() error { return errInsert }, usecase.WithMessageConcurrency(1))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.True(t, errors.Is(err, errInsert))
	gt.A(t, sub.ackedIDs()).Length(0)
}

func TestRunWithSubscriptionsAcknowledgesObjectWithoutEventRule(t *testing.T) {
	// No event rule in testdata/policy matches "_check/obj-1", whose name ends with neither ".log" nor ".gz".
	sub := newFakeSubscription(t, "obj-0.log", "_check/obj-1", "obj-2.log")
	uc := newJobUseCase(t, sub, nil, nil, usecase.WithMessageConcurrency(1))

	gt.NoError(t, uc.RunWithSubscriptions(context.Background(), []string{"sub"}))

	// The message is acknowledged so that it is not redelivered, and the following messages are still processed.
	gt.A(t, sub.ackedIDs()).Equal([]string{"obj-0.log", "_check/obj-1", "obj-2.log"})
}

func TestRunWithSubscriptionsInvalidConcurrency(t *testing.T) {
	sub := newFakeSubscription(t, "obj-0.log")
	uc := newJobUseCase(t, sub, nil, nil, usecase.WithMessageConcurrency(0))

	err := uc.RunWithSubscriptions(context.Background(), []string{"sub"})
	gt.Error(t, err)
	gt.True(t, errors.Is(err, types.ErrInvalidOption))
	gt.A(t, sub.eventsOf("pull")).Length(0)
}
