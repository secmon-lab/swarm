package pubsub

import (
	"context"
	"sync"

	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
)

type SubscriptionMock struct {
	MockReceive func(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error
}

func (x *SubscriptionMock) Receive(ctx context.Context, subName string, f func(context.Context, interfaces.PubSubMessage)) error {
	return x.MockReceive(ctx, subName, f)
}

func (x *SubscriptionMock) Close() error {
	return nil
}

// MockMessage implements interfaces.PubSubMessage for testing.
type MockMessage struct {
	MessageID   string
	MessageData []byte

	mu    sync.Mutex
	acked bool
	naked bool
}

func (m *MockMessage) Data() []byte { return m.MessageData }
func (m *MockMessage) ID() string   { return m.MessageID }

func (m *MockMessage) Ack() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
}

func (m *MockMessage) Nack() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.naked = true
}

func (m *MockMessage) Acked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acked
}

func (m *MockMessage) Nacked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.naked
}
