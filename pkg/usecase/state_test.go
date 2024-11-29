package usecase_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
)

type mockFirestore struct {
	interfaces.Database
	state model.State
}

func (m *mockFirestore) GetState(ctx context.Context, msgType types.MsgType, id string) (*model.State, error) {
	return &m.state, nil
}

func TestStateWaitExpired(t *testing.T) {
	t.Parallel()

	mock := &mockFirestore{
		state: model.State{
			State:     types.MsgRunning,
			RequestID: types.NewRequestID(),
		},
	}

	now := time.Now()
	ctx := utils.CtxWithTime(context.Background(), func() time.Time {
		return now
	})
	uc := usecase.New(infra.New(infra.WithDatabase(mock)), usecase.WithStateCheckInterval(time.Millisecond))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gt.NoError(t, uc.WaitState(ctx, types.MsgType("test"), "test", now.Add(10*time.Second)))
	}()

	done := false
	go func() {
		wg.Wait()
		done = true
	}()

	time.Sleep(100 * time.Millisecond)
	gt.False(t, done)
	now = now.Add(5 * time.Second)
	gt.False(t, done)
	now = now.Add(6 * time.Second)
	time.Sleep(100 * time.Millisecond)
	gt.True(t, done)
}

func TestStateWaitCompleted(t *testing.T) {
	t.Parallel()

	mock := &mockFirestore{
		state: model.State{
			State:     types.MsgRunning,
			RequestID: types.NewRequestID(),
		},
	}

	now := time.Now()
	ctx := utils.CtxWithTime(context.Background(), func() time.Time {
		return now
	})
	uc := usecase.New(infra.New(infra.WithDatabase(mock)), usecase.WithStateCheckInterval(time.Millisecond))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gt.NoError(t, uc.WaitState(ctx, types.MsgType("test"), "test", now.Add(10*time.Second)))
	}()

	done := false
	go func() {
		wg.Wait()
		done = true
	}()

	time.Sleep(100 * time.Millisecond)
	gt.False(t, done)
	mock.state.State = types.MsgCompleted
	time.Sleep(100 * time.Millisecond)
	gt.True(t, done)
}
