package firestore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/firestore"
	"github.com/m-mizutani/swarm/pkg/utils"
)

func setupClient(t *testing.T) *firestore.Client {
	projectID := utils.LoadEnv(t, "TEST_FIRESTORE_PROJECT_ID")
	databaseID := utils.LoadEnv(t, "TEST_FIRESTORE_DATABASE_ID")

	ctx := context.Background()
	client := gt.R1(firestore.New(ctx, projectID, databaseID)).NoError(t)

	return client
}

func TestFirestoreState(t *testing.T) {
	client := setupClient(t)

	ctx := context.Background()
	id := uuid.NewString()

	now := time.Now()

	input1 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	}
	state1, acquired1 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input1)).NoError(t)
	gt.Equal(t, state1.ID, id)
	gt.Equal(t, state1.State, types.MsgRunning)
	gt.True(t, acquired1)

	input2 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	}

	state2, acquired2 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input2)).NoError(t)
	gt.Equal(t, state2.ID, id)
	gt.Equal(t, state2.State, types.MsgRunning)
	gt.False(t, acquired2)
}

func TestFirestoreStateExpired(t *testing.T) {
	client := setupClient(t)

	ctx := context.Background()
	id := uuid.NewString()

	now := time.Now()

	input1 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Second),
	}
	input2 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now.Add(2 * time.Second),
		UpdatedAt: now,
		ExpiresAt: now.Add(4 * time.Second),
	}

	state1, acquired1 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input1)).NoError(t)
	gt.Equal(t, state1.ID, id)
	gt.Equal(t, state1.State, types.MsgRunning)
	gt.True(t, acquired1)

	state2, acquired2 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input2)).NoError(t)
	gt.Equal(t, state2.ID, id)
	gt.Equal(t, state2.State, types.MsgRunning)
	gt.True(t, acquired2)
}

func TestFirestoreStateCompleted(t *testing.T) {
	client := setupClient(t)

	ctx := context.Background()
	id := uuid.NewString()

	now := time.Now()

	input1 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	}

	input2 := &model.State{
		ID:        id,
		State:     types.MsgCompleted,
		RequestID: types.NewRequestID(),
		CreatedAt: now.Add(2 * time.Second),
		UpdatedAt: now,
		ExpiresAt: now.Add(4 * time.Second),
	}

	state1, acquired1 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input1)).NoError(t)
	gt.Equal(t, state1.ID, id)
	gt.Equal(t, state1.State, types.MsgRunning)
	gt.True(t, acquired1)

	state1.State = types.MsgCompleted
	gt.NoError(t, client.UpdateState(ctx, types.MsgPubSub, id, types.MsgCompleted, now.Add(time.Second)))

	state2, acquired2 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input2)).NoError(t)
	gt.Equal(t, state2.ID, id)
	gt.Equal(t, state2.State, types.MsgCompleted)
	gt.False(t, acquired2)
}

func TestFirestoreStateFailed(t *testing.T) {
	client := setupClient(t)

	ctx := context.Background()
	id := uuid.NewString()

	now := time.Now()

	input1 := &model.State{
		ID:        id,
		State:     types.MsgRunning,
		RequestID: types.NewRequestID(),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	}

	input2 := &model.State{
		ID:        id,
		State:     types.MsgFailed,
		RequestID: types.NewRequestID(),
		CreatedAt: now.Add(2 * time.Second),
		UpdatedAt: now,
		ExpiresAt: now.Add(4 * time.Second),
	}

	state1, acquired1 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input1)).NoError(t)
	gt.Equal(t, state1.ID, id)
	gt.Equal(t, state1.State, types.MsgRunning)
	gt.True(t, acquired1)

	state1.State = types.MsgFailed
	gt.NoError(t, client.UpdateState(ctx, types.MsgPubSub, id, types.MsgFailed, now.Add(time.Second)))

	state2, acquired2 := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input2)).NoError(t)
	gt.Equal(t, state2.ID, id)
	gt.Equal(t, state2.State, types.MsgFailed)
	gt.True(t, acquired2)
}

func TestConcurrency(t *testing.T) {
	_ = utils.LoadEnv(t, "TEST_FIRESTORE_PROJECT_ID")
	_ = utils.LoadEnv(t, "TEST_FIRESTORE_DATABASE_ID")

	var wg sync.WaitGroup
	id := uuid.NewString()
	now := time.Now()

	const concurrency = 10
	cue := make(chan struct{}, concurrency)
	results := make(chan bool, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client := setupClient(t)
			ctx := context.Background()
			input := &model.State{
				ID:        id,
				State:     types.MsgRunning,
				RequestID: types.NewRequestID(),
				CreatedAt: now,
				UpdatedAt: now,
				ExpiresAt: now.Add(1 * time.Hour),
			}
			<-cue
			_, acquired := gt.R2(client.GetOrCreateState(ctx, types.MsgPubSub, input)).NoError(t)
			results <- acquired
		}()
	}

	time.Sleep(time.Second)
	for i := 0; i < concurrency; i++ {
		cue <- struct{}{}
	}
	wg.Wait()
	close(results)

	var acquired int
	for a := range results {
		if a {
			acquired++
		}
	}

	gt.Equal(t, acquired, 1)
}
