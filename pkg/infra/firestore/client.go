package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	client     *firestore.Client
	projectID  string
	databaseID string
}

// GetOrCreateState returns the state of message processing. If the state is not found, it creates a new state and returns it. If the state is already acquired, it returns the state.
func (x *Client) GetOrCreateState(ctx context.Context, msgType types.MsgType, input *model.State) (*model.State, bool, error) {
	var result *model.State
	var acquired bool

	collection := string(msgType)
	if err := x.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		var existed model.State
		ref := x.client.Collection(collection).Doc(input.ID)
		if resp, err := tx.Get(ref); err != nil {
			if status.Code(err) != codes.NotFound {
				return goerr.Wrap(err, "failed to get state from firestore")
			}
		} else {
			if err := resp.DataTo(&existed); err != nil {
				return goerr.Wrap(err, "failed to unmarshal state")
			}

			if !existed.Acquired(input.CreatedAt) {
				result = &existed
				acquired = false
				return nil
			}
		}

		if err := tx.Set(x.client.Collection(collection).Doc(input.ID), input); err != nil {
			return goerr.Wrap(err, "failed to create new state")
		}
		result = input
		acquired = true

		return nil
	}); err != nil {
		return nil, false, goerr.Wrap(err, "failed firestore transaction")
	}

	return result, acquired, nil
}

// GetState returns the state of message processing.
func (x *Client) GetState(ctx context.Context, msgType types.MsgType, id string) (*model.State, error) {
	collection := string(msgType)
	doc, err := x.client.Collection(collection).Doc(id).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, goerr.Wrap(types.ErrStateNotFound, "state not found")
		}
		return nil, goerr.Wrap(err, "failed to get state")
	}

	var state model.State
	if err := doc.DataTo(&state); err != nil {
		return nil, goerr.Wrap(err, "failed to unmarshal state")
	}

	return &state, nil
}

// UpdateState updates the state of message processing.
func (x *Client) UpdateState(ctx context.Context, msgType types.MsgType, id string, state types.MsgState, now time.Time) error {
	collection := string(msgType)
	if _, err := x.client.Collection(collection).Doc(id).Set(ctx, map[string]interface{}{
		"state":      state,
		"updated_at": now,
	}, firestore.MergeAll); err != nil {
		return goerr.Wrap(err, "failed to update state")
	}
	return nil
}

func New(ctx context.Context, projectID string, databaseID string) (*Client, error) {
	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to initialize firebase app")
	}

	return &Client{
		client:     client,
		projectID:  projectID,
		databaseID: databaseID,
	}, nil
}

func (x *Client) Close() error {
	if err := x.client.Close(); err != nil {
		return goerr.Wrap(err, "failed to close firestore client")
	}
	return nil
}

var _ interfaces.Database = &Client{}
