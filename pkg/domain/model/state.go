package model

import (
	"time"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type State struct {
	ID        string          `firestore:"id"`
	RequestID types.RequestID `firestore:"request_id"`
	State     types.MsgState  `firestore:"state"`
	CreatedAt time.Time       `firestore:"created_at"`
	UpdatedAt time.Time       `firestore:"updated_at"`
	ExpiresAt time.Time       `firestore:"expires_at"`
	TTL       time.Time       `firestore:"ttl"`
}

func (x *State) Acquired(now time.Time) bool {
	switch x.State {
	case types.MsgRunning:
		return x.ExpiresAt.Before(now)
	case types.MsgCompleted:
		return false
	case types.MsgFailed:
		return true

	default:
		panic("unexpected state type: " + string(x.State))
	}
}
