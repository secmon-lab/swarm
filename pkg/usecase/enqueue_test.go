package usecase_test

import (
	"context"
	"encoding/json"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/infra/cs"
	"github.com/m-mizutani/swarm/pkg/infra/pubsub"
	"github.com/m-mizutani/swarm/pkg/usecase"
)

func TestEnqueue(t *testing.T) {
	var calledList int
	csMock := &cs.Mock{
		MockList: func(ctx context.Context, bucket types.CSBucket, query *storage.Query) interfaces.CSObjectIterator {
			calledList++
			return &cs.MockObjectIterator{
				Attrs: []*storage.ObjectAttrs{
					{
						Bucket: "bucket",
						Name:   "object1",
						Size:   100,
					},
					{
						Bucket: "bucket",
						Name:   "object2",
						Size:   200,
					},
				},
			}
		},
	}

	pubsubMock := pubsub.NewMock()

	uc := usecase.New(infra.New(
		infra.WithCloudStorage(csMock),
		infra.WithPubSub(pubsubMock),
	))

	req := &model.EnqueueRequest{
		URLs: []types.ObjectURL{"gs://bucket/prefix/"},
	}

	resp := gt.R1(uc.Enqueue(context.Background(), req)).NoError(t)
	gt.V(t, resp.Count).Equal(2)
	gt.A(t, pubsubMock.Results).Length(1).At(0, func(t testing.TB, v *pubsub.MockResult) {
		var msg model.SwarmMessage
		gt.NoError(t, json.Unmarshal(v.Data, &msg))
		gt.A(t, msg.Objects).Length(2)
	})
	gt.V(t, calledList).Equal(1)
}
