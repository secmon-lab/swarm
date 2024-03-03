package usecase

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"google.golang.org/api/iterator"
)

const (
	enqueueObjectCountLimit = 1024
	enqueueObjectSizeLimit  = 4 * 1024 * 1024 // 4MB
)

func (x *UseCase) Enqueue(ctx context.Context, req *model.EnqueueRequest) (*model.EnqueueResponse, error) {
	startedAt := time.Now()
	var (
		totalCount int64
		totalSize  int64
	)

	bucket, objPrefix, err := req.URL.ParseAsCloudStorage()
	if err != nil {
		return nil, err
	}

	query := &storage.Query{
		Prefix: objPrefix.String(),
	}

	var objects []*model.Object
	it := x.clients.CloudStorage().List(ctx, bucket, query)
	for {
		attrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, goerr.Wrap(err, "failed to list objects")
		}

		obj := model.NewObjectFromCloudStorageAttrs(attrs)
		if obj.Size != nil {
			totalSize += *obj.Size
		}
		totalCount++

		if sumObjectSize(&obj, objects...) > enqueueObjectSizeLimit ||
			len(objects) >= enqueueObjectCountLimit {
			if err := enqueueObjects(ctx, x.clients.PubSub(), objects); err != nil {
				return nil, err
			}
			objects = nil
		}

		objects = append(objects, &obj)
	}

	if len(objects) > 0 {
		if err := enqueueObjects(ctx, x.clients.PubSub(), objects); err != nil {
			return nil, err
		}
	}

	return &model.EnqueueResponse{
		Elapsed: time.Since(startedAt),
		Count:   totalCount,
		Size:    totalSize,
	}, nil
}

func sumObjectSize(newOjb *model.Object, objects ...*model.Object) int64 {
	var sum int64
	if newOjb.Size != nil {
		sum += *newOjb.Size
	}

	for _, obj := range objects {
		if obj.Size != nil {
			sum += *obj.Size
		}
	}
	return sum
}

func enqueueObjects(ctx context.Context, client interfaces.PubSub, objects []*model.Object) error {
	msg := model.SwarmMessage{
		Objects: objects,
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return goerr.Wrap(err, "failed to marshal message")
	}

	if _, err := client.Publish(ctx, raw); err != nil {
		return err
	}

	return nil
}
