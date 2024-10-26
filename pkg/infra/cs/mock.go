package cs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"google.golang.org/api/iterator"
)

type Mock struct {
	MockOpen  func(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error)
	MockAttrs func(ctx context.Context, obj model.CloudStorageObject) (*storage.ObjectAttrs, error)
	MockList  func(ctx context.Context, bucket types.CSBucket, query *storage.Query) interfaces.CSObjectIterator
}

type MockObjectIterator struct {
	MockNext func() (*storage.ObjectAttrs, error)
	Attrs    []*storage.ObjectAttrs
}

func (x *MockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if x.MockNext != nil {
		return x.MockNext()
	}

	if len(x.Attrs) == 0 {
		return nil, iterator.Done
	}
	resp := x.Attrs[0]
	x.Attrs = x.Attrs[1:]
	return resp, nil
}

func (x *Mock) Open(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
	if x.MockOpen != nil {
		return x.MockOpen(ctx, obj)
	}
	return nil, nil
}

func (x *Mock) Attrs(ctx context.Context, obj model.CloudStorageObject) (*storage.ObjectAttrs, error) {
	if x.MockAttrs != nil {
		return x.MockAttrs(ctx, obj)
	}
	return nil, nil
}

func (x *Mock) List(ctx context.Context, bucket types.CSBucket, query *storage.Query) interfaces.CSObjectIterator {
	if x.MockList != nil {
		return x.MockList(ctx, bucket, query)
	}
	return nil
}

var _ interfaces.CloudStorage = &Mock{}
