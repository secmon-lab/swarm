package cs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type Mock struct {
	MockOpen  func(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (io.ReadCloser, error)
	MockAttrs func(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (*storage.ObjectAttrs, error)
	MockList  func(ctx context.Context, bucket types.CSBucket, query *storage.Query) interfaces.CSObjectIterator
}

type MockObjectIterator struct {
	MockNext func() (*storage.ObjectAttrs, error)
}

func (x *MockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if x.MockNext != nil {
		return x.MockNext()
	}
	return nil, nil
}

func (x *Mock) Open(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (io.ReadCloser, error) {
	if x.MockOpen != nil {
		return x.MockOpen(ctx, bucket, object)
	}
	return nil, nil
}

func (x *Mock) Attrs(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (*storage.ObjectAttrs, error) {
	if x.MockAttrs != nil {
		return x.MockAttrs(ctx, bucket, object)
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

type MockIterator struct {
	MockNext func(dst interface{}) error
}

func (x *MockIterator) Next(dst interface{}) error {
	if x.MockNext != nil {
		return x.MockNext(dst)
	}
	return nil
}
