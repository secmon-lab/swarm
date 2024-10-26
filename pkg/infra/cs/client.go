package cs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type Client struct {
	client *storage.Client
}

func New(ctx context.Context) (*Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create storage client")
	}

	return &Client{
		client: client,
	}, nil
}

func (x *Client) Open(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
	r, err := x.client.
		Bucket(obj.Bucket.String()).
		Object(obj.Name.String()).
		NewReader(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create reader")
	}

	return r, nil
}

func (x *Client) Attrs(ctx context.Context, obj model.CloudStorageObject) (*storage.ObjectAttrs, error) {
	attrs, err := x.client.
		Bucket(obj.Bucket.String()).
		Object(obj.Name.String()).
		Attrs(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to get object attributes")
	}

	return attrs, nil
}

func (x *Client) List(ctx context.Context, bucket types.CSBucket, query *storage.Query) interfaces.CSObjectIterator {
	return x.client.Bucket(bucket.String()).Objects(ctx, query)
}

var _ interfaces.CloudStorage = &Client{}
