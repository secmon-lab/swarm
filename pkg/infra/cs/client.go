package cs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/types"
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

func (x *Client) Open(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (io.ReadCloser, error) {
	r, err := x.client.
		Bucket(bucket.String()).
		Object(object.String()).
		NewReader(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to create reader")
	}

	return r, nil
}

func (x *Client) Attrs(ctx context.Context, bucket types.CSBucket, object types.CSObjectID) (*storage.ObjectAttrs, error) {
	attrs, err := x.client.
		Bucket(bucket.String()).
		Object(object.String()).
		Attrs(ctx)
	if err != nil {
		return nil, goerr.Wrap(err, "failed to get object attributes")
	}

	return attrs, nil
}

var _ interfaces.CloudStorage = &Client{}
