package usecase

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"
	"google.golang.org/api/iterator"
)

func (x *UseCase) ApplyInferredSchema(ctx context.Context, urls []types.CSUrl) error {
	var objects []model.Object
	logger := utils.CtxLogger(ctx)

	for _, url := range urls {
		var tmp []model.Object
		bucket, objPrefix, err := url.Parse()
		if err != nil {
			return err
		}

		query := &storage.Query{
			Prefix: objPrefix.String(),
		}
		it := x.clients.CloudStorage().List(ctx, bucket, query)

		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			obj := model.NewObjectFromCloudStorageAttrs(attrs)
			tmp = append(tmp, obj)
		}

		logger.Info("found objects", "url", url, "count", len(tmp))
		objects = append(objects, tmp...)
	}

	return x.applyInferredSchema(ctx, objects)
}

func (x *UseCase) applyInferredSchema(ctx context.Context, objects []model.Object) error {
	var requests []*model.LoadRequest

	for _, obj := range objects {
		sources, err := x.ObjectToSources(ctx, obj)
		if err != nil {
			return err
		}

		for _, src := range sources {
			requests = append(requests, &model.LoadRequest{
				Object: obj,
				Source: *src,
			})
		}
	}

	logger := utils.CtxLogger(ctx)
	logger.Info("importing objects", "source.size", len(requests))
	records, _, err := importLogRecords(ctx, x.clients, requests, x.readObjectConcurrency)
	if err != nil {
		return err
	}

	for dst, records := range records {
		schema, err := inferSchema(records)
		if err != nil {
			return err
		}

		md, err := buildBQMetadata(schema, dst.Partition)
		if err != nil {
			return err
		}

		if _, err := createOrUpdateTable(ctx, x.clients.BigQuery(), dst.Dataset, dst.Table, md); err != nil {
			return err
		}
	}

	return nil
}
