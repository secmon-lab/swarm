package cmd

import (
	"log/slog"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/pubsub"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func jobCommand() *cli.Command {
	var (
		addr                    string
		readConcurrency         int
		ingestTableConcurrency  int
		ingestRecordConcurrency int

		bq       config.BigQuery
		policy   config.Policy
		metadata config.Metadata
		sentry   config.Sentry

		memoryLimit   string
		subscriptions cli.StringSlice
	)

	return &cli.Command{
		Name:  "job",
		Usage: "Start swarm server",
		Flags: mergeFlags([]cli.Flag{
			&cli.IntFlag{
				Name:        "read-concurrency",
				EnvVars:     []string{"SWARM_READ_CONCURRENCY"},
				Usage:       "Number of concurrent read from CloudStorage",
				Destination: &readConcurrency,
				Value:       32,
			},
			&cli.IntFlag{
				Name:        "ingest-table-concurrency",
				EnvVars:     []string{"SWARM_INGEST_TABLE_CONCURRENCY"},
				Usage:       "Number of concurrent ingest to BigQuery (for tables)",
				Destination: &ingestTableConcurrency,
				Value:       16,
			},
			&cli.IntFlag{
				Name:        "ingest-record-concurrency",
				EnvVars:     []string{"SWARM_INGEST_RECORD_CONCURRENCY"},
				Usage:       "Number of concurrent ingest to BigQuery (for tables)",
				Destination: &ingestRecordConcurrency,
				Value:       16,
			},
			&cli.StringFlag{
				Name:        "memory-limit",
				EnvVars:     []string{"SWARM_MEMORY_LIMIT"},
				Usage:       "Memory limit for each process. If it exceeds the limit, the process return 429 too many requests error. (e.g. 1GiB)",
				Destination: &memoryLimit,
			},
			&cli.StringSliceFlag{
				Name:        "subscriptions",
				Usage:       "Pub/Sub subscriptions to listen",
				EnvVars:     []string{"SWARM_SUBSCRIPTIONS"},
				Destination: &subscriptions,
			},
		}, bq.Flags(), policy.Flags(), metadata.Flags(), sentry.Flags()),

		Action: func(c *cli.Context) error {
			ctx := c.Context

			utils.Logger().Info("starting server",
				slog.Group("config",
					"addr", addr,
					"read-concurrency", readConcurrency,
					"ingest-table-concurrency", ingestTableConcurrency,
					"ingest-record-concurrency", ingestRecordConcurrency,
					"memory-limit", memoryLimit,

					"bigquery", &bq,
					"policy", &policy,
					"metadata", &metadata,
					"sentry", &sentry,
				),
			)

			if err := sentry.Configure(); err != nil {
				return goerr.Wrap(err, "failed to configure sentry")
			}

			var infraOptions []infra.Option

			policyClient, err := policy.Configure()
			if err != nil {
				return goerr.Wrap(err, "failed to configure policy client")
			}
			infraOptions = append(infraOptions, infra.WithPolicy(policyClient))

			bqClient, err := bq.Configure(ctx)
			if err != nil {
				return goerr.Wrap(err, "failed to configure BigQuery client")
			}
			infraOptions = append(infraOptions, infra.WithBigQuery(bqClient))

			csClient, err := cs.New(ctx)
			if err != nil {
				return goerr.Wrap(err, "failed to configure CloudStorage client")
			}
			infraOptions = append(infraOptions, infra.WithCloudStorage(csClient))

			subClient, err := pubsub.NewSubscriptionClient(ctx)
			if err != nil {
				return goerr.Wrap(err, "failed to configure Pub/Sub subscription client")
			}
			infraOptions = append(infraOptions, infra.WithPubSubSubscription(subClient))

			ucOptions := []usecase.Option{
				usecase.WithIngestTableConcurrency(ingestTableConcurrency),
				usecase.WithIngestRecordConcurrency(ingestRecordConcurrency),
			}

			if meta, err := metadata.Configure(); err != nil {
				return goerr.Wrap(err, "failed to configure metadata")
			} else if meta != nil {
				ucOptions = append(ucOptions, usecase.WithMetadata(meta))
			}

			if readConcurrency > 0 {
				ucOptions = append(ucOptions, usecase.WithReadObjectConcurrency(readConcurrency))
			}

			uc := usecase.New(infra.New(infraOptions...), ucOptions...)

			return uc.RunWithSubscriptions(ctx, subscriptions.Value())
		},
	}
}
