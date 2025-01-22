package cmd

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/controller/server"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/firestore"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func serveCommand() *cli.Command {
	var (
		addr                    string
		readConcurrency         int
		ingestTableConcurrency  int
		ingestRecordConcurrency int
		stateTimeout            time.Duration
		stateTTL                time.Duration

		bq       config.BigQuery
		policy   config.Policy
		metadata config.Metadata
		sentry   config.Sentry

		firestoreProject  string
		firestoreDatabase string

		memoryLimit string
	)

	return &cli.Command{
		Name:  "serve",
		Usage: "Start swarm server",
		Flags: mergeFlags([]cli.Flag{
			&cli.StringFlag{
				Name:        "addr",
				Aliases:     []string{"a"},
				EnvVars:     []string{"SWARM_ADDR"},
				Usage:       "Address to listen",
				Destination: &addr,
				Value:       "localhost:8080",
			},
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
			&cli.DurationFlag{
				Name:        "state-timeout",
				EnvVars:     []string{"SWARM_STATE_TIMEOUT"},
				Usage:       "Timeout duration to wait state",
				Destination: &stateTimeout,
				Value:       30 * time.Minute,
			},
			&cli.DurationFlag{
				Name:        "state-ttl",
				EnvVars:     []string{"SWARM_STATE_TTL"},
				Usage:       "TTL duration to keep state",
				Destination: &stateTTL,
				Value:       7 * 24 * time.Hour,
			},
			&cli.StringFlag{
				Name:        "firestore-project-id",
				EnvVars:     []string{"SWARM_FIRESTORE_PROJECT_ID"},
				Usage:       "Project ID of Firestore (To manage state)",
				Destination: &firestoreProject,
			},
			&cli.StringFlag{
				Name:        "firestore-database-id",
				EnvVars:     []string{"SWARM_FIRESTORE_DATABASE_ID"},
				Usage:       "Database ID of Firestore (To manage state)",
				Destination: &firestoreDatabase,
			},
			&cli.StringFlag{
				Name:        "memory-limit",
				EnvVars:     []string{"SWARM_MEMORY_LIMIT"},
				Usage:       "Memory limit for each process. If it exceeds the limit, the process return 429 too many requests error. (e.g. 1GiB)",
				Destination: &memoryLimit,
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
					"state-timeout", stateTimeout.String(),
					"state-ttl", stateTTL.String(),
					"firestore-project-id", firestoreProject,
					"firestore-database-id", firestoreDatabase,
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

			if firestoreProject != "" && firestoreDatabase != "" {
				dbClient, err := firestore.New(ctx, firestoreProject, firestoreDatabase)
				if err != nil {
					return goerr.Wrap(err, "failed to configure Firestore client")
				}
				infraOptions = append(infraOptions, infra.WithDatabase(dbClient))
			} else if firestoreProject != "" || firestoreDatabase != "" {
				return goerr.New("both firestore-project-id and firestore-database-id are required")
			} else {
				utils.Logger().Warn("firestore is not configured")
			}

			ucOptions := []usecase.Option{
				usecase.WithIngestTableConcurrency(ingestTableConcurrency),
				usecase.WithIngestRecordConcurrency(ingestRecordConcurrency),
				usecase.WithStateTimeout(stateTimeout),
				usecase.WithStateTTL(stateTTL),
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

			var serverOptions []server.Option
			if memoryLimit != "" {
				limit, err := humanize.ParseBytes(memoryLimit)
				if err != nil {
					return goerr.Wrap(err, "invalid memory limit option")
				}
				serverOptions = append(serverOptions, server.WithMemoryLimit(limit))
			}

			srv := server.New(uc, serverOptions...)

			// Listen srv on addr
			httpServer := &http.Server{
				Addr:              addr,
				ReadHeaderTimeout: 3 * time.Second,
				Handler:           srv,
			}

			errCh := make(chan error, 1)
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

			go func() {
				defer close(errCh)
				utils.Logger().Info("starting server", "addr", addr)
				if err := httpServer.ListenAndServe(); err != nil {
					errCh <- goerr.Wrap(err, "failed to listen")
				}
			}()

			select {
			case sig := <-sigCh:
				utils.Logger().Info("received signal and shutting down", "signal", sig)
				if err := httpServer.Shutdown(c.Context); err != nil {
					return goerr.Wrap(err, "failed to shutdown server")
				}

			case err := <-errCh:
				return err
			}

			return nil
		},
	}
}
