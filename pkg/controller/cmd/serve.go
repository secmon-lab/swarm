package cmd

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/controller/cmd/config"
	"github.com/m-mizutani/swarm/pkg/controller/server"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/infra/cs"
	"github.com/m-mizutani/swarm/pkg/infra/firestore"
	"github.com/m-mizutani/swarm/pkg/usecase"
	"github.com/m-mizutani/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func serveCommand() *cli.Command {
	var (
		addr              string
		readConcurrency   int
		ingestConcurrency int
		stateTimeout      time.Duration
		stateTTL          time.Duration

		bq       config.BigQuery
		policy   config.Policy
		metadata config.Metadata
		sentry   config.Sentry

		firestoreProject  string
		firestoreDatabase string
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
				Name:        "ingest-concurrency",
				EnvVars:     []string{"SWARM_INGEST_CONCURRENCY"},
				Usage:       "Number of concurrent ingest to BigQuery",
				Destination: &ingestConcurrency,
				Value:       32,
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
		}, bq.Flags(), policy.Flags(), metadata.Flags(), sentry.Flags()),
		Action: func(c *cli.Context) error {
			ctx := c.Context

			utils.Logger().Info("starting server",
				slog.Group("config",
					"addr", addr,
					"read-concurrency", readConcurrency,
					"ingest-concurrency", ingestConcurrency,
					"state-timeout", stateTimeout.String(),
					"state-ttl", stateTTL.String(),
					"firestore-project-id", firestoreProject,
					"firestore-database-id", firestoreDatabase,

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
				usecase.WithIngestConcurrency(ingestConcurrency),
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
			srv := server.New(uc)

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
