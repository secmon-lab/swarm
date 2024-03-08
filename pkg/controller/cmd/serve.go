package cmd

import (
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
	"github.com/m-mizutani/swarm/pkg/usecase"
	"github.com/m-mizutani/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func serveCommand() *cli.Command {
	var (
		addr        string
		concurrency int
		bq          config.BigQuery
		policy      config.Policy
		metadata    config.Metadata
		sentry      config.Sentry
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
				Destination: &concurrency,
				Value:       32,
			},
		}, bq.Flags(), policy.Flags(), metadata.Flags(), sentry.Flags()),
		Action: func(c *cli.Context) error {
			ctx := c.Context

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

			var ucOptions []usecase.Option
			if meta, err := metadata.Configure(); err != nil {
				return goerr.Wrap(err, "failed to configure metadata")
			} else if meta != nil {
				ucOptions = append(ucOptions, usecase.WithMetadata(meta))
			}

			if concurrency > 0 {
				ucOptions = append(ucOptions, usecase.WithReadObjectConcurrency(concurrency))
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
