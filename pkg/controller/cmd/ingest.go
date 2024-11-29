package cmd

import (
	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/dump"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func ingestCommand() *cli.Command {
	var (
		dryRun   bool
		output   string
		bigquery config.BigQuery
		policy   config.Policy
		metadata config.Metadata
	)
	return &cli.Command{
		Name:      "ingest",
		Aliases:   []string{"i"},
		Usage:     "Ingest data from Cloud Storage into BigQuery directly",
		ArgsUsage: "[object path...]",
		Flags: mergeFlags([]cli.Flag{
			&cli.BoolFlag{
				Name:        "dry-run",
				Aliases:     []string{"d"},
				Usage:       "Dry run mode",
				EnvVars:     []string{"SWARM_DRY_RUN"},
				Destination: &dryRun,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "Output directory path, default is current directory",
				EnvVars:     []string{"SWARM_OUTPUT"},
				Value:       ".",
				Destination: &output,
			},
		}, bigquery.Flags(), policy.Flags(), metadata.Flags()),

		Action: func(c *cli.Context) error {
			ctx := c.Context

			policyClient, err := policy.Configure()
			if err != nil {
				return goerr.Wrap(err, "failed to configure policy client")
			}

			var bqClient interfaces.BigQuery
			if dryRun {
				utils.Logger().Info("dry run mode")
				bqClient = dump.New(output)
			} else {
				client, err := bigquery.Configure(ctx)
				if err != nil {
					return goerr.Wrap(err, "failed to configure BigQuery client")
				}
				bqClient = client
			}

			csClient, err := cs.New(ctx)
			if err != nil {
				return goerr.Wrap(err, "failed to configure CloudStorage client")
			}

			md, err := metadata.Configure()
			if err != nil {
				return goerr.Wrap(err, "failed to configure metadata")
			}

			uc := usecase.New(
				infra.New(
					infra.WithPolicy(policyClient),
					infra.WithCloudStorage(csClient),
					infra.WithBigQuery(bqClient),
				),
				usecase.WithMetadata(md),
			)

			for _, url := range c.Args().Slice() {
				if err := uc.LoadDataByObject(ctx, types.CSUrl(url)); err != nil {
					return goerr.Wrap(err, "failed to load data").With("url", url)
				}
			}
			return nil
		},
	}
}
