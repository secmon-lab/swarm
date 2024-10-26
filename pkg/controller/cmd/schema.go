package cmd

import (
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/dump"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/urfave/cli/v2"
)

func schemaCommand() *cli.Command {
	var (
		outputDir string
		bq        config.BigQuery
		policy    config.Policy
	)
	return &cli.Command{
		Name:  "schema",
		Usage: "Infer schema from Cloud Storage object, and apply it to BigQuery table",
		Flags: mergeFlags([]cli.Flag{
			&cli.StringFlag{
				Name:        "output-dir",
				Aliases:     []string{"o"},
				Usage:       "Output directory path, default is current directory",
				EnvVars:     []string{"SWARM_OUTPUT_DIR"},
				Destination: &outputDir,
			},
		}, bq.Flags(), policy.Flags()),

		Action: func(c *cli.Context) error {
			var bqClient interfaces.BigQuery
			if outputDir != "" {
				bqClient = dump.New(outputDir)
			} else {
				client, err := bq.Configure(c.Context)
				if err != nil {
					return err
				}
				bqClient = client
			}

			policyClient, err := policy.Configure()
			if err != nil {
				return err
			}

			csClient, err := cs.New(c.Context)
			if err != nil {
				return err
			}

			clients := infra.New(
				infra.WithBigQuery(bqClient),
				infra.WithCloudStorage(csClient),
				infra.WithPolicy(policyClient),
			)
			uc := usecase.New(clients)

			var urls []types.CSUrl
			for i := 0; i < c.Args().Len(); i++ {
				urls = append(urls, types.CSUrl(c.Args().Get(i)))
			}

			return uc.ApplyInferredSchema(c.Context, urls)
		},
	}
}
