package cmd

import (
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/utils"

	"github.com/urfave/cli/v2"
)

func Run(argv []string) error {
	var (
		logger config.Logger
	)

	app := cli.App{
		Name:        "swarm",
		Description: "Data ingestion tool from Google Cloud Storage to BigQuery",
		Version:     types.AppVersion,
		Flags:       mergeFlags([]cli.Flag{}, logger.Flags()),
		Before: func(c *cli.Context) error {
			logger, err := logger.Configure()
			if err != nil {
				return err
			}
			utils.SetLogger(logger)

			return nil
		},
		Commands: []*cli.Command{
			ingestCommand(),
			serveCommand(),
			clientCommand(),
			schemaCommand(),
			enqueueCommand(),
			migrateCommand(),
		},
	}

	if err := app.Run(argv); err != nil {
		utils.Logger().Error("failed to run command", utils.ErrLog(err))
		return err
	}

	return nil
}
