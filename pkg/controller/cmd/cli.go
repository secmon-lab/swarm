package cmd

import (
	"github.com/m-mizutani/swarm/pkg/controller/cmd/config"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/utils"

	"github.com/urfave/cli/v2"
)

type runtime struct {
}

func Run(argv []string) error {
	var (
		rt     runtime
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
			ingestCommand(&rt),
			serveCommand(&rt),
			retryCommand(&rt),
			clientCommand(&rt),
			schemaCommand(&rt),
		},
	}

	if err := app.Run(argv); err != nil {
		utils.Logger().Error("failed to run command", utils.ErrLog(err))
		return err
	}

	return nil
}
