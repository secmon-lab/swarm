package cmd

import (
	"log/slog"

	"github.com/m-mizutani/swarm/pkg/controller/cmd/config"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/infra/cs"
	"github.com/m-mizutani/swarm/pkg/infra/pubsub"
	"github.com/m-mizutani/swarm/pkg/usecase"
	"github.com/m-mizutani/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func enqueueCommand() *cli.Command {
	var (
		pubsubCfg config.PubSub
		outDir    string
	)

	return &cli.Command{
		Name:      "enqueue",
		Aliases:   []string{"e"},
		Usage:     "Enqueue object ingestion request to Pub/Sub topic",
		ArgsUsage: "[object URL]",
		Flags: mergeFlags([]cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "Output directory path",
				Destination: &outDir,
			},
		}, pubsubCfg.Flags()),
		Action: func(ctx *cli.Context) error {
			var pubsubClient interfaces.PubSub

			utils.Logger().Info("Start enqueue command", "output", outDir)

			if outDir != "" {
				pubsubClient = pubsub.NewDumper(outDir)
			} else {
				client, err := pubsubCfg.Configure(ctx.Context)
				if err != nil {
					return err
				}
				pubsubClient = client
			}

			csClient, err := cs.New(ctx.Context)
			if err != nil {
				return err
			}

			clients := infra.New(
				infra.WithPubSub(pubsubClient),
				infra.WithCloudStorage(csClient),
			)
			uc := usecase.New(clients)

			req := &model.EnqueueRequest{
				URL: types.ObjectURL(ctx.Args().First()),
			}

			resp, err := uc.Enqueue(ctx.Context, req)
			if err != nil {
				return err
			}

			utils.Logger().Info("Enqueue request is completed",
				slog.Int64("object_count", resp.Count),
				slog.Int64("object_size", resp.Size),
				slog.Any("elapsed", resp.Elapsed.String()),
			)

			return nil
		},
	}
}
