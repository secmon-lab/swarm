package cmd

import (
	"log/slog"

	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/pubsub"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/secmon-lab/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func enqueueCommand() *cli.Command {
	var (
		pubsubCfg  config.PubSub
		countLimit int
		sizeLimit  int
		outDir     string
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
			&cli.IntFlag{
				Name:        "count-limit",
				EnvVars:     []string{"SWARM_ENQUEUE_COUNT_LIMIT"},
				Usage:       "Limit of object count to enqueue",
				Destination: &countLimit,
				Value:       128,
			},
			&cli.IntFlag{
				Name:        "size-limit",
				EnvVars:     []string{"SWARM_ENQUEUE_SIZE_LIMIT"},
				Usage:       "Limit of object size to enqueue (MiB)",
				Destination: &sizeLimit,
				Value:       4,
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

			var urls []types.ObjectURL
			for _, arg := range ctx.Args().Slice() {
				urls = append(urls, types.ObjectURL(arg))
			}

			req := &model.EnqueueRequest{
				URLs: urls,
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
