package cmd

import (
	"net/http"

	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

func clientCommand() *cli.Command {
	return &cli.Command{
		Name:    "client",
		Aliases: []string{"c"},
		Usage:   "Start swarm client",
		Subcommands: []*cli.Command{
			clientHealthCheck(),
		},
	}
}

func clientHealthCheck() *cli.Command {
	var (
		url string
	)

	return &cli.Command{
		Name:  "health",
		Usage: "Check health of swarm server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Aliases:     []string{"u"},
				EnvVars:     []string{"SWARM_SERVER_URL"},
				Usage:       "URL of swarm server",
				Destination: &url,
				Value:       "http://localhost:8080/health",
			},
		},
		Action: func(c *cli.Context) error {
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				return goerr.Wrap(err, "failed to create request")
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return goerr.Wrap(err, "failed to send request")
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return goerr.New("server is not healthy").With("status", resp.Status)
			}

			utils.Logger().Info("Server is healthy")

			return nil
		},
	}
}
