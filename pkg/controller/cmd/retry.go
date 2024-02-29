package cmd

import "github.com/urfave/cli/v2"

func retryCommand() *cli.Command {
	return &cli.Command{
		Name:    "retry",
		Aliases: []string{"r"},
		Usage:   "Retry data injection for failed logs",
		Action: func(c *cli.Context) error {
			return nil
		},
	}
}
