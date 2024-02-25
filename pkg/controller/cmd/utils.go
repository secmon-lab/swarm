package cmd

import "github.com/urfave/cli/v2"

func mergeFlags(flags ...[]cli.Flag) []cli.Flag {
	var merged []cli.Flag
	for _, f := range flags {
		merged = append(merged, f...)
	}
	return merged
}
