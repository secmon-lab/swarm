package config

import (
	"github.com/getsentry/sentry-go"
	"github.com/m-mizutani/swarm/pkg/utils"
	"github.com/urfave/cli/v2"
)

type Sentry struct {
	dsn string
	env string
}

func (x *Sentry) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "sentry-dsn",
			Usage:       "Sentry DSN for error reporting",
			EnvVars:     []string{"SWARM_SENTRY_DSN"},
			Destination: &x.dsn,
		},
		&cli.StringFlag{
			Name:        "sentry-env",
			Usage:       "Sentry environment",
			EnvVars:     []string{"SWARM_SENTRY_ENV"},
			Destination: &x.env,
		},
	}
}

func (x *Sentry) Configure() error {
	if x.dsn != "" {
		utils.Logger().Info("Enable Sentry", "DSN", x.dsn, "env", x.env)
		sentry.Init(sentry.ClientOptions{
			Dsn:         x.dsn,
			Environment: x.env,
		})
	} else {
		utils.Logger().Warn("sentry is not enabled")
	}

	return nil
}
