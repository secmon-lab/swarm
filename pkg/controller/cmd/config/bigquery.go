package config

import (
	"context"

	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/bq"
	"github.com/urfave/cli/v2"
)

type BigQuery struct {
	projectID string
}

func (x *BigQuery) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "bigquery-project-id",
			Usage:       "Google Cloud project ID for BigQuery",
			EnvVars:     []string{"SWARM_BIGQUERY_PROJECT_ID"},
			Destination: &x.projectID,
		},
	}
}

func (x *BigQuery) Configure(ctx context.Context) (*bq.Client, error) {
	if x.projectID == "" {
		return nil, goerr.Wrap(types.ErrInvalidOption, "bigquery-project-id is required")
	}

	return bq.New(ctx, x.projectID)
}
