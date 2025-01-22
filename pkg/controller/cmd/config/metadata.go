package config

import (
	"log/slog"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/urfave/cli/v2"
)

type Metadata struct {
	dataset types.BQDatasetID
	table   types.BQTableID
}

func (x *Metadata) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "meta-bq-dataset-id",
			Usage:       "BigQuery dataset ID for metadata",
			EnvVars:     []string{"SWARM_META_BQ_DATASET_ID"},
			Destination: (*string)(&x.dataset),
		},
		&cli.StringFlag{
			Name:        "meta-bq-table-id",
			Usage:       "BigQuery table ID for metadata",
			EnvVars:     []string{"SWARM_META_BQ_TABLE_ID"},
			Destination: (*string)(&x.table),
		},
	}
}

func (x *Metadata) Configure() (*model.MetadataConfig, error) {
	if x.dataset == "" && x.table == "" {
		return nil, nil
	}
	if x.dataset == "" {
		return nil, goerr.Wrap(types.ErrInvalidOption, "bq-dataset is required")
	}
	if x.table == "" {
		return nil, goerr.Wrap(types.ErrInvalidOption, "bq-table is required")
	}

	return model.NewMetadataConfig(x.dataset, x.table), nil
}

func (x *Metadata) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("dataset", string(x.dataset)),
		slog.String("table", string(x.table)),
	)
}
