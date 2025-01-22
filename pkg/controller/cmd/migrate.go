package cmd

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/usecase"
	"github.com/urfave/cli/v2"
)

func migrateCommand() *cli.Command {
	var (
		srcID     string
		dstID     string
		partition types.BQPartition
		query     string
	)

	const (
		defaultQuery = "INSERT `{{.dst}}` SELECT * FROM `{{.src}}`"
	)

	return &cli.Command{
		Name:    "migrate",
		Aliases: []string{"m"},
		Usage:   "Copy schema and data from source table to destination table",
		Flags: mergeFlags([]cli.Flag{
			&cli.StringFlag{
				Name:        "src",
				Aliases:     []string{"s"},
				Usage:       "Source table ID (<project>.<dataset>.<table>)",
				EnvVars:     []string{"SWARM_MIGRATE_SRC"},
				Destination: &srcID,
				Required:    true,
			},

			&cli.StringFlag{
				Name:        "dst",
				Aliases:     []string{"d"},
				Usage:       "Destination table ID (<project>.<dataset>.<table>)",
				EnvVars:     []string{"SWARM_MIGRATE_DST"},
				Destination: &dstID,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "partition-type",
				Aliases:     []string{"p"},
				Usage:       "Time partition type of destination table [hour|day|month|year]",
				EnvVars:     []string{"SWARM_MIGRATE_PARTITION_TYPE"},
				Destination: (*string)(&partition),
			},
			&cli.StringFlag{
				Name:        "query",
				Aliases:     []string{"q"},
				Usage:       "Query to copy data",
				EnvVars:     []string{"SWARM_QUERY"},
				Destination: &query,
				Value:       defaultQuery,
			},
		}),

		Action: func(c *cli.Context) error {
			tmpl, err := template.New("query").Parse(query)
			if err != nil {
				return goerr.Wrap(err, "failed to parse query template")
			}
			args := map[string]any{
				"src": srcID,
				"dst": dstID,
			}
			var q bytes.Buffer
			if err := tmpl.Execute(&q, args); err != nil {
				return goerr.Wrap(err, "failed to render query template")
			}

			srcTable, err := parseBigQueryTableID(srcID)
			if err != nil {
				return err
			}
			dstTable, err := parseBigQueryTableID(dstID)
			if err != nil {
				return err
			}

			if srcTable.Project != dstTable.Project {
				return goerr.Wrap(types.ErrInvalidOption, "source and destination table must be in the same project")
			}

			bqClient, err := bq.New(c.Context, srcTable.Project)
			if err != nil {
				return err
			}
			clients := infra.New(
				infra.WithBigQuery(bqClient),
			)
			uc := usecase.New(clients)

			src := &model.BigQueryDest{
				Dataset: srcTable.DatasetID,
				Table:   srcTable.TableID,
			}
			dst := &model.BigQueryDest{
				Dataset:   dstTable.DatasetID,
				Table:     dstTable.TableID,
				Partition: partition,
			}

			return uc.Migrate(c.Context, src, dst, q.String())
		},
	}
}

type bqTable struct {
	Project   types.GoogleProjectID
	DatasetID types.BQDatasetID
	TableID   types.BQTableID
}

func parseBigQueryTableID(id string) (*bqTable, error) {
	parts := strings.Split(id, ".")
	if len(parts) != 3 {
		return nil, goerr.Wrap(types.ErrInvalidOption, "invalid table ID", goerr.V("id", id))
	}

	return &bqTable{
		Project:   types.GoogleProjectID(parts[0]),
		DatasetID: types.BQDatasetID(parts[1]),
		TableID:   types.BQTableID(parts[2]),
	}, nil
}
