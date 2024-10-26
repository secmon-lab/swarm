package config_test

import (
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/controller/cmd/config"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/urfave/cli/v2"
)

func Test(t *testing.T) {
	meta := &config.Metadata{}

	testCases := map[string]struct {
		args    []string
		meta    *model.MetadataConfig
		wantErr bool
	}{
		"no args": {
			args:    []string{},
			meta:    nil,
			wantErr: false,
		},
		"with args": {
			args:    []string{"--meta-bq-dataset-id", "test-dataset", "--meta-bq-table-id", "test-table"},
			meta:    model.NewMetadataConfig("test-dataset", "test-table"),
			wantErr: false,
		},
		"missing dataset": {
			args:    []string{"--meta-bq-table-id", "test-table"},
			meta:    nil,
			wantErr: true,
		},
		"missing table": {
			args:    []string{"--meta-bq-dataset-id", "test-dataset"},
			meta:    nil,
			wantErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			app := cli.App{
				Name:  "test",
				Flags: meta.Flags(),
				Action: func(c *cli.Context) error {
					md, err := meta.Configure()
					if tc.wantErr {
						gt.Error(t, err)
					} else {
						gt.NoError(t, err)
						gt.Equal(t, md, tc.meta)
					}
					return nil
				},
			}

			gt.NoError(t, app.Run(append([]string{"cmd"}, tc.args...)))
		})
	}
}
