package model

import "github.com/m-mizutani/swarm/pkg/domain/types"

type MetadataConfig struct {
	dataset types.BQDatasetID
	table   types.BQTableID
}

func NewMetadataConfig(dataset types.BQDatasetID, table types.BQTableID) *MetadataConfig {
	return &MetadataConfig{dataset: dataset, table: table}
}
func (x *MetadataConfig) Dataset() types.BQDatasetID { return x.dataset }
func (x *MetadataConfig) Table() types.BQTableID     { return x.table }
