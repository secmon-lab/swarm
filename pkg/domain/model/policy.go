package model

import (
	"github.com/m-mizutani/goerr/v2"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type AuthPolicyInput struct {
	Method string              `json:"method"`
	Path   string              `json:"path"`
	Remote string              `json:"remote"`
	Query  map[string][]string `json:"query"`
	Header map[string][]string `json:"header"`
	Body   string              `json:"body"`
}

type AuthPolicyOutput struct {
	Deny bool `json:"deny"`
}

type EventPolicyOutput struct {
	Sources []*Source `json:"src"`
}

type Source struct {
	// Source object information
	Parser   types.ObjectParser   `json:"parser" bigquery:"parser"`
	Schema   types.ObjectSchema   `json:"schema" bigquery:"schema"`
	Compress types.ObjectCompress `json:"compress" bigquery:"compress"`
}

func (x Source) Validate() error {
	switch x.Parser {
	case types.JSONParser:
		// OK
	default:
		return goerr.Wrap(types.ErrInvalidPolicyResult, "src.format is invalid", goerr.V("format", x.Parser))
	}

	if x.Schema == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "src.record is required")
	}

	switch x.Compress {
	case types.GZIPComp, "":
		// OK
	default:
		return goerr.Wrap(types.ErrInvalidPolicyResult, "src.comp is invalid", goerr.V("comp", x.Compress))
	}

	return nil
}

type SchemaPolicyOutput struct {
	Logs []*Log `json:"log"`
}

type BigQueryDest struct {
	Dataset   types.BQDatasetID `json:"dataset"`
	Table     types.BQTableID   `json:"table"`
	Partition types.BQPartition `json:"partition"`
}

type Log struct {
	// Destination BigQuery table information
	BigQueryDest

	ID        types.LogID    `json:"id"`
	Timestamp float64        `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

func (x *Log) Validate() error {
	if x.Dataset == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.dataset is required")
	}
	if x.Table == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.table is required")
	}

	if x.Timestamp == 0 {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.timestamp is required, or must be more than 0")
	}
	if x.Data == nil {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.data is required")
	}

	return nil
}
