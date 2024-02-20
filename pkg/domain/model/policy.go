package model

import (
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/types"
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

type Stream struct {
	// Source object information
	Format types.ObjectFormat `json:"format"`
	Schema types.ObjectSchema `json:"schema"`
	Comp   types.ObjectComp   `json:"comp"`

	// Destination BigQuery table information
	Dataset types.BQDatasetID `json:"dataset"`
	Table   types.BQTableID   `json:"table"`
}

func (x Stream) Validate() error {
	if x.Dataset == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "stream.dataset is required")
	}
	if x.Table == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "stream.table is required")
	}

	switch x.Format {
	case types.JSONFormat:
		// OK
	default:
		return goerr.Wrap(types.ErrInvalidPolicyResult, "stream.format is invalid").With("format", x.Format)
	}

	if x.Schema == "" {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "stream.schema is required")
	}

	switch x.Comp {
	case types.GZIPComp, "":
		// OK
	default:
		return goerr.Wrap(types.ErrInvalidPolicyResult, "stream.comp is invalid").With("comp", x.Comp)
	}

	return nil
}

type PipelinePolicyOutput struct {
	Streams []Stream `json:"stream"`
}

type SchemaPolicyOutput struct {
	Logs []*LogOutput `json:"logs"`
}

type LogOutput struct {
	ID        types.LogID    `json:"id"`
	Timestamp float64        `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

func (x *LogOutput) Validate() error {
	if x.Timestamp == 0 {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.timestamp is required, or must be more than 0")
	}
	if x.Data == nil {
		return goerr.Wrap(types.ErrInvalidPolicyResult, "log.data is required")
	}

	return nil
}
