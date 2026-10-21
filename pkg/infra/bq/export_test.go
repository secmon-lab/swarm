package bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/secmon-lab/swarm/pkg/infra/bq/writer"
)

const MaxAppendFailures = maxAppendFailures

// NewStreamWithOpener returns Stream that serializes rows with schema and sends them to RowAppender opened by open.
func NewStreamWithOpener(ctx context.Context, schema bigquery.Schema, open writer.OpenFunc) (*Stream, error) {
	md, _, err := schemaToDescriptor(schema)
	if err != nil {
		return nil, err
	}
	return newStreamWithOpener(ctx, md, open)
}
