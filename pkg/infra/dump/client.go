package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type Client struct {
	outDir string
}

// CreateTable implements interfaces.BigQuery. Nothing to do in dumper.
func (*Client) CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
	return nil
}

// GetMetadata implements interfaces.BigQuery.
func (x *Client) GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error) {
	return &bigquery.TableMetadata{}, nil
}

// Insert implements interfaces.BigQuery. It writes data to a file in JSON format. The file name is "{outDir}/{dataset}.{table}.log". If the file does not exist, it creates a new file. If the file exists, it appends data to the file. The file is not uploaded to BigQuery.
func (x *Client) Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error {
	fname := fmt.Sprintf("%s.%s.log", datasetID, tableID)
	fpath := filepath.Join(x.outDir, fname)
	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return goerr.Wrap(err, "failed to create file").With("file", fpath)
	}
	defer fd.Close()

	encoder := json.NewEncoder(fd)
	for _, record := range data {
		if err := encoder.Encode(record); err != nil {
			return goerr.Wrap(err, "failed to encode record").With("record", record)
		}
	}

	return nil
}

// Query implements interfaces.BigQuery. It is not implemented and panics if called.
func (x *Client) Query(ctx context.Context, query string) (interfaces.BigQueryIterator, error) {
	panic("unimplemented, must not be called in dumper")
}

// UpdateSchema implements interfaces.BigQuery. It writes schema to a file in JSON format. The file name is "{outDir}/{dataset}.{table}.schema.json". If the file does not exist, it creates a new file. If the file exists, it overwrites the file. The file is not uploaded to BigQuery.
func (x *Client) UpdateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md bigquery.TableMetadataToUpdate, eTag string) error {
	fname := fmt.Sprintf("%s.%s.schema.json", dataset, table)
	fpath := filepath.Join(x.outDir, fname)
	fd, err := os.Create(fpath)
	if err != nil {
		return goerr.Wrap(err, "failed to create file").With("file", fpath)
	}
	defer fd.Close()

	raw, err := md.Schema.ToJSONFields()
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema to JSON fields").With("schema", md.Schema)
	}

	if _, err := fd.Write(raw); err != nil {
		return goerr.Wrap(err, "failed to write schema").With("file", fpath)
	}

	return nil
}

// New returns a new instance of dumper Client.
func New(outDir string) *Client {
	return &Client{
		outDir: filepath.Clean(outDir),
	}
}

var _ interfaces.BigQuery = &Client{}
