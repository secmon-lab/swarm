package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr"
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

type Client struct {
	outDir string
}

// CreateTable implements interfaces.BigQuery. Nothing to do in dumper.
func (x *Client) CreateTable(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID, md *bigquery.TableMetadata) error {
	return dumpSchema(x.outDir, dataset, table, md.Schema)
}

// GetMetadata implements interfaces.BigQuery.
func (x *Client) GetMetadata(ctx context.Context, dataset types.BQDatasetID, table types.BQTableID) (*bigquery.TableMetadata, error) {
	return &bigquery.TableMetadata{}, nil
}

func (x *Client) NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
	return &Stream{}, nil
}

// TODO: Implement Stream
type Stream struct {
}

func (x *Stream) Insert(ctx context.Context, data []any) error {
	return nil
}

func (x *Stream) Close() error {
	return nil
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
	return dumpSchema(x.outDir, dataset, table, md.Schema)
}

func dumpSchema(dir string, dataset types.BQDatasetID, table types.BQTableID, schema bigquery.Schema) error {
	fname := fmt.Sprintf("%s.%s.schema.json", dataset, table)
	fpath := filepath.Join(dir, fname)
	fd, err := os.Create(fpath)
	if err != nil {
		return goerr.Wrap(err, "failed to create file").With("file", fpath)
	}
	defer fd.Close()

	raw, err := schema.ToJSONFields()
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema to JSON fields")
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
