package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr/v2"
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

// NewStream implements interfaces.BigQuery. The stream writes data to the same file as Insert.
func (x *Client) NewStream(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema) (interfaces.BigQueryStream, error) {
	return &Stream{path: x.dataPath(datasetID, tableID)}, nil
}

// Stream writes data to a file in JSON lines format, in the same way as Client.Insert.
type Stream struct {
	path string
}

func (x *Stream) Insert(ctx context.Context, data []any) error {
	rows, err := x.SerializeRows(data)
	if err != nil {
		return err
	}
	return x.InsertSerializedRows(ctx, rows)
}

// SerializeRows converts each data to one line of JSON.
func (x *Stream) SerializeRows(data []any) ([][]byte, error) {
	return marshalLines(data)
}

func (x *Stream) InsertSerializedRows(ctx context.Context, rows [][]byte) error {
	return appendLines(x.path, rows)
}

func (x *Stream) Close() error {
	return nil
}

func (x *Client) dataPath(datasetID types.BQDatasetID, tableID types.BQTableID) string {
	return filepath.Join(x.outDir, fmt.Sprintf("%s.%s.log", datasetID, tableID))
}

func marshalLines(data []any) ([][]byte, error) {
	lines := make([][]byte, len(data))
	for i, record := range data {
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to encode record", goerr.V("record", record))
		}
		lines[i] = append(raw, '\n')
	}
	return lines, nil
}

func appendLines(fpath string, lines [][]byte) error {
	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return goerr.Wrap(err, "failed to create file", goerr.V("file", fpath))
	}

	for _, line := range lines {
		if _, err := fd.Write(line); err != nil {
			_ = fd.Close()
			return goerr.Wrap(err, "failed to write record", goerr.V("file", fpath))
		}
	}

	if err := fd.Close(); err != nil {
		return goerr.Wrap(err, "failed to close file", goerr.V("file", fpath))
	}

	return nil
}

// Insert implements interfaces.BigQuery. It writes data to a file in JSON format. The file name is "{outDir}/{dataset}.{table}.log". If the file does not exist, it creates a new file. If the file exists, it appends data to the file. The file is not uploaded to BigQuery.
func (x *Client) Insert(ctx context.Context, datasetID types.BQDatasetID, tableID types.BQTableID, schema bigquery.Schema, data []any) error {
	lines, err := marshalLines(data)
	if err != nil {
		return err
	}
	return appendLines(x.dataPath(datasetID, tableID), lines)
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
		return goerr.Wrap(err, "failed to create file", goerr.V("file", fpath))
	}

	raw, err := schema.ToJSONFields()
	if err != nil {
		_ = fd.Close()
		return goerr.Wrap(err, "failed to convert schema to JSON fields")
	}

	if _, err := fd.Write(raw); err != nil {
		_ = fd.Close()
		return goerr.Wrap(err, "failed to write schema", goerr.V("file", fpath))
	}

	if err := fd.Close(); err != nil {
		return goerr.Wrap(err, "failed to close file", goerr.V("file", fpath))
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
