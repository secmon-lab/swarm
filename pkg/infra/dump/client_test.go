package dump_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra/dump"
)

func TestClient_Insert(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "dump_test")
	gt.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a test client
	client := dump.New(tmpDir)

	// Generate test data
	data := []interface{}{
		map[string]interface{}{"name": "Alice", "age": 25},
		map[string]interface{}{"name": "Bob", "age": 30},
	}

	// Insert the test data
	err = client.Insert(ctx, types.BQDatasetID("my_dataset"), types.BQTableID("my_table"), nil, data)
	gt.NoError(t, err)

	// Verify the inserted data
	fname := "my_dataset.my_table.log"
	fpath := filepath.Join(tmpDir, fname)
	fd, err := os.Open(fpath)
	gt.NoError(t, err)
	defer func() { _ = fd.Close() }()

	decoder := json.NewDecoder(fd)
	var records []interface{}
	for decoder.More() {
		var record interface{}
		err := decoder.Decode(&record)
		gt.NoError(t, err)
		records = append(records, record)
	}

	expectedRecords := []interface{}{
		map[string]interface{}{"name": "Alice", "age": float64(25)},
		map[string]interface{}{"name": "Bob", "age": float64(30)},
	}

	gt.Equal(t, records, expectedRecords)
}

func TestStream_Insert(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	client := dump.New(tmpDir)

	// The stream writes to the same file as Client.Insert.
	gt.NoError(t, client.Insert(ctx, "my_dataset", "my_table", nil, []any{map[string]any{"name": "Alice"}}))
	s := gt.R1(client.NewStream(ctx, "my_dataset", "my_table", nil)).NoError(t)
	gt.NoError(t, s.Insert(ctx, []any{map[string]any{"name": "Bob"}}))
	rows := gt.R1(s.SerializeRows([]any{map[string]any{"name": "Carol"}})).NoError(t)
	gt.NoError(t, s.InsertSerializedRows(ctx, rows))
	gt.NoError(t, s.Close())

	raw := gt.R1(os.ReadFile(filepath.Join(tmpDir, "my_dataset.my_table.log"))).NoError(t)
	gt.Equal(t, string(raw), `{"name":"Alice"}`+"\n"+`{"name":"Bob"}`+"\n"+`{"name":"Carol"}`+"\n")
}
