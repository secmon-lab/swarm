package dump_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/dump"
)

func TestClient_Insert(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "dump_test")
	gt.NoError(t, err)
	defer os.RemoveAll(tmpDir)

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
	defer fd.Close()

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
