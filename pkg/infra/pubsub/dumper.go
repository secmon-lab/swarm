package pubsub

import (
	"context"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type Dumper struct {
	outDir string
}

func NewDumper(outDir string) *Dumper {
	return &Dumper{outDir: outDir}
}

func (x *Dumper) Publish(ctx context.Context, data []byte) (types.PubSubMessageID, error) {
	id := types.PubSubMessageID(uuid.NewString())

	path := filepath.Clean(filepath.Join(x.outDir, string(id)+".msg"))
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}

	return id, nil
}
