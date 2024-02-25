package cmd_test

import (
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/controller/cmd"
)

func TestFlags(t *testing.T) {
	// Detecting flags conflicts
	testCases := []struct {
		subCommand string
	}{
		{"ingest"},
		{"serve"},
		{"retry"},
		{"client"},
	}

	for _, tc := range testCases {
		t.Run(tc.subCommand, func(t *testing.T) {
			gt.NoError(t, cmd.Run([]string{"swarm", tc.subCommand, "--help"}))
		})
	}
}
