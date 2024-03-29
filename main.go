package main

import (
	"os"

	"github.com/m-mizutani/swarm/pkg/controller/cmd"
)

// main is the entry point of the program
func main() {
	if err := cmd.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
