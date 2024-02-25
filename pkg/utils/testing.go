package utils

import (
	"os"
	"testing"
)

func LoadEnv(t *testing.T, key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		t.Skipf("Skip test because %s is not set", key)
	}

	return v
}
