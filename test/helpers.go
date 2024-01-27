package test

import (
	"os"
	"path/filepath"
	"testing"

	assert "github.com/stretchr/testify/require"
)

func getWd(t *testing.T, folder string) string {
	wd, err := os.Getwd()
	assert.NoError(t, err, "failed to get working directory")
	return filepath.Join(wd, folder)
}
