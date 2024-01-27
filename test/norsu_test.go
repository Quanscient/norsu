package test

import (
	"testing"

	"github.com/koskimas/norsu/internal/cmd"
	assert "github.com/stretchr/testify/require"
)

func TestNorsu(t *testing.T) {
	err := cmd.Run(cmd.Settings{
		WorkingDir: getWd(t, "tests/00001_simple"),
	})

	assert.NoError(t, err)
}
