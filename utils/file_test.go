package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDirSize(t *testing.T) {
	dirPath := "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database/datafileTest"
	size, err := DirSize(dirPath)
	assert.Nil(t, err)
	assert.True(t, size > 0)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
