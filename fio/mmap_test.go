package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database", "mmap-a.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)

	mmapIO, err := NewFileIOManager(path)
	assert.Nil(t, err)

	defer destoryFile(path)
	assert.NotNil(t, fio)
	assert.NotNil(t, mmapIO)

	// 1. 文件为空
	n1, err := mmapIO.Read(make([]byte, 5), 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	n2, err := fio.Write([]byte("key_a"))
	assert.Nil(t, err)

	n3, err := fio.Write([]byte("key_basdasd"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := mmapIO.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "key_a", string(b))

	b2 := make([]byte, n3)
	n, err = mmapIO.Read(b2, int64(n2))
	assert.Nil(t, err)
	assert.Equal(t, n3, n)
	assert.Equal(t, "key_basdasd", string(b2))

	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(n2+n3), size)
}
