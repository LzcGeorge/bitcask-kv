package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

const DataBasePath = "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database"

func destoryFile(fileName string) {
	if err := os.RemoveAll(fileName); err != nil {
		panic(err)
	}
}
func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join(DataBasePath, "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join(DataBasePath, "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join(DataBasePath, "b.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key_a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key_b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "key_a", string(b))
	//t.Log(string(b), n)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "key_b", string(b2))

}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join(DataBasePath, "b.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join(DataBasePath, "b.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)

}
