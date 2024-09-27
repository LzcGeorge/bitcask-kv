package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDateFile(t *testing.T) {

	file, err := OpenDateFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	file2, err := OpenDateFile(os.TempDir(), 22)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	// 重复打开同一个文件
	file3, err := OpenDateFile(os.TempDir(), 22)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	file, err := OpenDateFile(os.TempDir(), 9)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)
	err = file.Write([]byte(" selfknow\n"))
	assert.Nil(t, err)

	err = file.Write([]byte("hh\n"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file, err := OpenDateFile(os.TempDir(), 113)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDateFile(os.TempDir(), 123)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)
}
