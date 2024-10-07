package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_getMergePath(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put(utils.GetTestKey(1), utils.GetRandomValue(1024))

	db.Put(utils.GetTestKey(2), utils.GetRandomValue(1024))
	_, err = data.OpenHintFile(dir)
}
