package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
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

// 没有任何数据的情况下进行 merge
func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// 全都是有效数据的测试
func TestDB_Merge_With_Data(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 0; i < 10000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 有删除的数据，和被重复 put 的数据
func TestDB_Merge_With_Delete_And_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 30000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 10000; i < 30000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("newPut"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	keys := db2.ListKeys()
	assert.Equal(t, 20000, len(keys))

	// 验证删除的数据
	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}

	// 验证被重复 put 的数据
	for i := 10000; i < 30000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, "newPut", string(val))
	}
}

// 测试全是 delete 的场景
func TestDB_Merge_With_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge4")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 30000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 30000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))

}

// Merge 的过程中有新的数据写入或删除
func TestDB_Merge_With_New_Data(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-merge6")
	opts.DirPath = dir
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	//重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

	//keys := db2.ListKeys()
	//assert.Equal(t, 2000, len(keys))

	//for i := 5000; i < 7000; i++ {
	//	val, err := db2.Get(utils.GetTestKey(i))
	//	t.Log(i)
	//	assert.Nil(t, err)
	//	assert.NotNil(t, val)
	//}
}
