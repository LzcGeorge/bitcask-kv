package bitcask_go

import (
	"bitcask-go/utils"
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destoryDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-open")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 正常 put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.GetRandomValue(10))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.GetRandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.GetRandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.GetRandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 正常读取一条数据
	key1 := utils.GetTestKey(1)
	val1 := utils.GetRandomValue(10)
	err = db.Put(key1, val1)
	assert.Nil(t, err)
	resVal1, err := db.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, val1, resVal1)

	// 2. 读取一个不存在的 key
	val2, err := db.Get([]byte("unExistedKey"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val2)

	// 3. 修改 key 的值，再读取
	key3 := utils.GetTestKey(3)
	val3 := utils.GetRandomValue(24)
	err = db.Put(key3, val3)
	assert.Nil(t, err)
	val4 := utils.GetRandomValue(10)
	err = db.Put(key3, val4)
	assert.Nil(t, err)
	resVal4, err := db.Get(key3)
	assert.Nil(t, err)
	assert.Equal(t, val4, resVal4)

	// 4. 值被删除后再读取
	key4 := utils.GetTestKey(4)
	val5 := utils.GetRandomValue(24)
	err = db.Put(key4, val5)
	assert.Nil(t, err)
	err = db.Delete(key4)
	assert.Nil(t, err)
	resKey4Val, err := db.Get(key4)
	assert.Equal(t, 0, len(resKey4Val))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val6, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val6)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val7, err := db2.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val1, val7)

	val8, err := db2.Get(utils.GetTestKey(3))
	assert.Nil(t, err)
	assert.Equal(t, val4, val8)

	val9, err := db2.Get(utils.GetTestKey(4))
	assert.Equal(t, 0, len(val9))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 正常删除一个存在的 key
	key1 := utils.GetTestKey(1)
	err = db.Put(key1, utils.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Delete(key1)
	assert.Nil(t, err)
	_, err = db.Get(key1)
	assert.Equal(t, ErrKeyNotFound, err)

	// 2. 删除一个不存在的 key
	err = db.Delete([]byte("unExistedKey"))
	assert.Nil(t, err)

	// 3. 删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4. 值被删除一个重新put
	key4 := utils.GetTestKey(4)
	val4 := utils.GetRandomValue(24)
	err = db.Put(key4, val4)
	err = db.Delete(key4)
	assert.Nil(t, err)
	newVal4 := utils.GetRandomValue(10)
	err = db.Put(key4, newVal4)
	assert.Nil(t, err)
	resVal4, err := db.Get(key4)
	assert.Nil(t, err)
	assert.Equal(t, newVal4, resVal4)

	// 5.  重启数据库，在进行校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	getKey4, err := db2.Get(key4)
	assert.Nil(t, err)
	assert.Equal(t, newVal4, getKey4)

	getKey1, err := db2.Get(key1)
	assert.Equal(t, 0, len(getKey1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestExample(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-example")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 查看覆盖的数据类型
	key1 := utils.GetTestKey(1)
	val1 := utils.GetRandomValue(10)
	err = db.Put(key1, val1)
	assert.Nil(t, err)
	record, recordSize, err := db.activeFile.ReadLogRecord(0)
	println("覆盖前：", string(record.Key), string(record.Value), record.Type)
	// err = db.Put(key1, utils.GetRandomValue(24))
	// assert.Nil(t, err)
	err = db.Delete(key1)
	assert.Nil(t, err)
	deletedRecord, _, err := db.activeFile.ReadLogRecord(recordSize)
	println("覆盖后：", string(deletedRecord.Key), string(deletedRecord.Value), deletedRecord.Type)

	// 结论：删除只是追加一条日志记录，而并不是改之前的 record.type 为删除类型
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-listKeys")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 数据库为空时
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// 2. 一条数据
	key1 := utils.GetTestKey(1)
	err = db.Put(key1, utils.GetRandomValue(1))
	assert.Nil(t, err)
	keys = db.ListKeys()
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, key1, keys[0])

	// 3. 多条数据
	key2 := utils.GetTestKey(2)
	err = db.Put(key2, utils.GetRandomValue(2))
	key3 := utils.GetTestKey(3)
	err = db.Put(key3, utils.GetRandomValue(3))
	assert.Nil(t, err)
	keys = db.ListKeys()
	assert.Equal(t, 3, len(keys))
	for _, key := range keys {
		assert.NotNil(t, key)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-fold")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	key1 := utils.GetTestKey(1)
	err = db.Put(key1, utils.GetRandomValue(1))
	assert.Nil(t, err)
	key2 := utils.GetTestKey(2)
	err = db.Put(key2, utils.GetRandomValue(2))
	key3 := utils.GetTestKey(3)
	err = db.Put(key3, utils.GetRandomValue(3))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		if bytes.Compare(key, utils.GetTestKey(2)) == 0 {
			return false
		}
		return true
	})
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// destoryDB 中会调用 Close() 方法
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	key1 := utils.GetTestKey(1)
	err = db.Put(key1, utils.GetRandomValue(1))
	assert.Nil(t, err)

}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-sync")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	key1 := utils.GetTestKey(1)
	err = db.Put(key1, utils.GetRandomValue(1))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-filelock")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	assert.Equal(t, ErrDatabaseIsUsing, err)

	db.Close()

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	err = db2.Close()
	assert.Nil(t, err)

}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-stat")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	putSize1, deleteSize1 := 50000, 1000
	for i := 0; i < putSize1; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}
	stat := db.Stat()
	assert.Equal(t, uint(putSize1), stat.KeyNum)
	assert.Equal(t, int64(0), stat.ReclaimableSize)
	assert.Greater(t, stat.DataFileNum, uint(0))
	for i := 0; i < deleteSize1; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	stat = db.Stat()
	assert.Equal(t, uint(putSize1-deleteSize1), stat.KeyNum)

	putSize2, deleteSize2 := 10, 20
	for i := deleteSize1; i < deleteSize1+deleteSize2; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	stat = db.Stat()
	assert.Equal(t, uint(putSize1-deleteSize1-deleteSize2), stat.KeyNum)

	for i := putSize1; i < putSize1+putSize2; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}
	stat = db.Stat()
	assert.Equal(t, uint(putSize1-deleteSize1-deleteSize2+putSize2), stat.KeyNum)

}

func TestDB_Backup(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-backup")
	destDir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-backup-dest")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Backup(destDir)
	assert.Nil(t, err)
	err = db.Close()
	assert.Nil(t, err)

	opts2 := DefaultOptions
	opts2.DirPath = destDir
	db2, err := Open(opts2)
	defer destoryDB(db2)
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))
}

//func TestDB_OpenMMap(t *testing.T) {
//	opts := DefaultOptions
//	opts.DirPath = "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database/bitcask-go-writeBach33476478020"
//	opts.MMapAtStartup = false
//
//	now := time.Now()
//	db, err := Open(opts)
//	assert.Nil(t, err)
//	fmt.Println("io reader: ", time.Now().Sub(now))
//	defer destoryDB(db)
//
//	opts.MMapAtStartup = true
//	now = time.Now()
//	db2, err := Open(opts)
//	fmt.Println("mmap reader: ", time.Now().Sub(now))
//	destoryDB(db2)
//}
