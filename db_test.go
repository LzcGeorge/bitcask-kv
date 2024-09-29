package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destoryDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-test")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-test")
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
	err = db.activeFile.Close()
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
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-test")
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
	err = db.activeFile.Close()
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
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-test")
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
	err = db.activeFile.Close()
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
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-test")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 查看覆盖的数据类型
	key1 := utils.GetTestKey(1)
	val1 := utils.GetRandomValue(10)
	err = db.Put(key1, val1)
	assert.Nil(t, err)
	record, recordSize, err := db.activeFile.ReadLogRecord(0)
	println("覆盖前：", string(record.Key), string(record.Value), record.Type)
	//err = db.Put(key1, utils.GetRandomValue(24))
	// assert.Nil(t, err)
	err = db.Delete(key1)
	assert.Nil(t, err)
	deletedRecord, _, err := db.activeFile.ReadLogRecord(recordSize)
	println("覆盖后：", string(deletedRecord.Key), string(deletedRecord.Value), deletedRecord.Type)

	// 结论：删除只是追加一条日志记录，而并不是改之前的 record.type 为删除类型
}
