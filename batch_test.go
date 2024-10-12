package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-writeBach")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 写数据之后并不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	key1 := utils.GetTestKey(1)
	value1 := utils.GetRandomValue(1)
	err = wb.Put(key1, value1)
	assert.Nil(t, err)
	_, err = db.Get(key1)
	assert.Equal(t, ErrKeyNotFound, err)

	// 2. 对比 db 中的 put
	key2 := utils.GetTestKey(2)
	value2 := utils.GetRandomValue(2)
	err = db.Put(key2, value2)
	assert.Nil(t, err)
	_, record2Size, err := db.activeFile.ReadLogRecord(0)
	assert.Nil(t, err)
	// t.Log(string(record2.Key)) 0bitcask-key-000000002
	// t.Log(string(record2.Type)) 0 即 nonTransactionSeqNo

	// 3. 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)
	resValue1, err := db.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, value1, resValue1)
	_, _, err = db.activeFile.ReadLogRecord(record2Size)
	assert.Nil(t, err)
	// t.Log(string(record1.Key))  1bitcask-key-000000001
	// t.Log(record1.Type) 1 = seqNo

}

func TestWriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-writeBach2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 1. 写入数据
	key1 := utils.GetTestKey(1)
	value1 := utils.GetRandomValue(1)
	err = wb.Put(key1, value1)
	assert.Nil(t, err)
	key2 := utils.GetTestKey(2)
	value2 := utils.GetRandomValue(2)
	err = wb.Put(key2, value2)
	assert.Nil(t, err)

	// 2. 提交数据
	err = wb.Commit()
	assert.Nil(t, err)
	keys := db.ListKeys()
	assert.Equal(t, 2, len(keys))
	resValue2, err := db.Get(key2)
	assert.Nil(t, err)
	assert.Equal(t, value2, resValue2)

	// 3. 删除数据，删除后查询返回 key not found
	err = wb.Delete(key2)
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	resValue3, err := db.Get(key2)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, resValue3)

}

func TestWriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-writeBach3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 通过 db 插入一条数据
	key1 := utils.GetTestKey(1)
	value1 := utils.GetRandomValue(1)
	err = db.Put(key1, value1)

	// 2. 通过 write batch 插入一条数据
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	key2 := utils.GetTestKey(2)
	value2 := utils.GetRandomValue(2)
	err = wb.Put(key2, value2)
	assert.Nil(t, err)

	// 3. 删除 db 插入的数据
	err = wb.Delete(key1)
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	_, err = db.Get(key1)
	assert.Equal(t, ErrKeyNotFound, err)

	// 4. 校验序列号： 通过 write batch 再次插入一条数据
	key3 := utils.GetTestKey(3)
	value3 := utils.GetRandomValue(3)
	err = wb.Put(key3, value3)
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), db.seqNo)

	// 5. 重启数据库
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	_, err = db2.Get(key1)
	assert.Equal(t, ErrKeyNotFound, err)  // 校验删除
	assert.Equal(t, uint64(2), db2.seqNo) // 校验事务序列号
}

func TestWriteBatch4(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-writeBach4")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wbOpts := DefaultWriteBatchOptions
	wbOpts.MaxBatchSize = 10000000
	wb := db.NewWriteBatch(wbOpts)

	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.GetTestKey(i), utils.GetRandomValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)
	assert.Equal(t, 500000, len(db.ListKeys()))

}
