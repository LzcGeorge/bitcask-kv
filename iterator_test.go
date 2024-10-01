package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 初始化迭代器
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, false, iter.Valid())

}

func TestIterator_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-iterator-value")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	key := utils.GetTestKey(1)
	value := utils.GetRandomValue(10)
	err = db.Put(key, value)
	assert.Nil(t, err)

	// 初始化迭代器
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	resValue, err := iter.Value()
	assert.Nil(t, err)
	assert.EqualValues(t, value, resValue)
	assert.EqualValues(t, key, iter.Key())
	iter.Next()
	assert.Equal(t, false, iter.Valid())

	// 测试 prefix
	key2 := []byte("abc")
	value2 := []byte("123")
	_ = db.Put(key2, value2)
	iteratorOpts := DefaultIteratorOptions
	iteratorOpts.Prefix = []byte("a")
	iter2 := db.NewIterator(iteratorOpts)
	assert.Equal(t, true, iter2.Valid())
	assert.EqualValues(t, key2, iter2.Key())
	resValue2, err := iter2.Value()
	assert.Nil(t, err)
	assert.EqualValues(t, value2, resValue2)
}

func TestIterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(opts.DirPath, "bitcask-go-iterator-multi-value")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), utils.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), utils.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), utils.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), utils.GetRandomValue(10))
	assert.Nil(t, err)

	// prefix = "a"的反向迭代
	iteratorOpts := DefaultIteratorOptions
	iteratorOpts.Reverse = true
	iteratorOpts.Prefix = []byte("a")
	iter := db.NewIterator(iteratorOpts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		// t.Log("key = ", string(iter.Key()))
		assert.NotNil(t, iter.Key())
	}

	// seek 测试
	iteratorOpts.Prefix = []byte("")
	iteratorOpts.Reverse = true
	iter2 := db.NewIterator(iteratorOpts)
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}

}
