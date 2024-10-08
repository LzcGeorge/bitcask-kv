package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("hello2"), &data.LogRecordPos{Fid: 2, Offset: 2})
	art.Put([]byte("hello3"), &data.LogRecordPos{Fid: 3, Offset: 3})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	pos := art.Get([]byte("hello"))
	assert.NotNil(t, pos)

	// 测试不存在的key
	pos = art.Get([]byte("hello2"))
	assert.Nil(t, pos)

	// 重复的key，改变 pos
	art.Put([]byte("hello"), &data.LogRecordPos{Fid: 2, Offset: 2})
	pos = art.Get([]byte("hello"))
	assert.NotNil(t, pos)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	// 删除不存在的key
	res1 := art.Delete([]byte("notExist"))
	assert.False(t, res1)

	// 删除存在的key
	art.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	res2 := art.Delete([]byte("hello"))
	assert.True(t, res2)
	assert.Nil(t, art.Get([]byte("hello")))
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Equal(t, 1, art.Size())
	art.Put([]byte("hello2"), &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.Equal(t, 2, art.Size())
	art.Delete([]byte("hello"))
	assert.Equal(t, 1, art.Size())

}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	// 1. art 为空的情况
	iter1 := art.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2. art 中有一条数据的情况
	res1 := art.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.True(t, res1)
	iter2 := art.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.EqualValues(t, []byte("aa"), iter2.Key())
	assert.NotNil(t, iter2.Value)
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3. art 中有多条数据的情况, 且包含重复 key
	art.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 22})
	art.Put([]byte("cc"), &data.LogRecordPos{Fid: 3, Offset: 33})
	art.Put([]byte("cc"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter3 := art.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())

	}

	iter3 = art.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())
	}

	// 4. 测试 seek
	iter4 := art.Iterator(false)
	iter4.Seek([]byte("bc"))
	assert.Equal(t, true, iter4.Valid())
	assert.EqualValues(t, []byte("cc"), iter4.Key())
	assert.Equal(t, &data.LogRecordPos{4, 44}, iter4.Value())

	// 5. 测试反向 seek
	iter5 := art.Iterator(true)
	iter5.Seek([]byte("cb"))
	assert.Equal(t, true, iter5.Valid())
	assert.EqualValues(t, []byte("bb"), iter5.Key())
	assert.Equal(t, &data.LogRecordPos{2, 22}, iter5.Value())
}
