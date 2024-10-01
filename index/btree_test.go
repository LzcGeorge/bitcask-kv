package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	btree := NewBtree()
	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2 := btree.Put([]byte("aa"), &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	btree := NewBtree()

	// 插入一个key为nil的元素
	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 20, Offset: 2})
	assert.True(t, res1)

	pos1 := btree.Get(nil)
	assert.Equal(t, uint32(20), pos1.Fid)
	assert.Equal(t, int64(2), pos1.Offset)

	// 插入一个key为aa的元素
	res2 := btree.Put([]byte("aa"), &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res2)
	// 插入相同的key,修改 地址
	res3 := btree.Put([]byte("aa"), &data.LogRecordPos{Fid: 2, Offset: 22})
	assert.True(t, res3)

	pos2 := btree.Get([]byte("aa"))
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(22), pos2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	btree := NewBtree()
	// 删除一个 key 为 nil 的元素
	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.True(t, res1)
	res2 := btree.Delete(nil)
	assert.True(t, res2)

	// 删除一个 key 为 asd 的元素
	res3 := btree.Put([]byte("asd"), &data.LogRecordPos{Fid: 2, Offset: 201})
	assert.True(t, res3)
	res4 := btree.Delete([]byte("asd"))
	assert.True(t, res4)
}

func TestBtree_Iterator(t *testing.T) {
	btree := NewBtree()

	// 1. btree 为空的情况
	iter1 := btree.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2. btree 中有一条数据的情况
	res1 := btree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.True(t, res1)
	iter2 := btree.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.EqualValues(t, []byte("aa"), iter2.Key())
	assert.NotNil(t, iter2.Value)
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3. btree 中有多条数据的情况, 且包含重复 key
	btree.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 22})
	btree.Put([]byte("cc"), &data.LogRecordPos{Fid: 3, Offset: 33})
	btree.Put([]byte("cc"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter3 := btree.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())

	}

	iter3 = btree.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())
	}

	// 4. 测试 seek
	iter4 := btree.Iterator(false)
	iter4.Seek([]byte("bc"))
	assert.Equal(t, true, iter4.Valid())
	assert.EqualValues(t, []byte("cc"), iter4.Key())
	assert.Equal(t, &data.LogRecordPos{4, 44}, iter4.Value())

	// 5. 测试反向 seek
	iter5 := btree.Iterator(true)
	iter5.Seek([]byte("cb"))
	assert.Equal(t, true, iter5.Valid())
	assert.EqualValues(t, []byte("bb"), iter5.Key())
	assert.Equal(t, &data.LogRecordPos{2, 22}, iter5.Value())

}
