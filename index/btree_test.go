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
