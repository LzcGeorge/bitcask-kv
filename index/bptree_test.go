package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const dirPath = "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database/"

func TestBPlusTree_Put(t *testing.T) {
	bptree := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bptree.tree.Path()
		os.Remove(filePath)
	}()
	res1 := bptree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res1)
	assert.Equal(t, 1, bptree.Size())
	res2 := bptree.Put([]byte("bbb"), &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.Nil(t, res2)
	assert.Equal(t, 2, bptree.Size())

}

func TestBPlusTree_Get(t *testing.T) {
	bptree := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bptree.tree.Path()
		os.Remove(filePath)
	}()

	// 1. 测试不存在的key
	pos1 := bptree.Get([]byte("bbb"))
	assert.Nil(t, pos1)

	// 2. 插入一个key为 hello 的元素
	res2 := bptree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res2)
	pos2 := bptree.Get([]byte("hello"))
	assert.NotNil(t, pos2)
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, pos2)

	// 3. 修改 hello 的 pos
	res3 := bptree.Put([]byte("hello"), &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, res3) // 返回旧值
	pos3 := bptree.Get([]byte("hello"))
	assert.NotNil(t, pos3)
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 2}, pos3)

}

func TestBPlusTree_Delete(t *testing.T) {
	bptree := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bptree.tree.Path()
		os.Remove(filePath)
	}()

	// 1. 插入一个key为 hello 的元素
	res1 := bptree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res1)

	// 2. 删除一个不存在的key
	pos, res2 := bptree.Delete([]byte("bbb"))
	assert.False(t, res2)
	assert.Nil(t, pos)

	// 3. 删除一个存在的key
	recordPos, res3 := bptree.Delete([]byte("hello"))
	assert.True(t, res3)
	assert.Equal(t, 0, bptree.Size())
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, recordPos)

}

func TestBPlusTree_Size(t *testing.T) {
	bptree := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bptree.tree.Path()
		os.Remove(filePath)
	}()

	bptree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 1})
	bptree.Put([]byte("world"), &data.LogRecordPos{Fid: 2, Offset: 2})
	bptree.Put([]byte("bitcask"), &data.LogRecordPos{Fid: 3, Offset: 3})
	assert.Equal(t, 3, bptree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	bptree := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bptree.tree.Path()
		os.Remove(filePath)
	}()

	// 1. bptree 为空的情况
	iter1 := bptree.Iterator(false)
	assert.Equal(t, false, iter1.Valid())
	iter1.Close()

	// 2. bptree 中有一条数据的情况
	res1 := bptree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.Nil(t, res1)
	iter2 := bptree.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.EqualValues(t, []byte("aa"), iter2.Key())
	assert.NotNil(t, iter2.Value)
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	iter2.Close()

	// 3. bptree 中有多条数据的情况, 且包含重复 key
	bptree.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 22})
	bptree.Put([]byte("cc"), &data.LogRecordPos{Fid: 3, Offset: 33})
	bptree.Put([]byte("cc"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter3 := bptree.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())

	}

	iter3 = bptree.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, true, iter3.Valid())
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()

	// 4. 测试 seek
	iter4 := bptree.Iterator(false)
	iter4.Seek([]byte("bc"))
	assert.Equal(t, true, iter4.Valid())
	assert.EqualValues(t, []byte("cc"), iter4.Key())
	assert.Equal(t, &data.LogRecordPos{4, 44, 0}, iter4.Value())
	iter4.Close()

	// 5. 测试反向 seek
	iter5 := bptree.Iterator(true)

	iter5.Seek([]byte("cb"))
	assert.Equal(t, true, iter5.Valid())
	assert.EqualValues(t, []byte("cc"), iter5.Key())
	assert.Equal(t, &data.LogRecordPos{4, 44, 0}, iter5.Value())
	iter5.Close()
}

// 这个和 btree 中的反向不太一样，需要注意下
func TestReverseSeek(t *testing.T) {
	bpti := NewBPlusTree(dirPath, false)
	defer func() {
		filePath := bpti.tree.Path()
		os.Remove(filePath)
	}()

	bpti.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 11})
	bpti.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 22})
	bpti.Put([]byte("cc"), &data.LogRecordPos{Fid: 3, Offset: 33})
	bpti.Put([]byte("dd"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter := bpti.Iterator(true)

	iter.Seek([]byte("cb"))
	assert.EqualValues(t, []byte("cc"), iter.Key())
}
