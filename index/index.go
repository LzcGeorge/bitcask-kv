package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 通用索引接口
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTree 索引
	BTree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	case ART:
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	return bytes.Compare(item.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 倒回到迭代器的起点，即第一个迭代器
	Rewind()

	// Seek 找到第一个大于等于（或小于等于）key 的数据
	Seek(key []byte)

	// Next 移动到下一个数据
	Next()

	// Valid 判断当前迭代器是否有效, 即是否已经到了迭代器的末尾
	Valid() bool

	// Key 获取当前迭代器的 key
	Key() []byte

	// Value 获取当前迭代器的 value 所在的位置索引
	Value() *data.LogRecordPos

	// Close 关闭迭代器
	Close()
}
