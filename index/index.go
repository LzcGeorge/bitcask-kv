package index

import (
	"bitcast-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	return bytes.Compare(item.key, bi.(*Item).key) == -1
}
