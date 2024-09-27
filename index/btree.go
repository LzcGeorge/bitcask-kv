package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBtree 创建一个Btree
func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}
func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}
	// 读 不用加锁
	btreeItem := bt.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
