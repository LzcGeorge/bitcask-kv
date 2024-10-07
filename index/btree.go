package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	return NewBtreeIterator(bt.tree, reverse)
}

func (bt *Btree) Size() int {
	return bt.tree.Len()
}
func (bt *Btree) Close() error {
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	currentIndex int     // 当前索引
	reverse      bool    // 是否是反向遍历
	values       []*Item // key 和 位置索引
}

func NewBtreeIterator(bt *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, bt.Len())

	// 遍历 btree, 将数据保存到 values 数组中
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		bt.Descend(saveValues)
	} else {
		bt.Ascend(saveValues)
	}

	return &btreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}
func (bti *btreeIterator) Rewind() {
	bti.currentIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.currentIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.currentIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currentIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currentIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
