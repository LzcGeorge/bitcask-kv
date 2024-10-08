package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"
)

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

// AdaptiveRadixTree 自适应基数树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 创建一个空的自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art.tree, reverse)
}

// artIterator Art 索引迭代器
type artIterator struct {
	currentIndex int     // 当前索引
	reverse      bool    // 是否是反向遍历
	values       []*Item // key 和 位置索引
}

// NewARTIterator 创建一个 art 索引迭代器
func NewARTIterator(art goart.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, art.Size())

	if reverse {
		idx = art.Size() - 1
	}

	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	art.ForEach(saveValues)
	return &artIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}
func (arti *artIterator) Rewind() {
	arti.currentIndex = 0
}

func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currentIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currentIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.currentIndex += 1
}

func (arti *artIterator) Valid() bool {
	return arti.currentIndex < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.currentIndex].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currentIndex].pos
}

func (arti *artIterator) Close() {
	arti.values = nil
}
