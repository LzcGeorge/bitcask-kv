package index

// B+ 树索引
// 主要封装了 import bolt "go.etcd.io/bbolt" 库

import (
	"bitcask-go/data"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bolt.DB
}

// 初始化 B+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("filed to open bptree")
	}

	// 创建索引桶
	if err := bptree.Update(func(tx *bolt.Tx) error {
		_, err2 := tx.CreateBucketIfNotExists(indexBucketName)
		return err2
	}); err != nil {
		panic("filed to create bucket")
	}

	return &BPlusTree{tree: bptree}
}
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		println("filed to put key into bptree")
		panic(err)
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("filed to get key from bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		if value := bucket.Get(key); len(value) == 0 {
			return nil
		} else {
			return bucket.Delete(key)
		}
	}); err != nil {
		panic("filed to delete key from bptree")
	}
	return true
}
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("filed to get size from bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return NewBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

type bptreeIterator struct {
	tx        *bolt.Tx
	cursor    *bolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func NewBptreeIterator(bpt *bolt.DB, reverse bool) *bptreeIterator {
	// 开启一个事务
	tx, err := bpt.Begin(false)
	if err != nil {
		panic("filed to begin tx")
	}
	bpti := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()
	return bpti
}
func (bpti *bptreeIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

func (bpti *bptreeIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.currKey) != 0
}

func (bpti *bptreeIterator) Key() []byte {
	return bpti.currKey
}

func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currValue)
}

func (bpti *bptreeIterator) Close() {
	//bpti.tx.Commit()
	bpti.tx.Rollback()
}
