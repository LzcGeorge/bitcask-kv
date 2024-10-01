package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator 用户使用的迭代器
type Iterator struct {
	indexIterator index.Iterator // 索引迭代器
	db            *DB
	Options       IteratorOptions // 迭代器配置项

}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIterator := db.index.Iterator(opts.Reverse)

	iterator := Iterator{
		indexIterator: indexIterator,
		db:            db,
		Options:       opts,
	}
	// 跳过不符合前缀的 key
	iterator.skipToNext()
	return &iterator
}

func (it *Iterator) Rewind() {
	it.indexIterator.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIterator.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIterator.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIterator.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIterator.Key()
}

// Value 获取当前迭代器的 value 值
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIterator.Value()
	it.db.lock.RLock() // 读锁
	defer it.db.lock.RUnlock()
	return it.db.GetValueByRecordPos(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIterator.Close()
}

// 在 skipToNext() 中使用 it.indexIterator.Valid()，
// 确保我们直接处理底层迭代器的状态，避免了使用 it.Valid() 可能引发的递归或副作用。
func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return // 不存在前缀, 不需要跳过
	}

	for it.indexIterator.Valid() {
		key := it.indexIterator.Key()
		if prefixLen <= len(key) && bytes.HasPrefix(key, it.Options.Prefix) {
			break
		}
		it.indexIterator.Next()
	}
}
