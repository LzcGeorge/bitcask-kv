package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 事务批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	lock          *sync.Mutex // 写锁
	db            *DB
	pendingWrites map[string]*data.LogRecord // 待写入的数据，realKey（不含 seqNo）
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seqNo file not exists")
	}
	return &WriteBatch{
		options:       opts,
		lock:          new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	// 暂存到 pendingWrites 中
	record := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	// 数据不存在，直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		// 如果在 pendingWrites 中，删除
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存到 pendingWrites 中
	record := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	batchSize := len(wb.pendingWrites)
	if batchSize == 0 {
		return nil
	}
	if batchSize > wb.options.MaxBatchSize {
		return ErrBatchTooLarge
	}

	// 加数据库的锁，保证事务提交串行化
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 遍历暂存数据，写入数据文件
	pendingPos := make(map[string]*data.LogRecordPos)
	// batch 中 put 和 get 方法的 key 都是 realKey （不含 seqNo），只有在事务提交中才有的
	for _, record := range wb.pendingWrites {
		encodeKey := encodeKeyWithSeqNo(record.Key, seqNo)
		recordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   encodeKey,
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		pendingPos[string(record.Key)] = recordPos
	}

	// 写一条标识事务完成的 logRecord
	finishedRecord := &data.LogRecord{
		Type: data.LogRecordTxnFinished,
		Key:  encodeKeyWithSeqNo(txnFinKey, seqNo),
	}
	// println("finishedRecord: ", string(finishedRecord.Key))
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := pendingPos[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		} else if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// encodeKeyWithSeqNo 编码格式: seqNo + key
func encodeKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seqno := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seqno[:], seqNo)

	encodeKey := make([]byte, len(key)+n)
	copy(encodeKey[:n], seqno[:n])
	copy(encodeKey[n:], key)
	return encodeKey
}

// DecodeKeyWithSeqNo 获取实际的 key 和 事务序列号,返回格式： key, seqNo
func DecodeKeyWithSeqNo(encodeKey []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(encodeKey)
	return encodeKey[n:], seqNo
}
