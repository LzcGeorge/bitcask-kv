package bitcask_go

import (
	"bitcast-go/data"
	"bitcast-go/index"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options // 用户传过来的配置项，一般不可修改，所以没加指针
	lock       *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件,可以写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 数据内存索引
}

// Put 写入数据
func (db *DB) Put(key []byte, value []byte) error {
	//    判断 key 是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	record := &data.LogRecord{
		Key:   key,
		Type:  data.LogRecordNormal,
		Value: value,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}

	// TODO 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 判断 key 是否为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存中拿到 key 的索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取数据文件
	record, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return record.Value, nil
}

// appendLogRecord 追加写入到当前活跃数据文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 判断当前活跃数据文件是否存在，不存在则创建一个
	if db.activeFile != nil {
		if err := db.setActiveDateFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(record)

	// 判断当前活跃文件的写入位置是否超过阈值，超过则创建一个新文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 创建新的活跃文件
		if err := db.setActiveDateFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset

	// 写入新的活跃文件
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOffset}, nil
}

func (db *DB) setActiveDateFile() error {
	var initialField uint32 = 0
	if db.activeFile != nil {
		initialField = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDateFile(db.options.DirPath, initialField)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}
