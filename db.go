package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const Database_Path = "./Database"

// DB bitcask 存储引擎实例
type DB struct {
	options    Options // 用户传过来的配置项，一般不可修改，所以没加指针
	lock       *sync.RWMutex
	fileIds    []int                     // 数据文件 id，用时需转化为 uint32 类型，作为 fileId。只能在加载索引时使用，不能在其他地方更新和使用
	activeFile *data.DataFile            // 当前活跃的数据文件,可以写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 数据内存索引
	seqNo      uint64                    // 事务序列号，全局递增，和 key 一起写入索引中（文件中只有 key）
	isMerging  bool                      // 是否正在合并数据文件
}

// Open 打开一个 bitcask 数据库
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，不存在则创建新的目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		lock:       new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(index.IndexType(options.IndexType)),
	}

	// 从 merge DB 中加载数据文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件，保存文件的 id 到 fileIds
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hintFile 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加载数据文件中的索引: 从 fileIds 中拿到文件
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// Put 写入数据
func (db *DB) Put(key []byte, value []byte) error {
	//    判断 key 是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	record := &data.LogRecord{
		Key:   encodeKeyWithSeqNo(key, nonTransactionSeqNo),
		Type:  data.LogRecordNormal,
		Value: value,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(record)
	if err != nil {
		return err
	}

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

	// 从索引地址 取得对应的 value
	return db.GetValueByRecordPos(logRecordPos)
}

// Delete 删除数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查数据库中是否存在 key
	if pos := db.index.Get(key); pos == nil {
		// 不存在的话，删除一个不存在的键并不会改变数据库的状态。
		// 相当于直接删除了，直接忽略这次操作即可
		return nil

		// return ErrKeyNotFound
	}

	// 当前操作写入数据文件, 标识其是被删除的
	record := &data.LogRecord{
		Key:  encodeKeyWithSeqNo(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(record)
	if err != nil {
		return err
	}

	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// ListKeys 列出所有的 key
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	keyListSize := db.index.Size()
	keys := make([][]byte, keyListSize)
	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys
}

// Fold ：遍历所有的数据的循环内，执行用户自定义函数，函数返回 false 时终止遍历
func (db *DB) Fold(foldFunc func(key []byte, value []byte) bool) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	iter := db.index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value, err := db.GetValueByRecordPos(iter.Value())
		if err != nil {
			return err
		}

		// 执行用户指定的函数，如果返回 false 则终止遍历
		if !foldFunc(key, value) {
			break
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	// 只需要持久化当前活跃文件即可，旧的数据文件在被扔到 map 之前，就已经持久化了！
	// 具体见 db.go 中 appendLogRecord 方法
	return db.activeFile.Sync()
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	// 写锁
	db.lock.Lock()
	defer db.lock.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendLogRecord(record)
}

// appendLogRecord 追加写入到当前活跃数据文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃数据文件是否存在，不存在则创建一个
	if db.activeFile == nil {
		if err := db.setActiveDateFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(record)

	// 判断当前活跃文件的写入位置是否超过阈值，超过则创建一个新文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 持久化数据文件, 把当前活跃丢到 map 中去
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

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 遍历目录下的文件，找出data数据
	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			fileName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(fileName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件，打开对应的数据文件
	for i, fid := range fileIds {
		var fileId = uint32(fid) // 类型转换
		dataFile, err := data.OpenDateFile(db.options.DirPath, fileId)
		if err != nil {
			return err
		}

		// 最后一个文件，是当前的活跃文件. 其他的设置为 旧的数据文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fileId] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFile 遍历所有的数据文件，并更新到内存索引中
func (db *DB) loadIndexFromDataFile() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFIleId := false, uint32(0)
	mergeFinishedFilePath := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFilePath); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge, nonMergeFIleId = true, fid
	}

	updateIndex := func(key []byte, recordType data.LogRecordType, recordPos *data.LogRecordPos) {
		var ok bool
		if recordType == data.LogRecordNormal {
			ok = db.index.Put(key, recordPos)
		} else if recordType == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 存储事务中的记录，是一个列表 []*data.TransactionRecord
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid) // 类型转换
		var dataFile *data.DataFile

		// 如果 fileId 比 nonMergeFIleId 小，则说明已经从 hintFIle 中加载索引了
		if hasMerge && fileId < nonMergeFIleId {
			continue
		}
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			recordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 解析 key，拿到事务序列号
			realKey, seqNo := DecodeKeyWithSeqNo(record.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, record.Type, recordPos)
			} else {
				// 事务操作，对应的 seqNo 的数据更新到 内存索引 中
				if record.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
						delete(transactionRecords, seqNo)
					}
				} else {
					record.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Pos:    recordPos,
						Record: record,
					})
				}
			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读
			offset += size

		}

		// 如果当前是活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}

// GetValueByRecordPos 根据索引信息，从数据文件中读取数据
func (db *DB) GetValueByRecordPos(logRecordPos *data.LogRecordPos) ([]byte, error) {
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
	record, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return record.Value, nil
}
