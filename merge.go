package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	//  数据库为空
	if db.activeFile == nil {
		return nil
	}

	// 加锁
	db.lock.Lock()

	// 是否有进程在 Merge
	if db.isMerging {
		db.lock.Unlock()
		return ErrMergeIsPrecessing
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 将当前活跃文件转化为旧的数据文件
	if err := db.activeFile.Sync(); err != nil {
		db.lock.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	// 创建新的活跃文件,用于在 Merge 过程中的读写
	if err := db.setActiveDateFile(); err != nil {
		db.lock.Unlock()
		return err
	}
	nonMergeFileId := db.activeFile.FileId

	// 取出所有旧的数据文件，进行排序
	var mergeFiles []*data.DataFile
	for _, df := range db.olderFiles {
		mergeFiles = append(mergeFiles, df)
	}
	db.lock.Unlock() // 及时释放锁，为了在 Merge 过程中能够正常的读写新的数据
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// 创建新的 Merge 文件夹
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err // 如果存在的话，删除旧的 Merge 文件夹
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 创建新的 db 用于 merge
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 新建 Hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理 mergeFiles 中的 DataFile
	for _, dataFile := range mergeFiles {
		var offset int64
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break // 读到 文件末尾
				}
				return err
			}

			realKey, _ := DecodeKeyWithSeqNo(record.Key)
			recordPos := db.index.Get(realKey)

			// 和内存中的索引位置比较，如果是有效数据，则写入 mergeDB
			if recordPos != nil && recordPos.Fid == dataFile.FileId && recordPos.Offset == offset {
				// 清除事务标记, 并写入
				record.Key = encodeKeyWithSeqNo(realKey, nonTransactionSeqNo)
				mergeRecordPos, err := mergeDB.appendLogRecord(record)
				if err != nil {
					return err
				}

				// 更新 hint 文件,将当前位置写入 hint 文件
				if err := hintFile.WriteHintRecord(realKey, mergeRecordPos); err != nil {
					return err
				}
			}
			// 到下一个 record 的位置
			offset += size
		}
	}

	// 持久化 mergeDB
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写入标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encodeLogRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encodeLogRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// 在数据库启动的时候对 mergeIndex（在 hint 文件中） 进行处理
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil // 数据库不存在 merge 文件夹
	}
	defer func() {
		if err := os.RemoveAll(mergePath); err != nil {
			panic(err)
		}
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 遍历目录下的文件，找出需要 merge 的data数据
	var mergeFinished bool
	var mergeFileNames []string
	// TODO 我的正确（错误）代码
	//for _, entry := range dirEntries {
	//	if !mergeFinished {
	//		if entry.Name() == data.MergeFinishedFileName {
	//			mergeFinished = true
	//		} else {
	//			mergeFileNames = append(mergeFileNames, entry.Name())
	//		}
	//	}
	//}
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果没有找到 mergeFinished 文件，则直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFileId,err := db.
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishFile, err := data.OpenMergeFinishFile(dirPath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	strconv.Atoi(string(record.Value)),nil
}