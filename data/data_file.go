package data

import "bitcast-go/fio"

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件id
	WriteOffset int64         // 文件写到了哪个位置
	IOManager   fio.IOManager // IO读写管理器
}

// OpenDateFile 打开数据文件
func OpenDateFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// Sync 持久化数据文件
func (df *DataFile) Sync() error {
	return nil
}

// Write 写数据
func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
