package data

import (
	"bitcast-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix = ".data"
)

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件id
	WriteOffset int64         // 文件写到了哪个位置
	IOManager   fio.IOManager // IO读写管理器
}

// OpenDateFile 打开数据文件
func OpenDateFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// Sync 持久化数据文件
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// Write 写数据
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}

	df.WriteOffset += int64(n)
	return nil
}

// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	// 获取文件大小
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 已经超过了文件的长度，则只需读取到文件的末尾即可
	// 因为 header 是变长的，而每次读取默认读取 最大长度的 header
	var headerBufSize int64 = int64(maxLogRecordHeaderSize)
	if offset+headerBufSize > fileSize {
		headerBufSize = fileSize - offset
	}

	// 读取 header 数据
	headerBuf, err := df.readNBytes(headerBufSize, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := DecodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 读取 key 和 value 数据
	record := &LogRecord{Type: header.recordType}
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize int64 = headerSize + keySize + valueSize

	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, headerSize+offset)
		if err != nil {
			return nil, 0, err
		}
		record.Key = kvBuf[:keySize]
		record.Value = kvBuf[keySize:]
	}

	// 校验 crc
	if header.crc != getLogRecordCRC(record, headerBuf[crc32.Size:headerSize]) {
		return nil, 0, ErrInvalidCRC
	}

	return record, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}
