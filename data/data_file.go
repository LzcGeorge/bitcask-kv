package data

import (
	"bitcask-go/fio"
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
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件id
	WriteOffset int64         // 文件写到了哪个位置
	IOManager   fio.IOManager // IO读写管理器
}

func NewDateFile(filePath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(filePath, ioType)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// GetDataFileName 获取数据文件路径名
func GetDataFileName(dirPath string, fileId uint32) string {
	filePath := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	return filePath
}

// OpenDateFile 打开数据文件
func OpenDateFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	filePath := GetDataFileName(dirPath, fileId)
	return NewDateFile(filePath, fileId, ioType)
}

// 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, HintFileName)
	return NewDateFile(filePath, 0, fio.StandardIO)
}

// OpenMergeFinishFile 打开 标识merge完成的文件
func OpenMergeFinishFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, MergeFinishedFileName)
	return NewDateFile(filePath, 0, fio.StandardIO)
}

// OpenSeqNoFile 打开存储 seqNo 事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, SeqNoFileName)
	return NewDateFile(filePath, 0, fio.StandardIO)
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

// 向 hint 文件写入索引信息
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	logRecord, _ := EncodeLogRecord(record)
	return df.Write(logRecord)
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

	// 校验 crc，crc32.size 是 crc32 校验码的长度
	headerWithoutCRC := headerBuf[crc32.Size:headerSize]
	if header.crc != GetLogRecordCRC(record, headerWithoutCRC) {
		return nil, 0, ErrInvalidCRC
	}

	// 测试是否已经 key 中是否含有 seqNo
	// seqNo, n := binary.Uvarint(record.Key)
	// realKey := record.Key[n:]
	// println(seqNo, string(realKey))
	// fmt.Printf("key: %v,recordType: %v,recordSize: %v\n", string(record.Key), record.Type, recordSize)

	return record, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}

// SetIOManager 设置 IO 类型
func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IOManager = ioManager
	return nil
}
