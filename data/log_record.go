package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// 最大日志记录头大小: crc(4) + type(1) + keySize(5) + valueSize(5)
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录，数据是追加写入的
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引：数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id： 数据存储在那个文件
	Offset int64  // 偏移量： 数据在文件中的偏移量
}

type LogRecordHeader struct {
	crc        uint32        // crc 校验和
	recordType LogRecordType // LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// EncodeLogRecord 对 record 进行编码，返回字节数组和长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

// DecodeLogRecordHeader 从 buf 中解码出 LogRecordHeader
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC 计算 LogRecord 的 crc 校验和
func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	return 0
}
