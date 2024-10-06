package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

// TransactionRecord 事务的记录
type TransactionRecord struct {
	Record *LogRecord // key 中含 seqNo，写入到索引中的
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 record 进行编码，返回字节数组和长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	headerBuf := make([]byte, maxLogRecordHeaderSize)

	// 填充到 headerBuf 中
	var pos = 0
	// crc 最后写入, 先设置类型值
	pos += 4 // 预留 4 个字节位置给 crc
	headerBuf[pos] = record.Type
	pos += 1

	// 利用 binary 写入 keySize 和 valueSize
	keySize := int64(len(record.Key))
	valueSize := int64(len(record.Value))
	pos += binary.PutVarint(headerBuf[pos:], keySize)
	pos += binary.PutVarint(headerBuf[pos:], valueSize)

	// 重新封装 record 转化为 []byte
	var recordSize = int64(pos) + keySize + valueSize
	recordBytes := make([]byte, recordSize)
	copy(recordBytes[:pos], headerBuf[:pos])             // 将 header 填充到 recordBytes 中
	copy(recordBytes[pos:], record.Key)                  // 将 key 填充到 recordBytes 中
	copy(recordBytes[pos+int(keySize):], record.Value)   // 将 value 填充到 recordBytes 中
	crc := crc32.ChecksumIEEE(recordBytes[4:])           // 计算 crc 检验和
	binary.LittleEndian.PutUint32(recordBytes[0:4], crc) // 将 crc 填充到 recordBytes 中

	// fmt.Printf("headerSize: %d, type: %d, keySize: %d, valueSize: %d, crc: %d\n", pos, record.Type, keySize, valueSize, crc)
	return recordBytes, recordSize
}

// DecodeLogRecordHeader 从 headerBuf 中解码出 LogRecordHeader
func DecodeLogRecordHeader(headerBuf []byte) (*LogRecordHeader, int64) {
	if len(headerBuf) <= 4 {
		return nil, 0 // 长度不足，无效的数据
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(headerBuf[0:4]),
		recordType: headerBuf[4],
	}

	// 从 headerBuf 中解码出 keySize
	var pos = 5
	keySize, n := binary.Varint(headerBuf[pos:])
	header.keySize = uint32(keySize)
	pos += n

	// 从 headerBuf 中解码出 valueSize
	valueSize, n := binary.Varint(headerBuf[pos:])
	header.valueSize = uint32(valueSize)
	pos += n

	return header, int64(pos)
}

// GetLogRecordCRC 计算 LogRecord 的 crc 校验和
// 传过来的 headerBuf 中是不含 crc 的
func GetLogRecordCRC(record *LogRecord, headerWithoutCRC []byte) uint32 {
	if record == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerWithoutCRC[:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)
	return crc
}

// EncodeLogRecordPos 将 LogRecordPos 编码为字节数组, 格式：fid + offset
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeLogRecordPos 解码LogRecordPos的字节数组
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
	}
}
