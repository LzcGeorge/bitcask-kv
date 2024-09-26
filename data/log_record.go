package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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

// EncodeLogRecord 对 record 进行编码，返回字节数组和长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
