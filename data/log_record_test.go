package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func Test_EncodeLogRecord(t *testing.T) {
	// 正常数据
	record1 := &LogRecord{
		Key:   []byte("key"),
		Type:  LogRecordNormal,
		Value: []byte("value"),
	}

	recordBytes1, n1 := EncodeLogRecord(record1)
	// t.Log(recordBytes1, n1)
	assert.NotNil(t, recordBytes1)
	assert.Greater(t, n1, int64(5))

	// value 为空的时候
	record2 := &LogRecord{
		Key:  []byte("emptyValue"),
		Type: LogRecordNormal,
	}
	recordBytes2, n2 := EncodeLogRecord(record2)
	// t.Log(recordBytes2, n2)
	assert.NotNil(t, recordBytes2)
	assert.Greater(t, n2, int64(5))

	// 对类型为 Deleted 的数据测试
	record3 := &LogRecord{
		Key:   []byte("deletedKey"),
		Value: []byte("something"),
		Type:  LogRecordDeleted,
	}
	recordBytes3, n3 := EncodeLogRecord(record3)
	// t.Log(recordBytes3, n3)
	assert.NotNil(t, recordBytes3)
	assert.Greater(t, n3, int64(5))
}

func Test_DecodeLogRecordHeader(t *testing.T) {
	// 正常数据
	// headerSize: 7, type: 0, keySize: 3, valueSize: 5, crc: 1354786746
	headerBuf := [](byte){186, 103, 192, 80, 0, 6, 10}
	header1, headerSize1 := DecodeLogRecordHeader(headerBuf)
	// t.Log(header1.crc, header1.recordType, header1.keySize, header1.valueSize)
	// t.Log("headerSize1: ", headerSize1)
	assert.NotNil(t, header1)
	assert.Greater(t, headerSize1, int64(5))
	assert.Equal(t, int64(7), headerSize1)
	assert.Equal(t, uint32(1354786746), header1.crc)
	assert.Equal(t, LogRecordNormal, header1.recordType)
	assert.Equal(t, uint32(3), header1.keySize)
	assert.Equal(t, uint32(5), header1.valueSize)

	// value 为空的数据
	// headerSize: 7, type: 0, keySize: 10, valueSize: 0, crc: 407273546
	headerBuf2 := [](byte){74, 128, 70, 24, 0, 20, 0}
	header2, headerSize2 := DecodeLogRecordHeader(headerBuf2)
	// t.Log(header2.crc, header2.recordType, header2.keySize, header2.valueSize)
	// t.Log("headerSize2: ", headerSize2)
	assert.NotNil(t, header2)
	assert.Greater(t, headerSize2, int64(5))
	assert.Equal(t, int64(7), headerSize2)
	assert.Equal(t, uint32(407273546), header2.crc)
	assert.Equal(t, LogRecordNormal, header2.recordType)
	assert.Equal(t, uint32(10), header2.keySize)
	assert.Equal(t, uint32(0), header2.valueSize)

	// 对类型为 Deleted 的数据测试
	// headerSize: 7, type: 1, keySize: 10, valueSize: 9, crc: 667747257
	headerBuf3 := []byte{185, 3, 205, 39, 1, 20, 18}
	header3, headerSize3 := DecodeLogRecordHeader(headerBuf3)
	// t.Log(header3.crc, header3.recordType, header3.keySize, header3.valueSize)
	// t.Log("headerSize3: ", headerSize3)
	assert.NotNil(t, header3)
	assert.Greater(t, headerSize3, int64(5))
	assert.Equal(t, int64(7), headerSize3)
	assert.Equal(t, uint32(667747257), header3.crc)
	assert.Equal(t, LogRecordDeleted, header3.recordType)
	assert.Equal(t, uint32(10), header3.keySize)
	assert.Equal(t, uint32(9), header3.valueSize)
}
func TestGetLogRecordCRC(t *testing.T) {
	// 正常数据
	// headerSize: 7, type: 0, keySize: 3, valueSize: 5, crc: 1354786746
	record := &LogRecord{
		Key:   []byte("key"),
		Type:  LogRecordNormal,
		Value: []byte("value"),
	}
	headerBuf1 := [](byte){186, 103, 192, 80, 0, 6, 10}
	headerWithoutCRC1 := headerBuf1[crc32.Size:]
	crc1 := GetLogRecordCRC(record, headerWithoutCRC1)
	assert.NotNil(t, crc1)
	assert.Equal(t, uint32(1354786746), crc1)

	// value 为空的时候
	// headerSize: 7, type: 0, keySize: 10, valueSize: 0, crc: 407273546
	record2 := &LogRecord{
		Key:  []byte("emptyValue"),
		Type: LogRecordNormal,
	}
	headerBuf2 := [](byte){74, 128, 70, 24, 0, 20, 0}
	headerWithoutCRC2 := headerBuf2[crc32.Size:]
	crc2 := GetLogRecordCRC(record2, headerWithoutCRC2)
	assert.NotNil(t, crc2)
	assert.Equal(t, uint32(407273546), crc2)

	// 对类型为 Deleted 的数据测试
	// headerSize: 7, type: 1, keySize: 10, valueSize: 9, crc: 667747257
	record3 := &LogRecord{
		Key:   []byte("deletedKey"),
		Value: []byte("something"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{185, 3, 205, 39, 1, 20, 18}
	headerWithoutCRC3 := headerBuf3[crc32.Size:]
	crc3 := GetLogRecordCRC(record3, headerWithoutCRC3)
	assert.NotNil(t, crc3)
	assert.Equal(t, uint32(667747257), crc3)
}
