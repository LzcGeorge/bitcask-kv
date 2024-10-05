package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const Database_Path = "../Database/datafileTest"

func TestOpenDateFile(t *testing.T) {

	file, err := OpenDateFile(Database_Path, 2)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	file2, err := OpenDateFile(Database_Path, 22)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	// 重复打开同一个文件
	file3, err := OpenDateFile(Database_Path, 22)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}

func TestDataFile_Write(t *testing.T) {
	file, err := OpenDateFile(Database_Path, 9)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)
	err = file.Write([]byte(" selfknow\n"))
	assert.Nil(t, err)

	err = file.Write([]byte("hh\n"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file, err := OpenDateFile(Database_Path, 113)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDateFile(Database_Path, 123)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dateFile, err := OpenDateFile(Database_Path, 1)
	assert.Nil(t, err)
	assert.NotNil(t, dateFile)

	// 写入 record1 到 dataFile
	record1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world2"),
		Type:  LogRecordNormal,
	}
	recordBytes1, recordBytesSize1 := EncodeLogRecord(record1)
	err = dateFile.Write(recordBytes1)
	assert.Nil(t, err)

	// 从文件中读取 LogRecord，并验证是否与输入的相同
	resRecord, resRecordSize, err := dateFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, recordBytesSize1, resRecordSize)
	assert.Equal(t, record1, resRecord)

	// 追加 LogRecord 写入到 dataFile
	record2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("selfknow"),
		Type:  LogRecordNormal,
	}
	recordBytes2, recordBytesSize2 := EncodeLogRecord(record2)
	err = dateFile.Write(recordBytes2)
	assert.Nil(t, err)
	// 读取 record2，在 dataFile 中的 offset 为第一个 Record 的大小
	resRecord2, resRecordSize2, err := dateFile.ReadLogRecord(resRecordSize)
	assert.Nil(t, err)
	assert.Equal(t, recordBytesSize2, resRecordSize2)
	assert.Equal(t, record2, resRecord2)

	// 追加一个删除的数据 到 dataFile
	record3 := &LogRecord{
		Key:   []byte("deleteRecord"),
		Value: []byte("something"),
		Type:  LogRecordDeleted,
	}
	recordBytes3, recordBytesSize3 := EncodeLogRecord(record3)
	err = dateFile.Write(recordBytes3)
	assert.Nil(t, err)
	// 读取 record3, 在 dataFile 中的 offset 为 record2 和 record1 的大小之和
	resRecord3, resRecordSize3, err := dateFile.ReadLogRecord(resRecordSize + resRecordSize2)
	assert.Nil(t, err)
	assert.Equal(t, recordBytesSize3, resRecordSize3)
	assert.Equal(t, record3, resRecord3)

}
