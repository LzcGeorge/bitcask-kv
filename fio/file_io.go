package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 创建文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DateFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(bytes []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(bytes, offset)
}

func (fio *FileIO) Write(bytes []byte) (int, error) {
	return fio.fd.Write(bytes)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
