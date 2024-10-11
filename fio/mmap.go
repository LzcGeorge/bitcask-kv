package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DateFilePerm)
	if err != nil {
		return nil, err
	}
	reader, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: reader}, nil
}

func (mp *MMap) Read(bytes []byte, offset int64) (int, error) {
	return mp.readerAt.ReadAt(bytes, offset)
}

func (mp *MMap) Write(bytes []byte) (int, error) {
	panic("implement me")
}

func (mp *MMap) Sync() error {
	panic("implement me")
}

func (mp *MMap) Close() error {
	return mp.readerAt.Close()
}

func (mp *MMap) Size() (int64, error) {
	return int64(mp.readerAt.Len()), nil
}
