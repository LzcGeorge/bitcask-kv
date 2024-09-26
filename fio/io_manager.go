package fio

const DateFilePerm = 0644

// IOManager IO 管理器
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error // 持久化数据
	Close() error
}
