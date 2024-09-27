package fio

const DateFilePerm = 0644

// IOManager IO 管理器
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error // 持久化数据
	Close() error
	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前仅支持标准 FileIO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
