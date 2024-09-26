package bitcask_go

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 文件大小
	SyncWrites   bool   // 写数据是否持久化
}
