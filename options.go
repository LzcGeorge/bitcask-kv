package bitcask_go

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 文件大小
	SyncWrites   bool   // 写数据是否持久化
	IndexType    IndexerType
}

// 索引类型
type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}
