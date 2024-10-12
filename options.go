package bitcask_go

type Options struct {
	DirPath            string      // 数据库数据目录
	DataFileSize       int64       // 文件大小
	SyncWrites         bool        // 写数据是否持久化
	BytesPerSync       uint        // 累计写到多少字节后进行持久化
	IndexType          IndexerType // 索引类型
	MMapAtStartup      bool        // 是否在启动时使用 MMap 打开数据文件
	DataFileMergeRatio float32     // 数据文件合并的阈值
}

// 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为 prefix 的 key，默认 为空
	Prefix  []byte
	Reverse bool // 是否倒序，默认正向（false）
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 事务批量写配置项
type WriteBatchOptions struct {
	MaxBatchSize int
	// 提交时是否 sync 持久化
	SyncWrites bool
}

// 索引类型
type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
	// BPlusTree 索引
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            "/Volumes/kioxia/Repo/Distribution/bitcask-go/bitcask-go/Database",
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 10000,
	SyncWrites:   true,
}
