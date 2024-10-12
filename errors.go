package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
	ErrBatchTooLarge          = errors.New("batch too large")
	ErrMergeIsPrecessing      = errors.New("merge is precessing")
	ErrDatabaseIsUsing        = errors.New("the database directory is using by another process")
	ErrMergeRatioUnreached    = errors.New("merge ratio unreached")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough space for merge")
)
