package dbengine

type bytes []byte

// MemTable - A memtable handles the in-memory operatoins of the DB on data that
// has not been persisted into the file system
type MemTable interface {
	Get(key string) ([]byte, error)
	GetRange(start, end string) ([]bytes, error)
	Write(key string, value []byte) error
	// Wal - returns the write-ahead-log instance for write ops recording
	Wal() Wal
}

// SkipListMemTable - A memtable implementation using the skip list data structure
type SkipListMemTable struct {
	s         *skipList
	totalSize int64
}
