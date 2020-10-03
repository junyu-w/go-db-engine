package dbengine

// MemTable - A memtable handles the in-memory operatoins of the DB on data that
// has not been persisted into the file system
type MemTable interface {
	// Get - retrieves the value saved with key
	Get(key string) []byte
	// GetRange - retrieves all values from specified key range
	GetRange(start, end string) [][]byte
	// Write - write key with value into memtable
	Write(key string, value []byte)
	// Delete - delete a record with key
	Delete(key string)
	// Wal - returns the write-ahead-log instance for write ops recording
	Wal() Wal
	// Serialize - serialize the memtable into bytes that can be stored on filesystem
	Serialize() []byte
}

// SkipListMemTable - A memtable implementation using the skip list data structure
type SkipListMemTable struct {
	s              *skipList
	TotalSizeBytes uint64 // total size of key, value data stored
}

// NewMemTable - create a new memtable instance
func NewMemTable(threshold uint64) *SkipListMemTable {
	return &SkipListMemTable{
		s:              newSkipList(),
		TotalSizeBytes: 0,
	}
}

// Get - retrieves the value saved with key
func (m *SkipListMemTable) Get(key string) []byte {
	node := m.s.search(key)
	if node != nil {
		return node.value
	}
	return nil
}

// Write - write key with value into memtable
func (m *SkipListMemTable) Write(key string, value []byte) {
	m.s.upsert(key, value)

	sizeWritten := len(key) + len(value)
	m.TotalSizeBytes += uint64(sizeWritten)
}

// Delete - delete a record with key
// TODO
func (m *SkipListMemTable) Delete(key string) {
	// upon deletion, insert a tombstone record instead of performing actual deletion
}

// Serialize - serialize the memtable into bytes that can be stored on filesystem
// TODO
func (m *SkipListMemTable) Serialize() []byte {
	return nil
}

// GetRange - retrieves all values from specified key range
// TODO
func (m *SkipListMemTable) GetRange(start, end string) [][]byte {
	return nil
}
