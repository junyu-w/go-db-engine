package dbengine

import (
	"github.com/DrakeW/go-db-engine/pb"
	"google.golang.org/protobuf/proto"
)

// MemTable - A memtable handles the in-memory operatoins of the DB on data that
// has not been persisted into the file system
type MemTable interface {
	// Get - retrieves the value saved with key
	Get(key string) []byte

	// GetRange - retrieves all values from specified key range
	GetRange(start, end string) [][]byte

	// Write - write key with value into memtable
	Write(key string, value []byte) error

	// Delete - delete a record with key
	Delete(key string) error

	// Wal - returns the write-ahead-log instance for write ops recording
	Wal() Wal

	// GetAll - returns all records stored in the memtable
	GetAll() []*MemtableRecord

	// SizeBytes - returns the total size of data stored in this memtable
	SizeBytes() uint32
}

// MemtableRecord - represents a single inserted record
type MemtableRecord struct {
	Key   string
	Value []byte
}

// SkipListMemTable - A memtable implementation using the skip list data structure
type SkipListMemTable struct {
	s              *skipList
	wal            Wal
	TotalSizeBytes uint32 // total size of key, value data stored
}

// NewBasicMemTable - create a new memtable instance
// TODO: (p3) make the memtable implementaion thread-safe
func NewBasicMemTable(walDir string) MemTable {
	wal, err := NewBasicWal(walDir)
	if err != nil {
		panic(err)
	}

	return &SkipListMemTable{
		s:              newSkipList(),
		wal:            wal,
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
func (m *SkipListMemTable) Write(key string, value []byte) error {
	walLog, err := m.keyValueToWalLogBytes(key, value)
	if err != nil {
		return err
	}

	if err = m.wal.Append(walLog); err != nil {
		return err
	}
	m.s.upsert(key, value)

	sizeWritten := len(key) + len(value)
	m.TotalSizeBytes += uint32(sizeWritten)
	return nil
}

// keyValueToWalLogBytes - converts a key value pair into raw bytes for WAL insertion
func (m *SkipListMemTable) keyValueToWalLogBytes(key string, value []byte) ([]byte, error) {
	log := &pb.MemtableKeyValue{
		Key:   key,
		Value: value,
	}
	raw, err := proto.Marshal(log)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// SizeBytes - returns the total size of data stored in this memtable
func (m *SkipListMemTable) SizeBytes() uint32 {
	return m.TotalSizeBytes
}

// Wal - returns the write-ahead-log instance for write ops recording
func (m *SkipListMemTable) Wal() Wal {
	return m.wal
}

// Delete - delete a record with key
func (m *SkipListMemTable) Delete(key string) error {
	// upon deletion, insert a tombstone record instead of performing actual deletion
	// TODO: (P3) figure out a way so that tombstone record doesn't conincide with custom value
	tombstoneVal := []byte("tombstone")
	walLog, err := m.keyValueToWalLogBytes(key, tombstoneVal)
	if err != nil {
		return err
	}

	if err = m.wal.Append(walLog); err != nil {
		return err
	}
	m.s.upsert(key, tombstoneVal)
	return nil
}

// GetRange - retrieves all values from specified key range
// TODO: (p2) implement GetRange
func (m *SkipListMemTable) GetRange(start, end string) [][]byte {
	return nil
}

// GetAll - returns all records stored in the memtable
func (m *SkipListMemTable) GetAll() []*MemtableRecord {
	records := make([]*MemtableRecord, m.s.size, m.s.size)
	i := 0
	for node := m.s.head.forwardNodeAtLevel[0]; node != nil; node = node.forwardNodeAtLevel[0] {
		records[i] = &MemtableRecord{
			Key:   node.key,
			Value: node.value,
		}
		i++
	}
	return records
}
