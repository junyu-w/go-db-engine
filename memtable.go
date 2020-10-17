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
	Write(key string, value []byte)

	// Delete - delete a record with key
	Delete(key string)

	// Wal - returns the write-ahead-log instance for write ops recording
	Wal() Wal

	// Serialize - serialize the memtable into bytes that can be stored on filesystem
	Serialize() []byte

	// SizeBytes - returns the total size of data stored in this memtable
	SizeBytes() uint32
}

// SkipListMemTable - A memtable implementation using the skip list data structure
type SkipListMemTable struct {
	s              *skipList
	wal            Wal
	TotalSizeBytes uint32 // total size of key, value data stored
}

// NewBasicMemTable - create a new memtable instance
func NewBasicMemTable(walDir string) *SkipListMemTable {
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

// Serialize - serialize the memtable into bytes that can be stored on filesystem
// TODO: (P1) Also, should this be here? since the serialized data is supposed to be understood by the database during `Get`
func (m *SkipListMemTable) Serialize() []byte {
	return nil
}

// GetRange - retrieves all values from specified key range
// TODO: P(2)
func (m *SkipListMemTable) GetRange(start, end string) [][]byte {
	return nil
}
