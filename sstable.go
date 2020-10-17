package dbengine

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SSTable - represents a sstable file
type SSTable interface {
	// Index - returns the index of the sstable, if there is one
	Index() SSTableIndex

	// File - returns the file path of the sstable file
	File() string

	// Dump - dumps the memtable into the sstable file
	Dump(MemTable) error

	// Get - returns the value of key specified
	Get(key string) ([]byte, error)

	// GetRange - returns the values of key range specified
	GetRange(start, end string) ([][]byte, error)
}

// SSTableIndex - represents an index for a SSTable file
type SSTableIndex interface {
	// GetOffset - get offset (in byte) of key in the sstable file
	GetOffset(key string) (offset int32)

	// GetOffsetRange - get start, end offsets (in byte) of key range specified in the sstable file
	GetOffsetRange(start, end string) (startOffset, endOffset int32)

	// Serialize - turn the index data structure into bytes that can be stored on disk
	Serialize() ([]byte, error)
}

// BasicSSTable - a basic implementation of the `SSTable` interface
type BasicSSTable struct {
	file *os.File
	idx  SSTableIndex
}

// BasicSSTableIndex - a basic implementation of the `SSTableIndex` interface
type BasicSSTableIndex struct{}

// NewBasicSSTable - creates a new basic sstable object
func NewBasicSSTable(sstableDir string) *BasicSSTable {
	f, err := newSSTableFile(sstableDir)
	if err != nil {
		panic(err)
	}
	return &BasicSSTable{
		file: f,
		idx:  &BasicSSTableIndex{},
	}
}

func newSSTableFile(sstableDir string) (*os.File, error) {
	ts := time.Now().UnixNano()
	filename := filepath.Join(sstableDir, fmt.Sprintf("sstable_%d", ts))
	// os.O_CREATE|os.O_EXCL - create file only when it doesn't exist, error out otherwise
	// os.O_RDWR - open for read & write
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Index - returns the index of the sstable, if there is one
func (s *BasicSSTable) Index() SSTableIndex {
	return s.idx
}

// File - returns the file path of the sstable file
func (s *BasicSSTable) File() string {
	return s.file.Name()
}

// Dump - dumps the memtable into the sstable file
// TODO: (p0)
func (s *BasicSSTable) Dump(MemTable) error {
	return nil
}

// Get - returns the value of key specified
// TODO: (p1)
func (s *BasicSSTable) Get(key string) ([]byte, error) {
	return nil, nil
}

// GetRange - returns the values of key range specified
// TODO: (p2)
func (s *BasicSSTable) GetRange(start, end string) ([][]byte, error) {
	return nil, nil
}

// GetOffset - get offset (in byte) of key in the sstable file
// TODO: (p1)
func (idx *BasicSSTableIndex) GetOffset(key string) (offset int32) {
	return -1
}

// GetOffsetRange - get start, end offsets (in byte) of key range specified in the sstable file
// TODO: (p2)
func (idx *BasicSSTableIndex) GetOffsetRange(start, end string) (startOffset, endOffset int32) {
	return -1, -1
}

// Serialize - turn the index data structure into bytes that can be stored on disk
// TODO: (p0)
func (idx *BasicSSTableIndex) Serialize() ([]byte, error) {
	return nil, nil
}
