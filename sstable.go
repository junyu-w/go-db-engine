package dbengine

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/DrakeW/go-db-engine/pb"
	"google.golang.org/protobuf/proto"
)

// SSTable file layout:
// - <data_blocks>\n<data size>\n<index>\n<index size>
//
// NOTE: The reason that we write size after the actual data block is because this allows us to perform
// SEQUENTIAL WRITES.Instead of having to calculate and write final data block size by either holding everything
// in memory or reshuffling the stored bytes. This makes read a little bit unintuitive since we need to perform
// initial reads backwards to interpret the data layout.
//
//
// data_blocks layout
// - <block_1 size><block_1>\n...<block_N size><block_N>
//
// block layout
// - compressed serialized protocol buffer

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
	// GetOffset - get starting offset (in byte) of the block that contians the value for key
	GetOffset(key string) (offset uint)

	// GetOffsetRange - get start, end (non-inclusive) offsets (in byte) of blocks for key range specified
	GetOffsetRange(start, end string) (startOffset, endOffset uint)

	// Serialize - turn the index data structure into bytes that can be stored on disk
	Serialize() ([]byte, error)
}

// BasicSSTable - a basic implementation of the `SSTable` interface
type BasicSSTable struct {
	file *os.File
	idx  BasicSSTableIndex
	// BlockSize - controls roughly how big each block should be
	BlockSize uint
}

// BasicSSTableIndex - a basic implementation of the `SSTableIndex` interface
type BasicSSTableIndex map[string]uint64

// TODO:: better error handling in this file overall, similar to WAL file

// NewBasicSSTable - creates a new basic sstable object
func NewBasicSSTable(sstableDir string, blockSize uint) *BasicSSTable {
	f, err := newSSTableFile(sstableDir)
	if err != nil {
		panic(err)
	}
	return &BasicSSTable{
		file:      f,
		idx:       BasicSSTableIndex{},
		BlockSize: blockSize,
	}
}

func newSSTableFile(sstableDir string) (*os.File, error) {
	ts := time.Now().UnixNano()
	filename := filepath.Join(sstableDir, fmt.Sprintf("sstable_%d", ts))
	// os.O_CREATE|os.O_EXCL - create file only when it doesn't exist, error out otherwise
	// os.O_RDWR - open for read & write
	// os.O_APPEND - append only, since when writing to the sstable file we only need to perform sequential writes
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0644)
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
func (s *BasicSSTable) Dump(m MemTable) error {
	records := m.GetAll()
	// write data
	totalWritten, err := s.writeDataAndUpdateIndex(records)
	if err != nil {
		return err
	}
	s.file.Write([]byte("\n"))

	// write data size
	s.file.Write([]byte(fmt.Sprintf("%d", totalWritten)))
	s.file.Write([]byte("\n"))

	// write index
	written, err := s.writeIndexData()
	if err != nil {
		return err
	}
	s.file.Write([]byte("\n"))

	// write index size
	s.file.Write([]byte(fmt.Sprintf("%d", written)))

	return nil
}

// writeDataAndUpdateIndex - write memtable records to sstable file, update index corespondingly
// and return total bytes written
func (s *BasicSSTable) writeDataAndUpdateIndex(records []*MemtableRecord) (int, error) {
	accBlockKeyValueSize := 0
	totalDataSize := 0

	block := &pb.SSTableBlock{
		Data: make([]*pb.SSTableKeyValue, 0),
	}

	// write data blocks
	for i := 0; i < len(records); i++ {
		record := records[i]
		block.Data = append(block.Data, &pb.SSTableKeyValue{
			Key:   record.Key,
			Value: record.Value,
		})
		accBlockKeyValueSize += len(record.Key) + len(record.Value)

		if uint(accBlockKeyValueSize) >= s.BlockSize {
			// write block to disk once size reaches configured block size
			written, err := s.writeBlock(block)
			if err != nil {
				return totalDataSize, err
			}
			// update index, offset is previous total data size
			startingKey := block.Data[0].Key
			s.idx[startingKey] = uint64(totalDataSize)

			// update tracker states
			totalDataSize += written
			accBlockKeyValueSize = 0
			block.Data = make([]*pb.SSTableKeyValue, 0)
		}
	}

	if accBlockKeyValueSize > 0 {
		// write block to disk once size reaches configured block size
		written, err := s.writeBlock(block)
		if err != nil {
			return totalDataSize, err
		}
		// update index, offset is previous total data size
		startingKey := block.Data[0].Key
		s.idx[startingKey] = uint64(totalDataSize)

		totalDataSize += written
	}

	return totalDataSize, nil
}

func (s *BasicSSTable) writeBlock(block *pb.SSTableBlock) (int, error) {
	raw, err := s.serializeBlock(block)
	if err != nil {
		return 0, err
	}

	written, err := s.file.Write(raw)
	if err != nil {
		return written, err
	}
	return written, nil
}

func (s *BasicSSTable) serializeBlock(block *pb.SSTableBlock) ([]byte, error) {
	data, err := proto.Marshal(block)
	if err != nil {
		return nil, err
	}

	compressed, err := s.compress(data)
	if err != nil {
		return nil, err
	}

	return compressed, nil
}

// writeIndexData - write sstable index to sstable file and return total bytes written
func (s *BasicSSTable) writeIndexData() (int, error) {
	data, err := s.idx.Serialize()
	if err != nil {
		return 0, err
	}

	written, err := s.file.Write(data)
	if err != nil {
		return written, err
	}
	return written, nil
}

// TODO:: compress - compresses a data block
func (s *BasicSSTable) compress(raw []byte) ([]byte, error) {
	return raw, nil
}

// compress - decompresses a data block
// TODO: (p2)
func (s *BasicSSTable) decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
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
func (idx BasicSSTableIndex) GetOffset(key string) (offset uint) {
	return 0
}

// GetOffsetRange - get start, end offsets (in byte) of key range specified in the sstable file
// TODO: (p2)
func (idx BasicSSTableIndex) GetOffsetRange(start, end string) (startOffset, endOffset uint) {
	return 0, 0
}

// Serialize - turn the index data structure into bytes that can be stored on disk
func (idx BasicSSTableIndex) Serialize() ([]byte, error) {
	pbIdx := &pb.SSTableIndex{
		Data: idx,
	}

	data, err := proto.Marshal(pbIdx)
	if err != nil {
		return nil, err
	}
	return data, nil
}
