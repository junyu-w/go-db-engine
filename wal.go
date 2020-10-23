package dbengine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/DrakeW/go-db-engine/pb"
	"google.golang.org/protobuf/proto"
)

// Wal - represents a write-ahead-log
type Wal interface {
	// Append - append an operation log to the WAL file
	Append([]byte) error

	// TODO: (P1) Replay - returns a channel of operations logs, EOF indicates we've reached the end of the WAL
	// Replay() <-chan []byte

	// Delete - delete the WAL file
	Delete() error

	// File -- returns the underlying WAL file
	File() WalFile
}

// WalFile - file interface that defines basic methods needed for WAL operations
// the interface can be satisfied by `os.File`
type WalFile interface {
	io.Writer
	io.Reader

	Truncate(int64) error
	Stat() (os.FileInfo, error)
	Name() string
}

const (
	OP_CREATE_FILE = "OP_CREATE_FILE"
	OP_READ_FILE   = "OP_READ_FILE"
	OP_APPEND      = "OP_APPEND"
	OP_ROLLBACK    = "OP_ROLLBACK"
	OP_DELETE      = "OP_DELETE"
)

// WalError - wraps errors with WAL operation and basic information before the error happens
type WalError struct {
	Op            string
	BeforeLastSeq uint32
	Err           error
}

func (walErr WalError) Error() string {
	return fmt.Sprintf(
		"WAL operation (code %s) failed - Error: %s. Latest successful sequence: %d",
		walErr.Op, walErr.Err.Error(), walErr.BeforeLastSeq,
	)
}

func (walErr WalError) Unwrap() error {
	return walErr.Err
}

// BasicWal - implements the `Wal` interface
type BasicWal struct {
	lock sync.Mutex
	// file is the opened underlying file
	file WalFile
	// seq is the sequence number of the latest written log
	seq uint32
}

// BasicWalLog - represents a WAL log record
type BasicWalLog struct {
	seq  uint32
	data []byte
}

// Serialize - turn the WAL log into bytes
func (l *BasicWalLog) Serialize() ([]byte, error) {
	log := &pb.WalLog{
		Seq:  l.seq,
		Data: l.data,
	}
	logData, err := proto.Marshal(log)
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize WAL log - Error: %w", err)
	}
	return logData, nil
}

// NewBasicWal - creates a new WAL instance and an underlying WAL file
// errors out if file with same name already exists (no WAL file reuse between `BasicWal` instances)
func NewBasicWal(walDir string) (*BasicWal, error) {
	f, err := NewWalFile(walDir)
	if err != nil {
		return nil, err
	}

	return &BasicWal{
		file: f,
	}, nil
}

// NewWalFile - creates a new WAL file with name "wal_<unix timestamp>" under `walDir`
func NewWalFile(walDir string) (*os.File, error) {
	ts := time.Now().UnixNano()
	filename := filepath.Join(walDir, fmt.Sprintf("wal_%d", ts))
	// os.O_CREATE|os.O_EXCL - create file only when it doesn't exist, error out otherwise
	// os.O_RDWR - open for read & write
	// os.O_SYNC - enable synchronous IO (write always flush to underlying hardware, like "write" + "fsync")
	// os.O_APPEND - file is open for APPEND only (no seeking needed)
	// TODO: (P3) Since O_SYNC degrade perf, maybe it could be an optional flag for the user to determine how safe they want their system to be during crash. For some battery powered hardware, even when the OS crashes or machined died (powered-off), the file system cache can still be flushed to the underlying hardware
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		return nil, &WalError{
			Op:            OP_CREATE_FILE,
			BeforeLastSeq: 0,
			Err:           err,
		}
	}
	return f, nil
}

// Append - append an operation log to the WAL file
func (wal *BasicWal) Append(log []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	fileInfo, err := wal.file.Stat()
	if err != nil {
		return &WalError{
			Op:            OP_READ_FILE,
			BeforeLastSeq: wal.seq,
			Err:           err,
		}
	}
	oldSize := fileInfo.Size()

	newLog := &BasicWalLog{
		seq:  wal.seq + 1,
		data: log,
	}
	logBytes, err := newLog.Serialize()
	if err != nil {
		return &WalError{
			Op:            OP_APPEND,
			BeforeLastSeq: wal.seq,
			Err:           err,
		}
	}

	if _, err := WriteDataWithVarintSizePrefix(wal.file, logBytes); err != nil {
		if rollbackErr := wal.rollback(oldSize); rollbackErr != nil {
			return rollbackErr
		}
		return &WalError{
			Op:            OP_APPEND,
			BeforeLastSeq: wal.seq,
			Err:           err,
		}
	}

	wal.seq = newLog.seq
	return nil
}

func (wal *BasicWal) rollback(size int64) error {
	if err := wal.file.Truncate(size); err != nil {
		return &WalError{
			Op:            OP_ROLLBACK,
			BeforeLastSeq: wal.seq,
			Err:           err,
		}
	}
	return nil
}

// File -- returns the underlying WAL file
func (wal *BasicWal) File() WalFile {
	return wal.file
}

// Delete - delete the WAL file
func (wal *BasicWal) Delete() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := os.Remove(wal.file.Name()); err != nil {
		return &WalError{
			Op:            OP_DELETE,
			BeforeLastSeq: wal.seq,
			Err:           err,
		}
	}
	return nil
}
