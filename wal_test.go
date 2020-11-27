package dbengine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"testing/iotest"
)

// TestFile implements `WalFile` but with easy substituion for io.Reader and io.Writer to simulate failure scenarios
type TestFile struct {
	io.Reader
	io.Writer
	*os.File
}

func newTestFile(t *testing.T, r io.Reader, w io.Writer) *TestFile {
	t.Helper()

	f, err := NewWalFile(os.TempDir(), true)
	if err != nil {
		panic(err)
	}
	return &TestFile{
		Reader: r,
		Writer: io.MultiWriter(w, f), // have the write go to both the `Writer` and the `File`
		File:   f,
	}
}

func (tf *TestFile) Read(p []byte) (int, error)  { return tf.Reader.Read(p) }
func (tf *TestFile) Write(p []byte) (int, error) { return tf.Writer.Write(p) }
func (tf *TestFile) Stat() (os.FileInfo, error)  { return tf.File.Stat() }
func (tf *TestFile) Truncate(size int64) error   { return tf.File.Truncate(size) }
func (tf *TestFile) Name() string                { return tf.File.Name() }

// BadTruncateWriter - extends TruncateWriter behavior, it writes bytes up to `size` and fail
// with `errDeviceFull`
type BadTruncateWriter struct {
	io.Writer
	size    int64
	written int64
}

var errDeviceFull error = fmt.Errorf("Device full")

func (bw *BadTruncateWriter) Write(p []byte) (int, error) {
	truncateAt := bw.size - bw.written

	w := iotest.TruncateWriter(bw.Writer, truncateAt)
	written, err := w.Write(p)
	if err != nil {
		return written, err
	}
	if int64(written) >= truncateAt {
		return written, errDeviceFull
	}

	bw.written += int64(written)
	return written, nil
}

func badTruncateWriter(t *testing.T, w io.Writer, size int64) io.Writer {
	t.Helper()
	return &BadTruncateWriter{w, size, 0}
}

func Test_CreateNewWalFileShouldCreateFile(t *testing.T) {
	f, err := NewWalFile(os.TempDir(), true)
	if err != nil {
		t.Error(err)
	}

	if _, err = os.Stat(f.Name()); os.IsNotExist(err) {
		t.Errorf("file %s does not exist", f.Name())
	}
}

func Test_CreateNewWalFileShouldFailIfFileFailedToCreate(t *testing.T) {}

func Test_AppendShouldAppendNewLog(t *testing.T) {
	f, err := NewWalFile(os.TempDir(), true)
	if err != nil {
		t.Error(err)
	}
	wal := &BasicWal{file: f}
	data := []byte("1234567890")
	if err = wal.Append(data); err != nil {
		t.Error(err)
	}

	log := BasicWalLog{seq: 1, data: data}

	fileContent, _ := ioutil.ReadFile(f.Name())
	fileData, _ := ReadDataWithVarintPrefix(bytes.NewReader(fileContent), nil)
	expected, _ := log.Serialize()
	if string(fileData) != string(expected) {
		t.Errorf("Incorrect file content - %s", string(fileData))
	}

	if wal.seq != 1 {
		t.Errorf("Seqeuence number isn't incremented")
	}
}

func Test_AppendShouldFailIfFileDoesNotExist(t *testing.T) {}

func Test_AppendShouldRollbackIfLogNotFullyWritten(t *testing.T) {
	buf := new(bytes.Buffer)
	// create a bad truncate writer that errors after writing 5 bytes
	w := badTruncateWriter(t, buf, 5)
	testFile := newTestFile(t, buf, w)

	// add some original content to the underlying file before writing logs
	testFile.File.Write([]byte("old content"))

	fileName := testFile.Name()
	oldContent, _ := ioutil.ReadFile(fileName)
	fmt.Println(fileName)

	wal := &BasicWal{file: testFile}
	err := wal.Append([]byte("1234567890"))

	// check correct error type & content written
	var walErr *WalError
	if errors.As(err, &walErr) {
		if walErr.Op != OP_WAL_APPEND || walErr.BeforeLastSeq != 0 || walErr.Err != errDeviceFull {
			t.Errorf("Unexpected error returned - Error: %s", walErr)
		}
	}
	if len(buf.Bytes()) != 5 {
		t.Errorf("Incorrect content written to buffer, should be truncated - content: %s", buf.String())
	}

	// check if file has been rolledback
	fileContent, _ := ioutil.ReadFile(fileName)
	if string(oldContent) != string(fileContent) {
		t.Errorf("Content should have been rolled back, instead it is - %s", string(fileContent))
	}
}

func Test_AppendShouldFailIfLogWriteFailed(t *testing.T) {}

func Test_AppendShouldSupportConcurrentWrite(t *testing.T) {}

func Test_DeleteShouldLockTheFileFromBeingWritten(t *testing.T) {}
