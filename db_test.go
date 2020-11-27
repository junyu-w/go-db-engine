package dbengine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func setupTestDBDir(tb testing.TB) string {
	tb.Helper()

	dirpath := filepath.Join(os.TempDir(), fmt.Sprintf("test-db-%d", time.Now().UnixNano()))
	if err := os.Mkdir(dirpath, 0744); err != nil {
		panic(err)
	}
	return dirpath
}

func Test_dbInit(t *testing.T) {
	testDBDir := setupTestDBDir(t)

	_, err := NewDatabase(
		ConfigDBDir(testDBDir),
		ConfigWalStrictMode(true),
	)
	if err != nil {
		t.Errorf("Failed to initialize database - Error: %s", err.Error())
	}
}

func Test_dbWriteAndMemtableCompaction(t *testing.T) {
	testDBDir := setupTestDBDir(t)

	db, err := NewDatabase(
		ConfigDBDir(testDBDir),
		ConfigWalStrictMode(true),
		ConfigMemtableSizeByte(512),
		ConfigSStableDatablockSizeByte(512/4),
		ConfigLogLevel(log.InfoLevel),
	)
	if err != nil {
		t.Errorf("Failed to initialize database - Error: %s", err.Error())
	}

	for i := 0; i < 1000; i++ {
		db.Write(
			fmt.Sprintf("key-%03d", i),
			[]byte(fmt.Sprintf("value-%03d", i)),
		)
	}

	// test if the correct number of sstables are created bsaed on the configuration
	// with 512 byte memtable, and 1000 key-value pair that sums to a total of roughly 16 * 1000 bytes
	// we should have about 16 * 1000 / 512 -> 31 sstable files
	expectedNumberOfSSTables := (len("key-000") + len("value-000")) * 1000 / 512

	allMeta, err := db.getAllSSTableFileMetadata()
	if err != nil {
		t.Errorf("Failed to get sstable files metadata - Error: %s", err.Error())
	}
	if len(allMeta) != expectedNumberOfSSTables {
		t.Errorf("The incorrect number of sstable files were written. Expected %d, got %d", expectedNumberOfSSTables, len(allMeta))
	}
}

func Test_dbGet(t *testing.T) {
	testDBDir := setupTestDBDir(t)

	db, err := NewDatabase(
		ConfigDBDir(testDBDir),
		ConfigWalStrictMode(true),
		// make sure there is more than 1 sstable files generated
		ConfigMemtableSizeByte(512),
		// make sure each sstable contains multiple data blocks
		ConfigSStableDatablockSizeByte(512/4),
		ConfigLogLevel(log.InfoLevel),
	)
	if err != nil {
		t.Errorf("Failed to initialize database - Error: %s", err.Error())
	}

	for i := 0; i < 1000; i++ {
		db.Write(
			fmt.Sprintf("key-%03d", i),
			[]byte(fmt.Sprintf("value-%03d", i)),
		)
	}

	// read the contents back
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to read key %s from db - Error: %s", key, err.Error())
		}
		if value == nil {
			t.Errorf("Value for key %s not found", key)
		}
	}
}

func Benchmark_dbWrite(b *testing.B) {
	testDBDir := setupTestDBDir(b)
	// use default setting
	db, err := NewDatabase(
		ConfigDBDir(testDBDir),
		ConfigLogLevel(log.InfoLevel),
	)
	if err != nil {
		b.Errorf("Failed to initialize database - Error: %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		db.Write(
			fmt.Sprintf("key-%05d", i),
			[]byte(fmt.Sprintf("value-%05d", i)),
		)
	}
}

func Benchmark_dbWriteStrictModeOn(b *testing.B) {
	testDBDir := setupTestDBDir(b)
	// use default setting
	db, err := NewDatabase(
		ConfigWalStrictMode(true),
		ConfigDBDir(testDBDir),
		ConfigLogLevel(log.InfoLevel),
	)
	if err != nil {
		b.Errorf("Failed to initialize database - Error: %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		db.Write(
			fmt.Sprintf("key-%05d", i),
			[]byte(fmt.Sprintf("value-%05d", i)),
		)
	}
}
