package dbengine

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func Test_NewSSTableShouldCreateNewFileWithUniqueTimestamp(t *testing.T) {
	s, _ := NewBasicSSTableWriter(os.TempDir(), 10)

	if _, err := os.Stat(s.File()); os.IsNotExist(err) {
		t.Errorf("file at path %s does not exist", s.File())
	}
}

func Test_DumpShouldWriteBothDataAndIndex(t *testing.T) {
	s, _ := NewBasicSSTableWriter(os.TempDir(), 50)
	fmt.Println(s.File())

	memtable := getTestMemtable(t, 100)
	s.Dump(memtable)

	// verify content
	sr, _ := NewBasicSSTableReader(s.File())

	value, err := sr.Get("key-055")
	if err != nil {
		t.Error(err.Error())
	}
	if string(value) != "value-055" {
		t.Errorf("Got %s instead", string(value))
	}

	idx := sr.Index()
	offset, size, exist := idx.GetOffset("key-055")
	if !exist || offset != 721 || size != 54 {
		t.Error("index didn't get written correctly")
	}
}

func Test_DumpShouldWriteDataAndIndexEvenIfTotalDataToWriteIsLessThanConfiguredBlockSize(t *testing.T) {
	s, _ := NewBasicSSTableWriter(os.TempDir(), 1024*400)
	fmt.Println(s.File())

	memtable := getTestMemtable(t, 100)
	s.Dump(memtable)

	// verify content
	sr, _ := NewBasicSSTableReader(s.File())

	value, err := sr.Get("key-055")
	if err != nil {
		t.Error(err.Error())
	}
	if string(value) != "value-055" {
		t.Errorf("Got %s instead", string(value))
	}

	idx := sr.Index()
	offset, size, exist := idx.GetOffset("key-055")
	if !exist || offset != binary.MaxVarintLen64 || size != 825 {
		t.Error("index didn't get written correctly")
	}
}

func Benchmark_DumpWith4KBDataBlock(b *testing.B) {
	m := getTestMemtable(b, b.N)
	s, _ := NewBasicSSTableWriter(os.TempDir(), 1024*4)

	s.Dump(m)

	b.Cleanup(func() {
		os.Remove(s.File())
	})
}

func Benchmark_Get(b *testing.B) {
	s := getBenchmarkSSTableFile(b, b.N)
	r, _ := NewBasicSSTableReader(s)

	for i := 0; i < b.N; i++ {
		val, err := r.Get(fmt.Sprintf("key-%03d", i))
		if val == nil || err != nil {
			b.Error("value couldn't be retrieved")
		}
	}

	b.Cleanup(func() {
		os.Remove(r.File())
	})
}

func getBenchmarkSSTableFile(b *testing.B, numberOfEntries int) string {
	b.Helper()

	m := getTestMemtable(b, numberOfEntries)
	s, _ := NewBasicSSTableWriter(os.TempDir(), 1024*4)
	s.Dump(m)

	return s.File()
}

func getTestMemtable(tb testing.TB, numberOfItems int) MemTable {
	tb.Helper()

	m := NewBasicMemTable(os.TempDir())
	for i := 0; i < numberOfItems; i++ {
		m.Write(
			fmt.Sprintf("key-%03d", i),
			[]byte(fmt.Sprintf("value-%03d", i)),
		)
	}
	return m
}

func Test_IndexUpdateShouldChangeExistingEntryIfExist(t *testing.T) {
	idx := getTestIndex(t)

	oldEnetry := idx.entries[0]
	// changed size
	idx.update("key-05", "key-09", 0, 1000)

	if oldEnetry.size != 1000 {
		t.Error("entry did not get updated")
	}
}

func Test_IndexUpdateShouldAddNewEntryIfNotExist(t *testing.T) {
	idx := getTestIndex(t)

	idx.update("key-150", "key-200", 1500, 2000)

	offset, size, exist := idx.GetOffset("key-150")

	if !exist || offset != 1500 || size != 2000 {
		t.Error("entry didn't get added")
	}
}

func Test_IndexGetOffsetShouldReturnDataBlockInfoIfKeyFallsInRange(t *testing.T) {
	idx := getTestIndex(t)

	offset, size, exist := idx.GetOffset("key-26")

	if !exist || offset != 20 || size != 100 {
		t.Error("Entry not exist or wrong entry returned")
	}
}

func Test_IndexGetOffsetShouldReturnNotExistIfKeyIsLessThanFirstDataBlock(t *testing.T) {
	idx := getTestIndex(t)

	_, _, exist := idx.GetOffset("key-01")

	if exist {
		t.Error("Entry shouldn't exist")
	}
}

func Test_IndexGetOffsetShouldReturnNotExistIfKeyIsGreaterThanLastDataBlock(t *testing.T) {
	idx := getTestIndex(t)

	_, _, exist := idx.GetOffset("key-105")

	if exist {
		t.Error("Entry shouldn't exist")
	}
}

func Test_IndexGetOffsetShouldReturnNotExistIfKeyIsInBetweenTwoDataBlocks(t *testing.T) {
	idx := getTestIndex(t)

	_, _, exist := idx.GetOffset("key-22")

	if exist {
		t.Error("Entry shouldn't exist")
	}
}

// getTestIndex - returns data blocks with starting and end keys every other 5 numbers. e.g.
// [key5, key10], [key15, key20], ... [key95, key100] so that we can test different scenarios
func getTestIndex(t *testing.T) *BasicSSTableIndex {
	t.Helper()

	idx := NewBasicSSTableIndex()

	for i := 0; i < 100; i += 10 {
		idx.update(fmt.Sprintf("key-%02d", i+5), fmt.Sprintf("key-%02d", i+10), uint64(i), 100)
	}
	return idx
}
