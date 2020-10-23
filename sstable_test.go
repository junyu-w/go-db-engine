package dbengine

import (
	"fmt"
	"os"
	"testing"
)

func Test_NewSSTableShouldCreateNewFileWithUniqueTimestamp(t *testing.T) {
	s := NewBasicSSTable(os.TempDir(), 10)

	if _, err := os.Stat(s.File()); os.IsNotExist(err) {
		t.Errorf("file at path %s does not exist", s.File())
	}
}

func Test_DumpShouldWriteBothDataAndIndex(t *testing.T) {
	s := NewBasicSSTable(os.TempDir(), 50)
	fmt.Println(s.File())

	memtable := getTestMemtable(t, 100)
	// TODO: add test that verifies content
	s.Dump(memtable)
}

func Test_DumpShouldWriteDataAndIndexEvenIfTotalDataToWriteIsLessThanConfiguredBlockSize(t *testing.T) {
	s := NewBasicSSTable(os.TempDir(), 1000)
	fmt.Println(s.File())

	memtable := getTestMemtable(t, 10)
	// TODO: add test that verifies content
	s.Dump(memtable)
}

func getTestMemtable(t *testing.T, numberOfItems int) MemTable {
	t.Helper()

	m := NewBasicMemTable(os.TempDir())
	for i := 0; i < numberOfItems; i++ {
		m.Write(
			fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
	}
	return m
}
