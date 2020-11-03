package dbengine

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

// TODO: (p3) implement saving of database configs & load an existing database

// Database - something that you can write data to and read data from
type Database struct {
	setting                *DBSetting
	walDir                 string
	sstableDir             string
	curMem                 MemTable
	memtablesToCompact     []MemTable    // the list of memtables that are going to be compacted
	memtableCompactionChan chan MemTable // the channel handles the compactions tasks of converting memtable into sstable
}

// SSTableFileMetadata - metadata about sstable file
type SSTableFileMetadata struct {
	filename     string
	size         int64
	lastModified time.Time
}

// NewDatabase - creates a new database instance
func NewDatabase(configs ...DBConfig) (*Database, error) {
	setting := generateDBSetting(configs...)
	walDir := filepath.Join(setting.DBDir, "wal")
	sstableDir := filepath.Join(setting.DBDir, "sstable")

	if err := os.Mkdir(walDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(sstableDir, 0700); err != nil {
		return nil, err
	}

	db := &Database{
		setting:                setting,
		walDir:                 walDir,
		sstableDir:             sstableDir,
		curMem:                 NewBasicMemTable(walDir),
		memtablesToCompact:     make([]MemTable, 0),
		memtableCompactionChan: make(chan MemTable),
	}

	if err := db.setupLogging(); err != nil {
		return nil, err
	}

	go db.memtableCompactionLoop()
	go db.sstableFileCompactionLoop()

	return db, nil
}

// setupLogging - setup logging for the database
func (db *Database) setupLogging() error {
	file, err := os.OpenFile(filepath.Join(db.setting.DBDir, "db.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	log.SetOutput(file)
	log.SetLevel(db.setting.LogLevel)
	return nil
}

// getAllSSTableFileMetadata - get all sstable files metadata in reverse chronological order (latest first)
func (db *Database) getAllSSTableFileMetadata() ([]*SSTableFileMetadata, error) {
	files, err := ioutil.ReadDir(db.sstableDir)
	if err != nil {
		return nil, err
	}

	allMeta := make([]*SSTableFileMetadata, len(files))
	for idx, file := range files {
		allMeta[len(files)-idx-1] = &SSTableFileMetadata{
			filename:     file.Name(),
			size:         file.Size(),
			lastModified: file.ModTime(),
		}
	}
	return allMeta, nil
}

// memtableCompactionLoop - handles compacting memtable into sstable files into disk (a.k.a "minor compaction")
func (db *Database) memtableCompactionLoop() {
	for {
		select {
		case mem := <-db.memtableCompactionChan:
			if err := db.serializeMemtable(mem); err != nil {
				log.Fatalf("Failed to serialize memtable to sstable - Error: %s", err.Error())
			}

			// delete the WAL since the wal isn't needed anymore for a memtable that's serialized already
			if err := mem.Wal().Delete(); err != nil {
				log.Warnf("Failed to delete WAL file %s after serializing its corresponding memtable - Error: %s", mem.Wal().File().Name(), err.Error())
			}
			log.Infof("Deleted WAL file %s", mem.Wal().File().Name())
		}
	}
}

// sstableFileCompactionLoop - compacting smaller sstable files into larger file (a.k.a "major compaction")
// TODO: (p1) implement sstable compaction
func (db *Database) sstableFileCompactionLoop() {}

func (db *Database) serializeMemtable(mem MemTable) error {
	writer, err := NewBasicSSTableWriter(db.sstableDir, db.setting.SStableDatablockSizeByte)
	if err != nil {
		return err
	}
	if err = writer.Dump(mem); err != nil {
		return err
	}
	log.Infof("Serialized memtable to sstable at %s", writer.File())
	return nil
}

// Get - read value for key from the database
func (db *Database) Get(key string) ([]byte, error) {
	// Try to read first from the current memtable
	value := db.curMem.Get(key)
	if value != nil {
		return value, nil
	}

	// Try to read from the memtables that are in queue for serialization
	for _, memToSerialize := range db.memtablesToCompact {
		value = memToSerialize.Get(key)
		if value != nil {
			return value, nil
		}
	}

	// if still no luck, iterate through the sstable files from latest to earliest
	metas, err := db.getAllSSTableFileMetadata()
	if err != nil {
		return nil, err
	}

	for _, meta := range metas {
		reader, err := NewBasicSSTableReader(filepath.Join(db.sstableDir, meta.filename))
		if err != nil {
			return nil, err
		}
		value, err = reader.Get(key)
		if err != nil || value != nil {
			return value, err
		}
	}
	return nil, nil
}

// Write - write value into the database
func (db *Database) Write(key string, value []byte) error {
	if err := db.curMem.Write(key, value); err != nil {
		return err
	}

	sizeAfterWrite := db.curMem.SizeBytes()
	// when memtable has grown over threshold, send it for serialization
	if db.curMem.SizeBytes() >= uint32(db.setting.MemtableSizeByte) {
		db.memtableCompactionChan <- db.curMem
		db.curMem = NewBasicMemTable(db.walDir)

		log.Infof(
			"Memtable has exceeded size limit (size: %d, limit: %d). Enqueued for serialization to sstable",
			sizeAfterWrite,
			db.setting.MemtableSizeByte,
		)
	}
	return nil
}

// Delete - delete a key from the database
// TODO: (p1) make the deletion behavior correct across from memtable and sstable
func (db *Database) Delete(key string) error {
	if err := db.curMem.Delete(key); err != nil {
		return err
	}
	return nil
}
