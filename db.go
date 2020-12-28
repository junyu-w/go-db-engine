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
	setting    *DBSetting
	walDir     string
	sstableDir string
	curMem     MemTable
	memSvc     *memtableCompactService
	compactSvc *sstableCompactService
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
		setting:    setting,
		walDir:     walDir,
		sstableDir: sstableDir,
		curMem:     NewBasicMemTable(walDir, setting.WalStrictModeOn),
	}

	db.memSvc = newMemtableCompactService(db)
	db.compactSvc = newSSTableCompactService(db)

	if err := db.setupLogging(); err != nil {
		return nil, err
	}

	go db.memSvc.start()
	go db.compactSvc.start()

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

// Get - read value for key from the database
func (db *Database) Get(key string) ([]byte, error) {
	// Try to read first from the current memtable
	value := db.curMem.Get(key)
	if value != nil {
		return value, nil
	}

	// Try to read from the memtables that are in queue for serialization
	for _, memToSerialize := range db.memSvc.getQueuedTables() {
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
		// TODO: (p2) cache the opened reader using an LRU cache to improve performance
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
		db.memSvc.enqueue(db.curMem)
		db.curMem = NewBasicMemTable(db.walDir, db.setting.WalStrictModeOn)

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
