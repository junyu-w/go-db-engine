package dbengine

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// memtableCompactService - handles compacting memtable into sstable files into disk (a.k.a "minor compaction")
type memtableCompactService struct {
	db    *Database
	queue []MemTable
	c     chan MemTable
}

func newMemtableCompactService(db *Database) *memtableCompactService {
	return &memtableCompactService{
		db:    db,
		queue: make([]MemTable, 0),
		c:     make(chan MemTable),
	}
}

// enqueue - add the input memtable to the compaction queue for async compaction at a later time
func (mcs *memtableCompactService) enqueue(mem MemTable) {
	mcs.c <- mem
	mcs.queue = append(mcs.queue, mem)
}

// getQueuedTables - get all the memtables that are in the compaction queue but not yet compacted
// those tables should continue to serve get request before being serialized to disk.
func (mcs *memtableCompactService) getQueuedTables() []MemTable {
	return mcs.queue
}

// start - start the service to handle compaction tasks
func (mcs *memtableCompactService) start() {
	for {
		select {
		case mem := <-mcs.c:
			if err := mcs.serializeMemtable(mem); err != nil {
				log.Fatalf("Failed to serialize memtable to sstable - Error: %s", err.Error())
			}
			mcs.queue = mcs.queue[1:]

			// delete the WAL since the wal isn't needed anymore for a memtable that's serialized already
			if err := mem.Wal().Delete(); err != nil {
				log.Warnf("Failed to delete WAL file %s after serializing its corresponding memtable - Error: %s", mem.Wal().File().Name(), err.Error())
			}
			log.Infof("Deleted WAL file %s", mem.Wal().File().Name())
		}
	}
}

// serializeMemtable - serialize the input memtable into a sstable file
func (mcs *memtableCompactService) serializeMemtable(mem MemTable) error {
	writer, err := NewBasicSSTableWriter(mcs.db.sstableDir, mcs.db.setting.SStableDatablockSizeByte)
	if err != nil {
		return err
	}
	if err = writer.Dump(mem); err != nil {
		return err
	}
	log.Infof("Serialized memtable to sstable at %s", writer.File())
	return nil
}

// sstableCompactService - compacting smaller sstable files into larger file (a.k.a "major compaction")
type sstableCompactService struct {
	db       *Database
	interval time.Duration
	lastRun  time.Time
}

func newSSTableCompactService(db *Database) *sstableCompactService {
	return &sstableCompactService{
		db:       db,
		interval: 5 * time.Second,
		lastRun:  time.Now(),
	}
}

// TODO: (p0) implement sstable compaction
func (scs *sstableCompactService) start() {}
