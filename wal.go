package dbengine

// Wal - represents a write-ahead-log
type Wal interface {
	AppendLog([]byte)
	Replay() <-chan []byte
}
