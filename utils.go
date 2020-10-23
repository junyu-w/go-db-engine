package dbengine

import (
	"encoding/binary"
	"io"
)

// WriteDataWithVarintSizePrefix - writes data to w with a varint size prefix
func WriteDataWithVarintSizePrefix(w io.Writer, raw []byte) (written int, err error) {
	buf := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(raw)))
	buf = buf[:n]

	written, err = w.Write(buf)
	if err != nil {
		return written, err
	}

	dataWritten, err := w.Write(raw)
	written += dataWritten
	if err != nil {
		return written, err
	}

	return written, nil
}

// VarintSizePrefixDataReader - a reader interface for varint prefixed data
type VarintSizePrefixDataReader interface {
	io.Reader
	io.ByteReader
}

// ReadDataWithVarintPrefix - read a varint prefixed data block into buf. If buf is too small
// a new allocated slice will be returned. It is also valid to pass `nil` for buf.
func ReadDataWithVarintPrefix(r VarintSizePrefixDataReader, buf []byte) ([]byte, error) {
	l, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if buf == nil || uint64(len(buf)) < l {
		buf = make([]byte, l, l)
	}
	r.Read(buf)

	return buf, nil
}
