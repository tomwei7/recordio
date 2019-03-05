package recordio

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"sync"
)

const maxReuseSize = 32 * 1 << 10

// CompressFacotry .
type CompressFacotry func(w io.Writer) io.WriteCloser

type writer struct {
	dst  io.Writer
	pool sync.Pool
}

// NewWriter new record writer without compress.
func NewWriter(w io.Writer) Writer {
	return &writer{
		dst:  w,
		pool: sync.Pool{New: func() interface{} { return &writeRecord{Buffer: &bytes.Buffer{}} }},
	}
}

// NewCompressWriter new record writer without compress.
func NewCompressWriter(w io.Writer) Writer {
	return &writer{
		dst: w,
		pool: sync.Pool{New: func() interface{} {
			buf := &bytes.Buffer{}
			return &writeRecord{Buffer: buf, compress: gzip.NewWriter(buf)}
		}},
	}
}

func (w *writer) NextRecord() io.WriteCloser {
	wr := w.pool.Get().(*writeRecord)
	wr.writer = w
	return wr
}

type writeRecord struct {
	head           [16]byte
	length         uint64
	compressLength uint64
	compress       *gzip.Writer

	*bytes.Buffer
	*writer
}

func (wr *writeRecord) Write(p []byte) (n int, err error) {
	if wr.compress == nil {
		n, err = wr.Buffer.Write(p)
		wr.length += uint64(n)
		return n, err
	}
	var cn int
	if cn, err = wr.compress.Write(p); err != nil {
		return cn, err
	}
	n = len(p)
	wr.length += uint64(n)
	wr.compressLength += uint64(cn)
	return
}

func (wr *writeRecord) Close() error {
	_, err := wr.WriteTo(wr.writer.dst)
	wr.pool.Put(wr)
	wr.length = 0
	wr.compressLength = 0
	if wr.Buffer.Len() > maxReuseSize {
		wr.Buffer = &bytes.Buffer{}
	}
	wr.Buffer.Reset()
	if wr.compress != nil {
		wr.compress.Reset(wr.Buffer)
	}
	return err
}

func (wr *writeRecord) WriteTo(w io.Writer) (n int, err error) {
	if wr.compress != nil {
		if err = wr.compress.Flush(); err != nil {
			return 0, err
		}
	}
	if _, err = w.Write(magicHead); err != nil {
		return
	}
	binary.BigEndian.PutUint64(wr.head[:8], wr.length)
	binary.BigEndian.PutUint64(wr.head[8:], wr.compressLength)
	if _, err = w.Write(wr.head[:]); err != nil {
		return
	}
	n, err = w.Write(wr.Bytes())
	return n + 20, err
}
