package recordio

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var magicHead = []byte{0x3e, 0xd7, 0x23, 0x0a}
var magicHeadLen = len(magicHead)

const maxReuseSize = 1 << 18 // 256KB max reuse size
const bufSize = 1 << 16      // 64KB Fastest!

// Reader record io reader
type Reader interface {
	Read(p []byte) (n int, err error)
	Next() bool
}

// Writer record io writer
type Writer interface {
	NewRecord() *Record
	WriteRecord(r *Record) error
	Close() error
}

// Record single record
type Record struct {
	header      [20]byte
	length      int
	writer      io.Writer
	buffer      *bytes.Buffer
	initialized bool
	compressed  bool
	gzipWriter  *gzip.Writer
}

func (r *Record) Write(p []byte) (n int, err error) {
	n, err = r.writer.Write(p)
	r.length += n
	return n, err
}

func (r *Record) reset() {
	if r.length > maxReuseSize {
		r.buffer = &bytes.Buffer{}
	}
	r.initialized = false
}

func (r *Record) init() {
	r.buffer.Reset()
	r.writer = r.buffer
	r.length = 0
	r.initialized = true
	r.compressed = false
}

// SetCompress set compress
func (r *Record) SetCompress() error {
	if r.length > 0 {
		return errors.New("can't set compress after record has be write")
	}
	r.gzipWriter.Reset(r.buffer)
	r.writer = r.gzipWriter
	r.compressed = true
	return nil
}

func (r *Record) writeTo(w io.Writer) error {
	var clength int
	if r.compressed {
		if err := r.gzipWriter.Close(); err != nil {
			return err
		}
		clength = r.buffer.Len()
	}
	binary.BigEndian.PutUint64(r.header[4:], uint64(r.length))
	binary.BigEndian.PutUint64(r.header[12:], uint64(clength))
	_, err := w.Write(r.header[:])
	if err != nil {
		return err
	}
	_, err = w.Write(r.buffer.Bytes())
	return err
}

type writer struct {
	dst  *bufio.Writer
	pool sync.Pool
}

// NewWriter .
func NewWriter(w io.Writer) Writer {
	dst, ok := w.(*bufio.Writer)
	if !ok {
		dst = bufio.NewWriterSize(w, bufSize)
	}
	return &writer{dst: dst, pool: sync.Pool{New: func() interface{} {
		r := &Record{buffer: &bytes.Buffer{}, gzipWriter: gzip.NewWriter(nil)}
		copy(r.header[:], magicHead)
		return r
	}}}
}

func (w *writer) NewRecord() *Record {
	r := w.pool.Get().(*Record)
	r.init()
	return r
}

func (w *writer) WriteRecord(r *Record) error {
	if !r.initialized {
		return errors.New("record not initialized, use Writer.NewRecord instead of new Record instance by you self")
	}
	err := r.writeTo(w.dst)
	r.reset()
	w.pool.Put(r)
	return err
}

func (w *writer) Close() error {
	return w.dst.Flush()
}

type reader struct {
	reader     io.Reader
	gzipReader *gzip.Reader
	src        *bufio.Reader
	buffer     *bytes.Buffer
	err        error
}

// NewReader NewReader
func NewReader(r io.Reader) Reader {
	src, ok := r.(*bufio.Reader)
	if !ok {
		src = bufio.NewReaderSize(r, bufSize)
	}
	return &reader{src: src, buffer: &bytes.Buffer{}}
}

func (r *reader) Next() bool {
	// NOTE: it look like unnecessary to close gzip.Reader
	//if r.gzipReader != nil {
	//	if r.err = r.gzipReader.Close(); r.err != nil {
	//		return true
	//	}
	//}
	if r.buffer.Len() > maxReuseSize {
		r.buffer = &bytes.Buffer{}
	}
	r.buffer.Reset()
	var data []byte
	for {
		if data, r.err = r.src.Peek(magicHeadLen); r.err != nil {
			return r.err != io.EOF
		}
		if bytes.Equal(data, magicHead[:]) {
			r.readRecord()
			return true
		}
		if _, r.err = r.src.Discard(1); r.err != nil {
			return true
		}
	}
}

func (r *reader) readRecord() {
	var head [20]byte
	_, r.err = r.src.Read(head[:])
	if r.err != nil {
		if r.err == io.EOF {
			r.err = io.ErrUnexpectedEOF
		}
		return
	}
	length := binary.BigEndian.Uint64(head[4:])
	clength := binary.BigEndian.Uint64(head[12:])
	if clength == 0 {
		if _, r.err = io.CopyN(r.buffer, r.src, int64(length)); r.err != nil {
			r.err = io.ErrUnexpectedEOF
		}
		r.reader = r.buffer
		return
	}
	if _, r.err = io.CopyN(r.buffer, r.src, int64(clength)); r.err != nil {
		r.err = io.ErrUnexpectedEOF
	}
	if r.gzipReader == nil {
		r.gzipReader, r.err = gzip.NewReader(r.buffer)
	} else {
		r.err = r.gzipReader.Reset(r.buffer)
	}
	r.reader = r.gzipReader
	return
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.reader.Read(p)
}
