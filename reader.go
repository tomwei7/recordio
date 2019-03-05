package recordio

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

type reader struct {
	nextErr  error
	bufsrc   *bufio.Reader
	compress *gzip.Reader

	length  uint64
	clength uint64
	offset  uint64
	coffset uint64

	lenHead [20]byte
}

// NewReader new recoid reader.
func NewReader(r io.Reader) Reader {
	bufsrc, ok := r.(*bufio.Reader)
	if !ok {
		bufsrc = bufio.NewReader(r)
	}
	return &reader{bufsrc: bufsrc}
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.nextErr != nil {
		return 0, r.nextErr
	}

	var read io.Reader
	if r.clength == 0 {
		read = r.bufsrc
	} else {
		read = r.compress
	}

	if r.offset >= r.length {
		return 0, io.EOF
	}
	if uint64(len(p)) > r.length-r.offset {
		p = p[:r.length-r.offset]
	}
	n, err = read.Read(p)
	r.offset += uint64(n)
	if err == io.EOF && r.offset != r.length {
		return n, fmt.Errorf("unexpected EOF record length: %d read: %d", r.length, r.offset)
	}
	return
}

func (r *reader) Next() bool {
	var head []byte
	for {
		head, r.nextErr = r.bufsrc.Peek(4)
		if r.nextErr != nil {
			if r.nextErr == io.EOF {
				return false
			}
			break
		}
		if bytes.Equal(head, magicHead) {
			break
		}
		if _, r.nextErr = r.bufsrc.Discard(1); r.nextErr != nil {
			break
		}
		continue
	}
	_, r.nextErr = r.bufsrc.Read(r.lenHead[:])
	r.decodeHead()

	r.offset = 0
	r.coffset = 0
	if r.clength != 0 {
		r.nextErr = r.compress.Reset(readerFunc(func(p []byte) (n int, err error) {
			if r.coffset >= r.clength {
				return 0, io.EOF
			}
			if uint64(len(p)) > r.clength-r.coffset {
				p = p[:r.clength-r.coffset]
			}
			n, err = r.bufsrc.Read(p)
			r.coffset += uint64(n)
			if err == io.EOF && r.clength != r.coffset {
				return n, fmt.Errorf("unexpected EOF compressed record clength: %d cread: %d", r.clength, r.coffset)
			}
			return
		}))
	}
	return true
}

func (r *reader) decodeHead() {
	r.length = binary.BigEndian.Uint64(r.lenHead[4:])
	r.clength = binary.BigEndian.Uint64(r.lenHead[12:])
}

type readerFunc func([]byte) (int, error)

func (w readerFunc) Read(p []byte) (n int, err error) {
	return w(p)
}
