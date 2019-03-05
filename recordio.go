package recordio

import (
	"io"
)

var magicHead = []byte{0x3e, 0xd7, 0x23, 0x0a}

// Reader record io reader
type Reader interface {
	io.Reader
	Next() bool
}

// Writer record io writer
type Writer interface {
	NextRecord() io.WriteCloser
}
