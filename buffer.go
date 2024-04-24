package memdproto

import (
	"bytes"
	"strings"
)

type readbuf struct {
	data  []byte
	nread int
}

func (rb *readbuf) Advance() {
	rb.data = rb.data[1:]
	rb.nread += 1
}

func (rb *readbuf) Len() int {
	return len(rb.data)
}

func (rb *readbuf) NRead() int {
	return rb.nread
}

func (rb *readbuf) ReadToken() string {
	var val strings.Builder
	for rb.Len() > 0 && rb.data[0] != ' ' {
		val.WriteByte(rb.data[0])
		rb.Advance()
	}
	return val.String()
}

func (rb *readbuf) ReadTokenBytes() []byte {
	var val bytes.Buffer
	for rb.Len() > 0 && rb.data[0] != ' ' {
		val.WriteByte(rb.data[0])
		rb.Advance()
	}
	return val.Bytes()
}
