package memdproto

import (
	"encoding"
	"io"
)

var crlf = []byte("\r\n")
var space = []byte{' '}
var end = []byte("END")
var setCmdName = []byte("set")
var addCmdName = []byte("add")
var casCmdName = []byte("cas")
var appendCmdName = []byte("append")
var prependCmdName = []byte("prepend")
var replaceCmdName = []byte("replace")

type Cmd interface {
	io.WriterTo
	encoding.TextUnmarshaler
}

type Reply interface {
	io.WriterTo
	encoding.TextUnmarshaler
}
