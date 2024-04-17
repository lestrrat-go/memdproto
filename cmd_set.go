package memdproto

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

type storageCmd struct {
	cmdName string
	key     string
	flags   uint16
	expires int64
	noreply bool
	data    []byte
	cas     uint64
}

func (cmd *storageCmd) SetFlags(flags uint16) *storageCmd {
	cmd.flags = flags
	return cmd
}

func (cmd *storageCmd) SetExpires(expires int64) *storageCmd {
	cmd.expires = expires
	return cmd
}

func (cmd *storageCmd) SetNoReply(noreply bool) *storageCmd {
	cmd.noreply = noreply
	return cmd
}

// protected from commands other than CasCmd
func (cmd *storageCmd) setCas(cas uint64) *storageCmd {
	cmd.cas = cas
	return cmd
}

func (cmd *storageCmd) Reset() {
	cmd.key = ""
	cmd.flags = 0
	cmd.expires = 0
	cmd.noreply = false
	cmd.data = nil
	cmd.cas = 0
}

func (cmd *storageCmd) WriteTo(dst io.Writer) (int64, error) {
	var written int64

	n, err := fmt.Fprintf(dst, "%s %s %d %d %d", cmd.cmdName, cmd.key, cmd.flags, cmd.expires, len(cmd.data))
	written += int64(n)
	if err != nil {
		return written, err
	}

	if cmd.cmdName == "cas" {
		n, err := fmt.Fprintf(dst, " %d", cmd.cas)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	if cmd.noreply {
		n, err := fmt.Fprintf(dst, " noreply")
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	n, err = dst.Write(crlf)
	written += int64(n)
	if err != nil {
		return written, err
	}

	n, err = dst.Write(cmd.data)
	written += int64(n)
	if err != nil {
		return written, err
	}

	n, err = dst.Write(crlf)
	written += int64(n)
	return written, err
}

func (cmd *storageCmd) UnmarshalText(data []byte) error {
	// set, cas, add, append, prepend, replace (3, 3, 3, 6, 7, 7) bytes
	ldata := len(data)
	if ldata < 4 {
		return fmt.Errorf("invalid storage command")
	}

	switch data[0] {
	case 's':
		if !bytes.Equal(data[:3], setCmdName) {
			return fmt.Errorf("invalid storage command: expected set command")
		}
		cmd.cmdName = "set"
		data = data[3:]
	case 'c':
		if !bytes.Equal(data[:3], casCmdName) {
			return fmt.Errorf("invalid storage command")
		}
		cmd.cmdName = "cas"
		data = data[3:]
	case 'a':
		if ldata > 6 && bytes.Equal(data[:6], appendCmdName) {
			cmd.cmdName = "append"
			data = data[6:]
		} else {
			if !bytes.Equal(data[:3], addCmdName) {
				return fmt.Errorf("invalid storage command")
			}
			cmd.cmdName = "add"
			data = data[3:]
		}
	case 'r':
		if ldata < 7 && !bytes.Equal(data[:6], replaceCmdName) {
			return fmt.Errorf("invalid storage command")
		}
		cmd.cmdName = "replace"
		data = data[7:]
	case 'p':
		if ldata < 7 && !bytes.Equal(data[:7], prependCmdName) {
			return fmt.Errorf("invalid storage command")
		}
		data = data[7:]
	default:
		return fmt.Errorf("invalid storage command: unknown command")
	}

	// must be a space
	if len(data) < 1 || data[0] != ' ' {
		return fmt.Errorf("invalid storage command: missing space after command")
	}
	data = data[1:]

	var sb strings.Builder
	for len(data) > 0 {
		if data[0] == ' ' || data[0] > unicode.MaxASCII || unicode.IsControl(rune(data[0])) {
			break
		}
		sb.WriteByte(data[0])
		data = data[1:]
	}
	if sb.Len() == 0 {
		return fmt.Errorf("invalid storage command: missing key")
	}
	cmd.key = sb.String()

	// must be a space
	if len(data) < 1 || data[0] != ' ' {
		return fmt.Errorf("invalid storage command")
	}
	data = data[1:]

	// flags
	sb.Reset()
	for len(data) > 0 {
		if data[0] == ' ' || data[0] > unicode.MaxASCII || unicode.IsControl(rune(data[0])) {
			break
		}
		sb.WriteByte(data[0])
		data = data[1:]
	}
	if sb.Len() == 0 {
		return fmt.Errorf("invalid storage command: missing flags")
	}

	u16, err := strconv.ParseUint(sb.String(), 10, 16)
	if err != nil {
		return fmt.Errorf("invalid storage command: invalid flags")
	}
	cmd.flags = uint16(u16)

	// must be a space
	if len(data) < 1 || data[0] != ' ' {
		return fmt.Errorf("invalid storage command")
	}
	data = data[1:]

	// expires
	sb.Reset()
	for len(data) > 0 {
		if data[0] == ' ' || data[0] > unicode.MaxASCII || unicode.IsControl(rune(data[0])) {
			break
		}
		sb.WriteByte(data[0])
		data = data[1:]
	}
	if sb.Len() == 0 {
		return fmt.Errorf("invalid storage command: missing expires")
	}

	i64, err := strconv.ParseInt(sb.String(), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid storage command: invalid expires")
	}
	cmd.expires = i64

	// must be a space
	if len(data) < 1 || data[0] != ' ' {
		return fmt.Errorf("invalid storage command")
	}
	data = data[1:]

	// data length
	sb.Reset()
	for len(data) > 0 {
		if data[0] == ' ' || data[0] > unicode.MaxASCII || unicode.IsControl(rune(data[0])) {
			break
		}
		sb.WriteByte(data[0])
		data = data[1:]
	}
	if sb.Len() == 0 {
		return fmt.Errorf("invalid storage command: missing data length")
	}

	datalen, err := strconv.ParseUint(sb.String(), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid storage command: invalid data length")
	}

	// if there's a space, we're expecting "noreply"
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
		if len(data) < 7 || !bytes.Equal(data[:7], []byte("noreply")) {
			return fmt.Errorf("invalid storage command: expected noreply")
		}
		cmd.noreply = true
		data = data[7:]
	}

	if !bytes.Equal(data[:2], crlf) {
		return fmt.Errorf("invalid storage command: expected CRLF")
	}
	data = data[2:]

	if uint64(len(data)) < datalen {
		return fmt.Errorf("invalid storage command: data length mismatch")
	}

	cmd.data = data[:datalen]
	data = data[datalen:]

	if !bytes.Equal(data, crlf) {
		return fmt.Errorf("invalid storage command: expected CRLF")
	}
	return nil
}

type SetCmd struct {
	storageCmd
}

var _ Cmd = (*SetCmd)(nil)

func NewSetCmd(key string, value []byte) *SetCmd {
	return &SetCmd{
		storageCmd: storageCmd{
			cmdName: "set",
			key:     key,
			data:    value,
		},
	}
}

func (cmd *SetCmd) Reset() *SetCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type AddCmd struct {
	storageCmd
}

var _ Cmd = (*AddCmd)(nil)

func NewAddCmd(key string, value []byte) *AddCmd {
	return &AddCmd{
		storageCmd: storageCmd{
			cmdName: "add",
			key:     key,
			data:    value,
		},
	}
}

func (cmd *AddCmd) Reset() *AddCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type PrependCmd struct {
	storageCmd
}

var _ Cmd = (*PrependCmd)(nil)

func NewPrependCmd(key string, value []byte) *PrependCmd {
	return &PrependCmd{
		storageCmd: storageCmd{
			cmdName: "prepend",
			key:     key,
			data:    value,
		},
	}
}

func (cmd *PrependCmd) Reset() *PrependCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type AppendCmd struct {
	storageCmd
}

var _ Cmd = (*AppendCmd)(nil)

func NewAppendCmd(key string, value []byte) *AppendCmd {
	return &AppendCmd{
		storageCmd: storageCmd{
			cmdName: "append",
			key:     key,
			data:    value,
		},
	}
}

func (cmd *AppendCmd) Reset() *AppendCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type ReplaceCmd struct {
	storageCmd
}

var _ Cmd = (*ReplaceCmd)(nil)

func NewReplaceCmd(key string, value []byte) *ReplaceCmd {
	return &ReplaceCmd{
		storageCmd: storageCmd{
			cmdName: "replace",
			key:     key,
			data:    value,
		},
	}
}

func (cmd *ReplaceCmd) Reset() *ReplaceCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type CasCmd struct {
	storageCmd
}

var _ Cmd = (*CasCmd)(nil)

func NewCasCmd(key string, value []byte, cas uint64) *CasCmd {
	return (&CasCmd{
		storageCmd: storageCmd{
			cmdName: "cas",
			key:     key,
			data:    value,
		},
	}).SetCas(cas)
}

func (cmd *CasCmd) SetCas(cas uint64) *CasCmd {
	cmd.setCas(cas)
	return cmd
}

func (cmd *CasCmd) Reset() *CasCmd {
	cmd.storageCmd.Reset()
	return cmd
}

type SetCmdReplyType uint8

const (
	SetCmdReplyInvalid SetCmdReplyType = iota
	SetCmdReplyStored
	SetCmdReplyNotStored
	SetCmdReplyExists
	SetCmdReplyNotFound
	SetCmdReplyTypeMax
)

type SetCmdReply struct {
	status SetCmdReplyType
}

var _ Reply = (*SetCmdReply)(nil)

func (cmd *SetCmdReply) Status() SetCmdReplyType {
	return cmd.status
}

func (cmd *SetCmdReply) WriteTo(dst io.Writer) (int64, error) {
	var written int64
	switch cmd.status {
	case SetCmdReplyStored:
		n, err := dst.Write(setCmdReplyStored)
		written += int64(n)
		if err != nil {
			return written, err
		}
	case SetCmdReplyNotStored:
		n, err := dst.Write(setCmdReplyNotStored)
		written += int64(n)
		if err != nil {
			return written, err
		}
	case SetCmdReplyExists:
		n, err := dst.Write(setCmdReplyExists)
		written += int64(n)
		if err != nil {
			return written, err
		}
	case SetCmdReplyNotFound:
		n, err := dst.Write(setCmdReplyNotFound)
		written += int64(n)
		if err != nil {
			return written, err
		}
	default:
		return 0, fmt.Errorf("invalid set command reply")
	}

	n, err := dst.Write(crlf)
	written += int64(n)
	return written, err
}

var setCmdReplyStored = []byte("STORED")
var setCmdReplyNotStored = []byte("NOT_STORED")
var setCmdReplyExists = []byte("EXISTS")
var setCmdReplyNotFound = []byte("NOT_FOUND")

func (reply *SetCmdReply) UnmarshalText(data []byte) error {
	ldata := len(data)
	if ldata == len(setCmdReplyStored)+2 { // STORED or EXISTS followed by CRLF
		switch {
		case bytes.Equal(data, setCmdReplyStored):
			reply.status = SetCmdReplyStored
		case bytes.Equal(data, setCmdReplyExists):
			reply.status = SetCmdReplyExists
		default:
			return fmt.Errorf("invalid set command reply")
		}
	} else if ldata == len(setCmdReplyNotStored)+2 { // NOT_STORED followed by CRLF
		if !bytes.Equal(data, setCmdReplyNotStored) {
			return fmt.Errorf("invalid set command reply")
		}
		reply.status = SetCmdReplyNotStored
	} else if ldata == len(setCmdReplyNotFound)+2 { // NOT_FOUND followed by CRLF
		if !bytes.Equal(data, setCmdReplyNotFound) {
			return fmt.Errorf("invalid set command reply")
		}
		reply.status = SetCmdReplyNotFound
	} else {
		return fmt.Errorf("invalid set command reply")
	}
	return nil
}
