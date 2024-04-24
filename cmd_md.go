package memdproto

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

type MetaDeleteCmd struct {
	key        string
	b64        *FlagKeyAsBase64
	ccas       *FlagCompareCas
	rkey       *FlagRetrieveKey
	invalidate *FlagInvalidateOnOldCas
	opaque     FlagOpaque
	noreply    *FlagNoReply
	ttl        *FlagUpdateTTL
}

func NewMetaDeleteCmd(key string) *MetaDeleteCmd {
	return &MetaDeleteCmd{
		key: key,
	}
}

func (cmd *MetaDeleteCmd) Key() string {
	return cmd.key
}

func (cmd *MetaDeleteCmd) SetKeyAsBase64(b64 bool) *MetaDeleteCmd {
	if b64 {
		cmd.b64 = &FlagKeyAsBase64{}
	} else {
		cmd.b64 = nil
	}
	return cmd
}

func (cmd *MetaDeleteCmd) SetCompareCas(cas uint64) *MetaDeleteCmd {
	ccas := FlagCompareCas(cas)
	cmd.ccas = &ccas
	return cmd
}

func (cmd *MetaDeleteCmd) SetInvalidateOnOldCas(v bool) *MetaDeleteCmd {
	if v {
		cmd.invalidate = &FlagInvalidateOnOldCas{}
	} else {
		cmd.invalidate = nil
	}
	return cmd
}

func (cmd *MetaDeleteCmd) SetOpaque(opaque []byte) *MetaDeleteCmd {
	cmd.opaque = FlagOpaque(opaque)
	return cmd
}

func (cmd *MetaDeleteCmd) SetNoReply(noreply bool) *MetaDeleteCmd {
	if noreply {
		cmd.noreply = &FlagNoReply{}
	} else {
		cmd.noreply = nil
	}
	return cmd
}

func (cmd *MetaDeleteCmd) SetUpdateTTL(ttl uint32) *MetaDeleteCmd {
	uttl := FlagUpdateTTL(ttl)
	cmd.ttl = &uttl
	return cmd
}

func (cmd *MetaDeleteCmd) WriteTo(dst io.Writer) (int64, error) {
	var written int64

	var key string
	if cmd.b64 != nil {
		key = base64.StdEncoding.EncodeToString([]byte(cmd.key))
	} else {
		key = cmd.key
	}

	n, err := fmt.Fprintf(dst, "md %s", key)
	if err != nil {
		return written, err
	}
	written += int64(n)

	n64, err := writeFlags(dst, cmd.b64, cmd.ccas, cmd.invalidate, cmd.opaque, cmd.noreply, cmd.ttl)
	written += n64
	if err != nil {
		return written, err
	}

	n, err = dst.Write(crlf)
	written += int64(n)
	if err != nil {
		return written, err
	}
	return written, nil
}

type MetaDeleteCmdStatus uint8

const (
	MetaDeleteCmdInvalidStatus MetaDeleteCmdStatus = iota
	MetaDeleteCmdStatusDeleted
	MetaDeleteCmdStatusExists
	MetaDeleteCmdStatusNotFound
)

type MetaDeleteReply struct {
	status MetaDeleteCmdStatus
	b64    *FlagKeyAsBase64
	rkey   *FlagRetrieveKey
	opaque FlagOpaque
}

func (reply *MetaDeleteReply) Status() MetaDeleteCmdStatus {
	return reply.status
}

func (reply *MetaDeleteReply) ReadFrom(src io.Reader) (int64, error) {
	line, err := bufio.NewReader(src).ReadBytes('\n')
	lline := len(line)
	if err != nil {
		return int64(lline), fmt.Errorf(`failed to read reply: %w`, err)
	}

	// we need at least 4 bytes for <CD>\r\n
	if lline < 4 {
		return int64(lline), fmt.Errorf(`invalid response for md command`)
	}

	if line[lline-2] != '\r' || line[lline-1] != '\n' {
		return int64(lline), fmt.Errorf(`expected CRLF in response for md command`)
	}
	line = line[:lline-2]

	// First two bytes is <CD>, where CD can be one of
	// HD, EX, NF
	if line[0] == 'H' && line[1] == 'D' {
		reply.status = MetaDeleteCmdStatusDeleted
	} else if line[0] == 'E' && line[1] == 'X' {
		reply.status = MetaDeleteCmdStatusExists
	} else if line[0] == 'N' && line[1] == 'F' {
		reply.status = MetaDeleteCmdStatusNotFound
	} else if lline >= 14 && bytes.Equal(line[:12], []byte(`CLIENT_ERROR`)) {
		return int64(lline), fmt.Errorf(`memdproto.MetaDeleteReply: client error: %s`, line[13:])
	} else {
		return int64(lline), fmt.Errorf(`memdproto.MetaDeleteReply: expected HD/EX/NF: invalid response for md command %q`, line[:2])
	}

	if lline == 4 {
		return int64(lline), nil
	}

	if err := reply.readFlags(line[2:]); err != nil {
		return int64(lline), fmt.Errorf(`memdproto.MetaDeleteReply: failed to read flags: %w`, err)
	}

	return int64(lline), nil
}

func (reply *MetaDeleteReply) readFlags(data []byte) error {
	rb := readbuf{data: data}
	for rb.Len() > 0 {
		// first, a space
		if rb.data[0] == ' ' {
			rb.Advance()
			continue
		}

		switch rb.data[0] {
		case 'b':
			rb.Advance()
			reply.b64 = &FlagKeyAsBase64{}
		case 'k':
			rb.Advance()
			var val strings.Builder
			for rb.Len() > 0 && rb.data[0] != ' ' {
				val.WriteByte(rb.data[0])
				rb.Advance()
			}

			if val.Len() == 0 {
				return fmt.Errorf(`expected value after k flag`)
			}

			key := val.String()
			reply.rkey = &FlagRetrieveKey{key: &key}
		}
	}
	return nil
}
