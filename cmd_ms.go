package memdproto

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type MetaSetCmd struct {
	key     string
	data    []byte
	b64     *FlagKeyAsBase64
	rkey    *FlagRetrieveKey
	opaque  FlagOpaque
	mode    *MetaSetMode
	noreply *FlagNoReply
}

var _ Cmd = (*MetaSetCmd)(nil)

func NewMetaSetCmd(key string, data []byte) *MetaSetCmd {
	return &MetaSetCmd{
		key:  key,
		data: data,
	}
}

func (cmd *MetaSetCmd) Key() string {
	return cmd.key
}

func (cmd *MetaSetCmd) SetKeyAsBase64(b64 bool) *MetaSetCmd {
	if b64 {
		cmd.b64 = &FlagKeyAsBase64{}
	} else {
		cmd.b64 = nil
	}
	return cmd
}

func (cmd *MetaSetCmd) SetRetrieveKey(v bool) *MetaSetCmd {
	if v {
		cmd.rkey = &FlagRetrieveKey{}
	} else {
		cmd.rkey = nil
	}
	return cmd

}

func (cmd *MetaSetCmd) SetMode(mode MetaSetMode) *MetaSetCmd {
	cmd.mode = &mode
	return cmd
}

func (cmd *MetaSetCmd) SetOpaque(opaque []byte) *MetaSetCmd {
	cmd.opaque = FlagOpaque(opaque)
	return cmd
}

func (cmd *MetaSetCmd) SetNoReply(noreply bool) *MetaSetCmd {
	if noreply {
		cmd.noreply = &FlagNoReply{}
	} else {
		cmd.noreply = nil
	}
	return cmd
}

func (cmd *MetaSetCmd) WriteTo(dst io.Writer) (int64, error) {
	var key string
	if cmd.b64 != nil {
		key = base64.StdEncoding.EncodeToString([]byte(cmd.key))
	} else {
		key = cmd.key
	}

	ldata := len(cmd.data)
	var written int64
	n, err := fmt.Fprintf(dst, "ms %s %d", key, ldata)
	written += int64(n)
	if err != nil {
		return written, err
	}

	n64, err := writeFlags(dst, cmd.b64, cmd.rkey, cmd.mode, cmd.opaque, cmd.noreply)
	written += n64
	if err != nil {
		return written, err
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
	if err != nil {
		return written, err
	}

	return written, err
}

func (cmd *MetaSetCmd) UnmarshalText(data []byte) error {
	return fmt.Errorf("not implemented")
}

type MetaSetCmdStatus int

const (
	MetaSetCmdStatusInvalid MetaSetCmdStatus = iota
	// Set operation was successful (command=HD)
	MetaSetCmdStatusStored
	// Data _not_ stored, but not because of an error (command=NS)
	MetaSetCmdStatusNotStored
	// Under CAS semantics, item has been modified since your
	// last fetch (as specified by the CAS value. command=EX)
	MetaSetCmdStatusExists
	// Under CAS semantics, item did not exist (command=NF)
	MetaSetCmdStatusNotFound
)

type MetaSetReply struct {
	status     MetaSetCmdStatus
	b64        *FlagKeyAsBase64
	cas        *FlagRetrieveCas
	ccas       *FlagCompareCas
	flags      *FlagSetClientFlags
	invalidate *FlagInvalidateOnOldCas
	rkey       *FlagRetrieveKey
	opaque     *FlagOpaque
	noreply    *FlagNoReply
	size       *FlagRetrieveSize
	ttl        *FlagSetTTL
	mode       *MetaSetMode
	vivify     *FlagVivifyOnMiss
}

func (reply *MetaSetReply) Key() string {
	if reply.rkey == nil {
		return ""
	}
	return *reply.rkey.key
}

func (reply *MetaSetReply) Status() MetaSetCmdStatus {
	return reply.status
}

func (reply *MetaSetReply) WriteTo(dst io.Writer) (int64, error) {
	return 0, nil
}

func (reply *MetaSetReply) ReadFrom(src io.Reader) (int64, error) {
	line, err := bufio.NewReader(src).ReadBytes('\n')
	lline := len(line)
	if err != nil {
		return int64(lline), fmt.Errorf(`failed to read reply: %w`, err)
	}

	// we need at least 4 bytes for <CD>\r\n
	if lline < 4 {
		return int64(lline), fmt.Errorf(`invalid response for ms command`)
	}

	if line[lline-2] != '\r' || line[lline-1] != '\n' {
		return int64(lline), fmt.Errorf(`expected CRLF in response for ms command`)
	}
	line = line[:lline-2]

	// First two bytes is <CD>, where CD can be one of
	// HD, NS, EX, NF
	if line[0] == 'H' && line[1] == 'D' {
		reply.status = MetaSetCmdStatusStored
	} else if line[0] == 'N' && line[1] == 'S' {
		reply.status = MetaSetCmdStatusNotStored
	} else if line[0] == 'E' && line[1] == 'X' {
		reply.status = MetaSetCmdStatusExists
	} else if line[0] == 'N' && line[1] == 'F' {
		reply.status = MetaSetCmdStatusNotFound
	} else {
		return int64(lline), fmt.Errorf(`expected HD/NS/EX/NF: invalid response for ms command`)
	}

	if lline == 4 {
		return int64(lline), nil
	}

	if err := reply.readFlags(line[2:]); err != nil {
		return int64(lline), fmt.Errorf(`failed to read flags: %w`, err)
	}

	return int64(lline), nil
}

func (reply *MetaSetReply) readFlags(data []byte) error {
	rb := readbuf{data: data}
	for rb.Len() > 0 {
		// first, a space
		if rb.data[0] != ' ' {
			return fmt.Errorf(`expected space`)
		}
		rb.Advance()

		switch rb.data[0] {
		case 'b':
			rb.Advance()
			reply.b64 = &FlagKeyAsBase64{}
		case 'c':
			rb.Advance()
			var val strings.Builder
			for rb.Len() > 0 && rb.data[0] != ' ' {
				val.WriteByte(rb.data[0])
				rb.Advance()
			}

			if val.Len() == 0 {
				return fmt.Errorf(`expected value after c flag`)
			}

			u64, err := strconv.ParseUint(val.String(), 64, 10)
			if err != nil {
				return fmt.Errorf(`expected numeric value after c flag: %w`, err)
			}
			casval := FlagRetrieveCas(u64)
			reply.cas = &casval
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
