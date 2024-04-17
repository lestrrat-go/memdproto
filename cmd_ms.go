package memdproto

import (
	"encoding/base64"
	"fmt"
	"io"
)

type MetaSetCmd struct {
	key     string
	data    []byte
	b64     *FlagKeyAsBase64
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

func (cmd *MetaSetCmd) SetKeyAsBase64(b64 bool) *MetaSetCmd {
	if b64 {
		cmd.b64 = &FlagKeyAsBase64{}
	} else {
		cmd.b64 = nil
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

	n64, err := writeFlags(dst, cmd.b64, cmd.mode, cmd.opaque, cmd.noreply)
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
