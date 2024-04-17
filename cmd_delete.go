package memdproto

import (
	"fmt"
	"io"
)

type DeleteCmd struct {
	key     string
	noreply bool
}

func (cmd *DeleteCmd) SetNoReply(noreply bool) *DeleteCmd {
	cmd.noreply = noreply
	return cmd
}

func (cmd *DeleteCmd) WriteTo(dst io.Writer) (int64, error) {
	var written int64

	n, err := fmt.Fprintf(dst, "delete %s", cmd.key)
	written += int64(n)
	if err != nil {
		return written, err
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
	return written, err
}
