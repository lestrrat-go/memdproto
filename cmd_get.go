package memdproto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"unicode"
)

type GetCmd struct {
	keys []string
	cas  bool
	mu   sync.RWMutex
}

var _ Cmd = (*GetCmd)(nil)

// Creates a new GetCmd with the specified keys.
//
// You can use the GetCmd to issue either `get` or `gets` commands, depending on the value of the
// `cas` parameter (which in turn can be set using the `SetRetrieveCas` method).
//
// It is possible for this object to contain 0 keys, but the `WriteTo` method will return an
// error if there are no keys specified.
func NewGetCmd(keys ...string) *GetCmd {
	return &GetCmd{keys: keys}
}

// Keys returns the keys associated with this command.
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) Keys() []string {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()
	return cmd.keys
}

// AddKeys adds the specified keys to the command.
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) AddKeys(keys ...string) *GetCmd {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	cmd.keys = append(cmd.keys, keys...)
	return cmd
}

// SetRetrieveCas sets whether or not the `gets` command should be used.
// If `cas` is true, the `gets` command will be used, otherwise the `get` command will be used.
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) SetRetrieveCas(b bool) *GetCmd {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	cmd.cas = b
	return cmd
}

// WriteTo writes the command to the specified writer.
// If there are no keys specified, this method will return an error.
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) WriteTo(dst io.Writer) (int64, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if len(cmd.keys) == 0 {
		return 0, fmt.Errorf("memdproto.GetCmd: no keys specified")
	}

	if cmd.cas {
		fmt.Fprintf(dst, "gets")
	} else {
		fmt.Fprintf(dst, "get")
	}
	var written int64
	var err error

	for _, key := range cmd.keys {
		n, err := fmt.Fprintf(dst, " %s", key)
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("memdproto.GetCmd: %w", err)
		}
	}

	n, err := dst.Write(crlf)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("memdproto.GetCmd: %w", err)
	}
	return written, nil
}

// Reset clears the command. The keys and cas fields are reset to their zero values.
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) Reset() *GetCmd {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	cmd.resetNL()
	return cmd
}

func (cmd *GetCmd) resetNL() {
	cmd.keys = nil
	cmd.cas = false
}

var getcmd = []byte("get")

// UnmarshalText parses the specified data and populates the command appropriately
//
// It is safe to call this method concurrently with other methods on this object.
func (cmd *GetCmd) UnmarshalText(data []byte) error {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	cmd.resetNL()

	if len(data) < 5 {
		return fmt.Errorf("memdproto.GetCmd: UnmarshalText: invalid get(s) command length")
	}

	if !bytes.Equal(data[:len(getcmd)], getcmd) {
		return fmt.Errorf("memdproto.GetCmd: UnmarshalText: invalid get command")
	}
	data = data[len(getcmd):]

	if data[0] == 's' {
		cmd.cas = true
		data = data[1:]
	}

	if data[0] != ' ' {
		return fmt.Errorf("memdproto.GetCmd: UnmarshalText: invalid get command: expected space after command")
	}
	data = data[1:]

	var buf bytes.Buffer
	for len(data) > 0 {
		if bytes.Equal(data, crlf) {
			// flush buffer, if any
			if buf.Len() > 0 {
				cmd.keys = append(cmd.keys, buf.String())
				buf.Reset()
			}
			data = data[2:]
			break
		}

		c := data[0]
		if c == ' ' || c > unicode.MaxASCII || unicode.IsControl(rune(c)) {
			// flush buffer, if any
			if buf.Len() > 0 {
				cmd.keys = append(cmd.keys, buf.String())
				buf.Reset()
			}
			data = data[1:]
			continue
		}

		buf.WriteByte(c)
		data = data[1:]
	}

	if len(data) != 0 {
		return fmt.Errorf("memdproto.GetCmd: UnmarshalText: invalid get command: trailing data")
	}
	// check that we have at least one key
	if len(cmd.keys) == 0 {
		return fmt.Errorf("memdproto.GetCmd: UnmarshalText: invalid get command: missing keys")
	}
	return nil
}

type GetReplyItem struct {
	key   string
	flags uint16
	cas   *uint64
	value []byte
}

func NewGetReplyItem(key string, value []byte) *GetReplyItem {
	return &GetReplyItem{
		key:   key,
		value: value,
	}
}

func (item *GetReplyItem) SetCas(cas uint64) *GetReplyItem {
	var v = cas
	item.cas = &v
	return item
}

func (item *GetReplyItem) SetFlags(flags uint16) *GetReplyItem {
	item.flags = flags
	return item
}

type GetReply struct {
	mu    sync.RWMutex
	items []*GetReplyItem
}

var _ Reply = (*GetReply)(nil)

func NewGetReply() *GetReply {
	return &GetReply{}
}

func (reply *GetReply) AddItems(items ...*GetReplyItem) *GetReply {
	reply.mu.Lock()
	defer reply.mu.Unlock()

	reply.items = append(reply.items, items...)
	return reply
}

func (reply *GetReply) WriteTo(dst io.Writer) (int64, error) {
	reply.mu.RLock()
	defer reply.mu.RUnlock()

	var written int64
	var err error

	for _, item := range reply.items {
		n, err := fmt.Fprintf(dst, "VALUE %s %d %d", item.key, item.flags, len(item.value))
		written += int64(n)
		if err != nil {
			return written, err
		}

		if item.cas != nil {
			n, err := fmt.Fprintf(dst, " %d", *item.cas)
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

		n64, err := dst.Write(item.value)
		written += int64(n64)
		if err != nil {
			return written, err
		}

		n, err = dst.Write(crlf)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	n, err := dst.Write(end)
	written += int64(n)
	if err != nil {
		return written, err
	}

	n, err = dst.Write(crlf)
	written += int64(n)
	return written, err
}

/*
func (reply *GetReply) UnmarshalText(data []byte) error {
	// VALUE <key> <flags> <size> [<cas unique>]
	rdr := bufio.NewReader(bytes.NewReader(data)
	cmd, err := rdr.ReadString(' ')
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected VALUE %w", err)
	}
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected key %w", err)
	}
	key = key[:len(key)-1]

	flagStr, err := rdr.ReadString(' ')
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected flags %w", err)
	}

	flagStr = flagStr[:len(flags)-1]
	flags, err := strconv.ParseUint(flagStr, 10, 16)
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected numeric flags: %w", err)
	}

	// next token is everything from the size to possible CAS value.
	sizeStr, err := rdr.ReadString('\r')
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected size %w", err)
	}
	sizeStr = sizeStr[:len(sizeStr)-1]

	// next ReadRune should be '\n'
	r, _, err := rdr.ReadRune()
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected newline after size %w", err)
	}
	if r != '\n' {
		return errors.New("memdproto.GetReply: expected newline after size")
	}

	var casStr string
	if i := strings.IndexRune(sizeStr, ' '); i > 0 {
		casStr = sizeStr[i+1:]
		sizeStr = sizeStr[:i]
	}

	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("memdproto.GetReply: expected numeric size: %w", err)
	}

	if casStr != "" {
		cas, err := strconv.ParseUint(casStr, 10, 64)
		if err != nil {
			return fmt.Errorf("memdproto.GetReply: expected numeric cas: %w", err)
		}
	}


}
*/

func (reply *GetReply) UnmarshalText(data []byte) error {
	return errors.New("not implemented")
}
