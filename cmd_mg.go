package memdproto

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

type MetaGetCmd struct {
	key                 string
	b64                 *FlagKeyAsBase64
	cas                 *FlagRetrieveCas
	clientFlags         *FlagRetrieveClientFlags
	prevHit             *FlagRetrievePreviousHit
	rkey                *FlagRetrieveKey
	timeSinceLastAccess *FlagRetrieveTimeSinceLastAccess
	opaque              FlagOpaque
	vivify              *FlagVivifyOnMiss
	noreply             *FlagNoReply
	recache             *FlagRecache
	itemSize            *FlagRetrieveSize
	remainingTTL        *FlagRetrieveRemainingTTL
	updateTTL           *FlagUpdateTTL
	skipLRUBump         *FlagSkipLRUBump
	value               *FlagRetrieveValue
}

var _ Cmd = (*MetaGetCmd)(nil)

func NewMetaGetCmd(key string) *MetaGetCmd {
	return &MetaGetCmd{
		key: key,
	}
}

func (cmd *MetaGetCmd) Key() string {
	return cmd.key
}

func (cmd *MetaGetCmd) SetKeyAsBase64(b bool) *MetaGetCmd {
	cmd.b64 = &FlagKeyAsBase64{}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveCas(b bool) *MetaGetCmd {
	if b {
		cmd.cas = new(FlagRetrieveCas)
	} else {
		cmd.cas = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveClientFlags(b bool) *MetaGetCmd {
	if b {
		cmd.clientFlags = new(FlagRetrieveClientFlags)
	} else {
		cmd.clientFlags = nil

	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrievePreviousHit(b bool) *MetaGetCmd {
	if b {
		cmd.prevHit = new(FlagRetrievePreviousHit)
	} else {
		cmd.prevHit = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveKey(b bool) *MetaGetCmd {
	if b {
		cmd.rkey = new(FlagRetrieveKey)
	} else {
		cmd.rkey = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveTimeSinceLastAccess(b bool) *MetaGetCmd {
	if b {
		cmd.timeSinceLastAccess = new(FlagRetrieveTimeSinceLastAccess)
	} else {
		cmd.timeSinceLastAccess = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetVivifyOnMiss(ttl uint64) *MetaGetCmd {
	v := FlagVivifyOnMiss(ttl)
	cmd.vivify = &v
	return cmd
}

func (cmd *MetaGetCmd) SetOpaque(o []byte) *MetaGetCmd {
	cmd.opaque = FlagOpaque(o)
	return cmd
}

func (cmd *MetaGetCmd) SetNoReply(b bool) *MetaGetCmd {
	if b {
		cmd.noreply = new(FlagNoReply)
	} else {
		cmd.noreply = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveSize(b bool) *MetaGetCmd {
	if b {
		cmd.itemSize = new(FlagRetrieveSize)
	} else {
		cmd.itemSize = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveRemainingTTL(b bool) *MetaGetCmd {
	if b {
		cmd.remainingTTL = new(FlagRetrieveRemainingTTL)
	} else {
		cmd.remainingTTL = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetUpdateTTL(ttl int64) *MetaGetCmd {
	v := FlagUpdateTTL(ttl)
	cmd.updateTTL = &v
	return cmd
}

func (cmd *MetaGetCmd) SetSkipLRUBump(b bool) *MetaGetCmd {
	if b {
		cmd.skipLRUBump = new(FlagSkipLRUBump)
	} else {
		cmd.skipLRUBump = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) SetRetrieveValue(b bool) *MetaGetCmd {
	if b {
		cmd.value = new(FlagRetrieveValue)
	} else {
		cmd.value = nil
	}
	return cmd
}

func (cmd *MetaGetCmd) WriteTo(dst io.Writer) (int64, error) {
	var written int64

	var key string
	if cmd.b64 != nil {
		key = base64.StdEncoding.EncodeToString([]byte(cmd.key))
	} else {
		key = cmd.key
	}
	n, err := fmt.Fprintf(dst, "mg %s", key)
	if err != nil {
		return written, err
	}
	written += int64(n)

	n64, err := writeFlags(dst, cmd.b64, cmd.cas, cmd.clientFlags, cmd.prevHit, cmd.rkey, cmd.timeSinceLastAccess, cmd.vivify, cmd.opaque, cmd.noreply, cmd.recache, cmd.itemSize, cmd.remainingTTL, cmd.updateTTL, cmd.skipLRUBump, cmd.value)
	written += n64
	if err != nil {
		return written, err
	}
	return written, nil
}

func (cmd *MetaGetCmd) String() string {
	var sb strings.Builder
	cmd.WriteTo(&sb)
	return sb.String()
}

func (cmd *MetaGetCmd) Reset() *MetaGetCmd {
	cmd.b64 = nil
	cmd.cas = nil
	cmd.clientFlags = nil
	cmd.prevHit = nil
	cmd.rkey = nil
	cmd.timeSinceLastAccess = nil
	cmd.opaque = nil
	cmd.vivify = nil
	cmd.noreply = nil
	cmd.recache = nil
	cmd.itemSize = nil
	cmd.remainingTTL = nil
	cmd.updateTTL = nil
	cmd.skipLRUBump = nil
	cmd.value = nil
	return cmd

}

// receives a byte slice with at least 1 byte.
// if the len(data) <=1, then we just processed the last char (the flag)/
// otherwise checks if the subsequent byte is a space
func isSuffixedWithSpaceOrEOL(data []byte) bool {
	if len(data) <= 1 {
		return true
	}
	return data[1] == ' '
}

var metagetCmd = []byte{'m', 'g'}

func (cmd *MetaGetCmd) UnmarshalText(data []byte) error {
	cmd.Reset()

	ldata := len(data)
	if ldata < 2 {
		return fmt.Errorf(`invalid mg command`)
	}

	if !bytes.Equal(data[:2], metagetCmd) {
		return fmt.Errorf(`invalid mg command`)
	}

	data = data[2:]
	if data[0] != ' ' {
		return fmt.Errorf(`expected space after mg command`)
	}
	data = data[1:]

	keyb, count, err := readBytes(data, 250)
	if err != nil {
		return err
	}
	data = data[count:]
	cmd.key = string(keyb)

	for len(data) > 0 {
		if data[0] == ' ' {
			data = data[1:]
			continue
		}

		switch data[0] {
		case 'b':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag b`)
			}
			cmd.b64 = &FlagKeyAsBase64{}
			data = data[1:]

			// Also, at this point we have already read the key, so we need to
			// decode it from base64
			decoded, err := base64.StdEncoding.DecodeString(cmd.key)
			if err != nil {
				return fmt.Errorf(`failed to decode base64 key: %w`, err)
			}
			cmd.key = string(decoded)
		case 'c':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag c`)
			}
			cmd.cas = new(FlagRetrieveCas)
			data = data[1:]
		case 'f':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag f`)
			}
			cmd.clientFlags = new(FlagRetrieveClientFlags)
			data = data[1:]
		case 'h':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag h`)
			}
			cmd.prevHit = new(FlagRetrievePreviousHit)
			data = data[1:]
		case 'k':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag k`)
			}
			cmd.rkey = new(FlagRetrieveKey)
			data = data[1:]
		case 'l':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag l`)
			}
			cmd.timeSinceLastAccess = new(FlagRetrieveTimeSinceLastAccess)
			data = data[1:]
		case 'O':
			// O must be followed by a string
			data = data[1:]
			if len(data) == 0 {
				return fmt.Errorf(`unexpected end of data after mg flag O`)
			}
			b, count, err := readBytes(data, 32)
			if err != nil {
				return err
			}
			data = data[count:]
			cmd.opaque = FlagOpaque(b)
		case 'N':
			// N must be followed by a number
			data = data[1:]
			if len(data) == 0 {
				return fmt.Errorf(`unexpected end of data after mg flag N`)
			}
			u64, count, err := readU64(data)
			if err != nil {
				return err
			}
			data = data[count:]
			v := FlagVivifyOnMiss(u64)
			cmd.vivify = &v
		case 'q':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag q`)
			}
			cmd.noreply = new(FlagNoReply)
			data = data[1:]
		case 'R':
			// R must be followed by a number
			data = data[1:]
			if len(data) == 0 {
				return fmt.Errorf(`unexpected end of data after mg flag R`)
			}
			i64, count, err := readI64(data)
			if err != nil {
				return err
			}
			v := FlagRecache(i64)
			cmd.recache = &v
			data = data[count:]
		case 's':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag s`)
			}
			cmd.itemSize = new(FlagRetrieveSize)
			data = data[1:]
		case 't':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag t`)
			}
			cmd.remainingTTL = new(FlagRetrieveRemainingTTL)
			data = data[1:]
		case 'T':
			// T must be followed by a number
			data = data[1:]
			if len(data) == 0 {
				return fmt.Errorf(`unexpected end of data after mg flag T`)
			}
			i64, count, err := readI64(data)
			if err != nil {
				return err
			}
			v := FlagUpdateTTL(i64)
			cmd.updateTTL = &v
			data = data[count:]
		case 'u':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag u`)
			}
			cmd.skipLRUBump = new(FlagSkipLRUBump)
			data = data[1:]
		case 'v':
			if !isSuffixedWithSpaceOrEOL(data) {
				return fmt.Errorf(`extra characters following mg flag v`)
			}
			cmd.value = new(FlagRetrieveValue)
			data = data[1:]
		default:
			return fmt.Errorf(`unknown flag %c`, data[0])
		}
	}

	return nil
}

func readI64(data []byte) (int64, int, error) {
	var ttlstr []byte
	var count int
	for len(data) > 0 {
		c := data[0]
		if c == ' ' {
			break
		}
		if c < '0' && c > '9' {
			return 0, count, fmt.Errorf(`unexpected character %c, expected numeric`, c)
		}
		ttlstr = append(ttlstr, c)
		data = data[1:]
		count++
	}
	i64, err := strconv.ParseInt(string(ttlstr), 10, 64)
	if err != nil {
		return 0, count, fmt.Errorf(`failed to parse ttl for mg flag R: %w`, err)
	}
	return i64, count, nil
}

func readU64(data []byte) (uint64, int, error) {
	var ttlstr []byte
	var count int
	for len(data) > 0 {
		c := data[0]
		if c == ' ' {
			break
		}
		if c < '0' && c > '9' {
			return 0, count, fmt.Errorf(`unexpected character %c, expected numeric`, c)
		}
		ttlstr = append(ttlstr, c)
		count++
		data = data[1:]
	}
	u64, err := strconv.ParseUint(string(ttlstr), 10, 64)
	if err != nil {
		return 0, count, fmt.Errorf(`failed to parse ttl for mg flag N: %w`, err)
	}
	return u64, count, nil
}

func readBytes(data []byte, maxlen int) ([]byte, int, error) {
	var b []byte
	var count int
	for len(data) > 0 {
		c := data[0]
		// We're dealing with ASCII, so while rune(c) looks sketchy, it's OK
		if c == ' ' || unicode.IsControl(rune(c)) {
			break
		}
		b = append(b, c)
		count++
		data = data[1:]
		if len(b) > maxlen {
			return nil, count, fmt.Errorf(`opaque value too long`)
		}
	}
	return b, count, nil
}

type MetaGetReply struct {
	miss                bool
	value               []byte
	b64                 *FlagKeyAsBase64
	cas                 *FlagRetrieveCas
	clientFlags         *FlagRetrieveClientFlags
	prevHit             *FlagRetrievePreviousHit
	rkey                *FlagRetrieveKey
	timeSinceLastAccess *FlagRetrieveTimeSinceLastAccess
	opaque              FlagOpaque
	itemSize            *FlagRetrieveSize
	remainingTTL        *FlagRetrieveRemainingTTL
	recacheResult       *FlagRecacheResult
	stale               *FlagStale
}

func NewMetaGetReply() *MetaGetReply {
	return &MetaGetReply{}
}

func (mr *MetaGetReply) IsMiss() bool {
	return mr.miss
}

func (mr *MetaGetReply) Value() []byte {
	return mr.value
}

func (mr *MetaGetReply) SetMiss(b bool) *MetaGetReply {
	mr.miss = b
	return mr
}

func (mr *MetaGetReply) SetValue(b []byte) *MetaGetReply {
	mr.value = b
	return mr
}

func (mr *MetaGetReply) SetCas(v uint64) *MetaGetReply {
	f := FlagRetrieveCas(v)
	mr.cas = &f
	return mr
}

func (mr *MetaGetReply) SetClientFlags(v uint32) *MetaGetReply {
	f := FlagRetrieveClientFlags(v)
	mr.clientFlags = &f
	return mr
}

func (mr *MetaGetReply) SetPreviousHit(b bool) *MetaGetReply {
	if b {
		mr.prevHit = new(FlagRetrievePreviousHit)
	} else {
		mr.prevHit = nil
	}
	return mr
}

// Key returns the value associated with the key flag ("k") in the response.
//
// If the base64 flag is toggled, the key is base64 decoded before being returned.
func (mr *MetaGetReply) Key() string {
	if mr.rkey == nil || mr.rkey.key == nil {
		return ""
	}
	s := *mr.rkey.key

	if mr.b64 != nil {
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return ""
		}
		s = string(decoded)
	}
	return s
}

// SetKey sets the key to be returned with the response using the key flag ("k").
// The provided key should always be in its "raw" form (i.e. not base64 encoded).
//
// If b64 is true, the base64 flag ("b") will be set, as well as
// the key being base64 encoded before being stored in the reply object.
//
// Note that unlike MetaGetCmd, there is no MetaGetReply.SetKeyAsBase64 method.
//
// If s is an empty string, both the key flag ("k") and the base64 flag ("b")
// will be cleared, regardless of the value of b64.
func (mr *MetaGetReply) SetKey(s string, b64 bool) *MetaGetReply {
	if s == "" {
		mr.rkey = nil
		mr.b64 = nil
	} else {
		mr.rkey = new(FlagRetrieveKey)

		s = base64.StdEncoding.EncodeToString([]byte(s))
		if b64 {
			mr.b64 = new(FlagKeyAsBase64)
		} else {
			mr.b64 = nil
		}
		mr.rkey.key = &s
	}
	return mr
}

func (mr *MetaGetReply) SetTimeSinceLastAccess(v uint64) *MetaGetReply {
	mr.timeSinceLastAccess = &FlagRetrieveTimeSinceLastAccess{value: &v}
	return mr
}

func (mr *MetaGetReply) SetOpaque(o []byte) *MetaGetReply {
	mr.opaque = FlagOpaque(o)
	return mr
}

func (mr *MetaGetReply) SetItemSize(v uint64) *MetaGetReply {
	mr.itemSize = &FlagRetrieveSize{value: &v}
	return mr
}

func (mr *MetaGetReply) SetRemainingTTL(v int64) *MetaGetReply {
	mr.remainingTTL = &FlagRetrieveRemainingTTL{value: &v}
	return mr
}

func (mr *MetaGetReply) SetRecacheResult(win bool) *MetaGetReply {
	mr.recacheResult = &FlagRecacheResult{won: win}
	return mr
}

func (mr *MetaGetReply) SetStale(b bool) *MetaGetReply {
	if b {
		mr.stale = new(FlagStale)
	} else {
		mr.stale = nil
	}
	return mr
}

func (mr *MetaGetReply) WriteTo(dst io.Writer) (int64, error) {
	if mr.miss {
		n, err := fmt.Fprintf(dst, "EN\r\n")
		return int64(n), err
	}

	var written int64
	if mr.value == nil {
		n, err := fmt.Fprintf(dst, "HD")
		written += int64(n)
		if err != nil {
			return written, err
		}
	} else {
		n, err := fmt.Fprintf(dst, "VA %d", len(mr.value))
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	n64, err := writeFlags(dst, mr.b64, mr.cas, mr.clientFlags, mr.prevHit, mr.rkey, mr.timeSinceLastAccess, mr.opaque, mr.itemSize, mr.remainingTTL, mr.recacheResult, mr.stale)
	written += n64
	if err != nil {
		return written, err
	}

	if mr.value != nil {
		n, err := dst.Write(crlf)
		written += int64(n)
		if err != nil {
			return written, err
		}

		n, err = dst.Write(mr.value)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	n, err := dst.Write(crlf)
	written += int64(n)
	return written, err
}
