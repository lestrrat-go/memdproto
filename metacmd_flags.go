package memdproto

import (
	"bytes"
	"fmt"
	"io"
)

func writeFlags(dst io.Writer, flags ...io.WriterTo) (int64, error) {
	var written int64
	for _, f := range flags {
		// we would like to insert a space before each flag, but not unless
		// there's actually something to write. So write the contents of the
		// flag into a scratch buffer, and then write the space and the contents
		var buf bytes.Buffer
		n64, err := f.WriteTo(&buf)
		written += n64
		if err != nil {
			return written, err
		}
		if n64 > 0 {
			n, err := dst.Write(space)
			written += int64(n)
			if err != nil {
				return written, err
			}
		}
		buf.WriteTo(dst)
	}
	return written, nil
}

// FlagKeyAsBase64 is a flag used in Meta Commands to indicate
// if the key used should be treated as a base64 encoded string
type FlagKeyAsBase64 struct{}

func (f *FlagKeyAsBase64) WriteTo(dst io.Writer) (int64, error) {
	if f != nil {
		n, err := fmt.Fprintf(dst, "b")
		return int64(n), err
	}
	return 0, nil
}

// FlagRetrieveCas is a flag used in Meta Commands for operations
// regarding retrieval of a CAS value for an item.
//
// When used in a request, it indicates that the client wants to
// retrieve the CAS value for the item. When used in a response,
// it is suffixed with the actual CAS value
type FlagRetrieveCas uint64

func (f *FlagRetrieveCas) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if *f == 0 {
		n, err := fmt.Fprintf(dst, "c")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "c%d", *f)
	return int64(n), err
}

// FlagRetrieveClientFlags is a flag used in Meta Commands for
// operations regarding retrieval of client flags for an item.
//
// When used in a request, it indicates that the client wants to
// retrieve the client flags for the item. When used in a response,
// it is suffixed with the actual client flags
type FlagRetrieveClientFlags uint32

func (f *FlagRetrieveClientFlags) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if *f == 0 {
		n, err := fmt.Fprintf(dst, "f")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "f%d", *f)
	return int64(n), err
}

// FlagRetrieveExpiry is a flag used in Meta Commands for operations
// to retrieve if the item has been hit before this action.
//
// When used in a request, it indicates that the client wants to
// retrieve the "has been hit?" value for the item. When used in
// a response, it is suffixed with either a 0 or 1 to indicate the
// "has been hit?" value.
type FlagRetrievePreviousHit struct {
	hit *uint8
}

func (f *FlagRetrievePreviousHit) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.hit == nil {
		n, err := fmt.Fprintf(dst, "h")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "h%d", *f.hit)
	return int64(n), err
}

type FlagRetrieveKey struct {
	key *string
}

func (f *FlagRetrieveKey) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.key == nil {
		n, err := fmt.Fprintf(dst, "k")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "k%s", *f.key)
	return int64(n), err
}

type FlagRetrieveTimeSinceLastAccess struct {
	value *uint64
}

func (f *FlagRetrieveTimeSinceLastAccess) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.value == nil {
		n, err := fmt.Fprintf(dst, "l")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "l%d", *f.value)
	return int64(n), err
}

type FlagVivifyOnMiss uint64

func (f *FlagVivifyOnMiss) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "N%d", *f)
	return int64(n), err
}

// FlagOpague is a flag used in Meta Commands to send and receive
// opaque value
type FlagOpaque []byte

func (f FlagOpaque) WriteTo(dst io.Writer) (int64, error) {
	if len(f) == 0 {
		return 0, nil
	}
	if len(f) > 32 {
		return 0, fmt.Errorf("opaque value too long")
	}
	n, err := fmt.Fprintf(dst, "O%s", f)
	return int64(n), err
}

type FlagNoReply struct{}

func (f *FlagNoReply) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "q")
	return int64(n), err
}

type FlagRecache uint64

func (f *FlagRecache) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "R%d", *f)
	return int64(n), err
}

type FlagRetrieveSize struct {
	value *uint64
}

func (f *FlagRetrieveSize) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.value == nil {
		n, err := fmt.Fprintf(dst, "s")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "s%d", *f.value)
	return int64(n), err
}

type FlagRetrieveRemainingTTL struct {
	value *int64
}

func (f *FlagRetrieveRemainingTTL) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.value == nil {
		n, err := fmt.Fprintf(dst, "t")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "t%d", *f.value)
	return int64(n), err
}

type FlagUpdateTTL int64

func (f *FlagUpdateTTL) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "T%d", *f)
	return int64(n), err
}

type FlagSkipLRUBump struct{}

func (f *FlagSkipLRUBump) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "u")
	return int64(n), err
}

type FlagRetrieveValue struct{}

func (f *FlagRetrieveValue) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "v")
	return int64(n), err
}

type FlagStale struct{}

func (f *FlagStale) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "X")
	return int64(n), err
}

type FlagDontRecache struct{}

func (f *FlagDontRecache) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	n, err := fmt.Fprintf(dst, "Z")
	return int64(n), err
}

type FlagRecacheResult struct {
	won bool
}

func (f *FlagRecacheResult) WriteTo(dst io.Writer) (int64, error) {
	if f == nil {
		return 0, nil
	}
	if f.won {
		n, err := fmt.Fprintf(dst, "W")
		return int64(n), err
	}
	n, err := fmt.Fprintf(dst, "Z")
	return int64(n), err
}

type MetaSetMode uint8

const (
	MetaSetModeSet MetaSetMode = iota
	MetaSetModeAdd
	MetaSetModeAppend
	MetaSetModePrepend
	MetaSetModeReplace
	MetaSetModeMax
)

func (m MetaSetMode) WriteTo(dst io.Writer) (int64, error) {
	flag := []byte{'M'}
	switch m {
	case MetaSetModeSet:
		flag = append(flag, 'S')
	case MetaSetModeAdd:
		flag = append(flag, 'E')
	case MetaSetModeAppend:
		flag = append(flag, 'A')
	case MetaSetModePrepend:
		flag = append(flag, 'P')
	case MetaSetModeReplace:
		flag = append(flag, 'R')
	default:
		return 0, fmt.Errorf("invalid MetaSetMode")
	}

	n, err := dst.Write(flag)

	return int64(n), err
}
