package memcache

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"net"
)

// Similar to:
// http://code.google.com/appengine/docs/go/memcache/reference.html

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServerError ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long, ASCII, and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrBadMagic is returned when the magic number in a response is not valid.
	ErrBadMagic = errors.New("memcache: bad magic number in response")

	// ErrBadIncrDec is returned when performing a incr/decr on non-numeric values.
	ErrBadIncrDec = errors.New("memcache: incr or decr on non-numeric value")

	putUint16 = binary.BigEndian.PutUint16
	putUint32 = binary.BigEndian.PutUint32
	putUint64 = binary.BigEndian.PutUint64
	bUint16   = binary.BigEndian.Uint16
	bUint32   = binary.BigEndian.Uint32
	bUint64   = binary.BigEndian.Uint64
)

type command uint8

const (
	cmdGet = iota
	cmdSet
	cmdAdd
	cmdReplace
	cmdDelete
	cmdIncr
	cmdDecr
	cmdQuit
	cmdFlush
	cmdGetQ
	cmdNoop
	cmdVersion
	cmdGetK
	cmdGetKQ
	cmdAppend
	cmdPrepend
	cmdStat
	cmdSetQ
	cmdAddQ
	cmdReplaceQ
	cmdDeleteQ
	cmdIncrementQ
	cmdDecrementQ
	cmdQuitQ
	cmdFlushQ
	cmdAppendQ
	cmdPrependQ
)

// Auth Ops
const (
	opAuthList command = command(iota + 0x20)
	opAuthStart
	opAuthStep
)

type response uint16

const (
	respOk = iota
	respKeyNotFound
	respKeyExists
	respValueTooLarge
	respInvalidArgs
	respItemNotStored
	respInvalidIncrDecr
	respWrongVBucket
	respAuthErr
	respAuthContinue
	respAuthUnknown
	respUnknownCmd   = 0x81
	respOOM          = 0x82
	respNotSupported = 0x83
	respInternalErr  = 0x85
	respBusy         = 0x85
	respTemporaryErr = 0x86
)

func (r response) asError() error {
	switch r {
	case respKeyNotFound:
		return ErrCacheMiss
	case respKeyExists:
		return ErrNotStored
	case respInvalidIncrDecr:
		return ErrBadIncrDec
	case respItemNotStored:
		return ErrNotStored
	}
	return r
}

func (r response) Error() string {
	switch r {
	case respOk:
		return "Ok"
	case respKeyNotFound:
		return "key not found"
	case respKeyExists:
		return "key already exists"
	case respValueTooLarge:
		return "value too large"
	case respInvalidArgs:
		return "invalid arguments"
	case respItemNotStored:
		return "item not stored"
	case respInvalidIncrDecr:
		return "incr/decr on non-numeric value"
	case respWrongVBucket:
		return "wrong vbucket"
	case respAuthErr:
		return "auth error"
	case respAuthContinue:
		return "auth continue"
	}
	return ""
}

const (
	reqMagic  uint8 = 0x80
	respMagic uint8 = 0x81
)

func sendConnCommand(cn net.Conn, key string, cmd command, value []byte, casid uint64, extras []byte) (err error) {
	var buf []byte

	buf = make([]byte, 24, 24+len(key)+len(extras))
	// Magic (0)
	buf[0] = reqMagic

	// Command (1)
	buf[1] = byte(cmd)
	kl := len(key)
	el := len(extras)
	// Key length (2-3)
	putUint16(buf[2:], uint16(kl))
	// Extras length (4)
	buf[4] = byte(el)
	// Data type (5), always zero
	// VBucket (6-7), always zero
	// Total body length (8-11)
	vl := len(value)
	bl := uint32(kl + el + vl)
	putUint32(buf[8:], bl)
	// Opaque (12-15), always zero
	// CAS (16-23)
	putUint64(buf[16:], casid)
	// Extras
	if el > 0 {
		buf = append(buf, extras...)
	}
	if kl > 0 {
		// Key itself
		buf = append(buf, stobs(key)...)
	}
	if _, err = cn.Write(buf); err != nil {
		return err
	}

	if vl > 0 {
		if _, err = cn.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func parseResponse(rKey string, cn net.Conn) ([]byte, []byte, []byte, []byte, error) {
	var err error
	hdr := make([]byte, 24)
	if err = readAtLeast(cn, hdr, 24); err != nil {
		return nil, nil, nil, nil, err
	}
	if hdr[0] != respMagic {
		return nil, nil, nil, nil, ErrBadMagic
	}
	total := int(bUint32(hdr[8:12]))
	status := bUint16(hdr[6:8])
	if status != respOk {
		if _, err = io.CopyN(ioutil.Discard, cn, int64(total)); err != nil {
			return nil, nil, nil, nil, err
		}
		if status == respInvalidArgs && !legalKey(rKey) {
			return nil, nil, nil, nil, ErrMalformedKey
		}
		return nil, nil, nil, nil, response(status).asError()
	}
	var extras []byte
	el := int(hdr[4])
	if el > 0 {
		extras = make([]byte, el)
		if err = readAtLeast(cn, extras, el); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	var key []byte
	kl := int(bUint16(hdr[2:4]))
	if kl > 0 {
		key = make([]byte, int(kl))
		if err = readAtLeast(cn, key, kl); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	var value []byte
	vl := total - el - kl
	if vl > 0 {
		value = make([]byte, vl)
		if err = readAtLeast(cn, value, vl); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	return hdr, key, extras, value, nil
}
