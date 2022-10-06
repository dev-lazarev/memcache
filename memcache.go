// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bytes"
	"errors"
	"net"
)

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] > 0x7e {
			return false
		}
	}
	return true
}

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(config []Config) (*Client, error) {
	servers, err := NewServerList(config)
	if err != nil {
		return nil, err
	}
	return NewFromServers(servers), nil
}

// NewFromServers returns a new Client using the provided Servers.
func NewFromServers(servers *ServerList) *Client {
	return &Client{
		servers: servers,
	}
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	servers *ServerList
}

// Close closes all currently open connections.
func (c *Client) Close() {
	c.servers.Release()
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (*Item, error) {
	serverIndex, err := c.servers.PickServerIndex(key)
	if err != nil {
		return nil, err
	}
	cn, err := c.servers.GetConnection(serverIndex)
	if err != nil {
		return nil, err
	}
	err = sendConnCommand(cn, key, cmdGet, nil, 0, nil)
	if err != nil {
		_ = c.servers.CloseConnection(serverIndex, cn)
		return nil, err
	}

	hdr, k, extras, value, err := parseResponse(key, cn)

	switch err {
	case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
		_ = c.servers.PutConnection(serverIndex, cn)
	default:
		_ = c.servers.CloseConnection(serverIndex, cn)
	}

	if err != nil {
		return nil, err
	}
	var flags uint32
	if len(extras) > 0 {
		flags = bUint32(extras)
	}
	if key == "" && len(k) > 0 {
		key = string(k)
	}
	return &Item{
		Key:   key,
		Value: value,
		Flags: flags,
		casid: bUint64(hdr[16:24]),
	}, nil
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	keyMap := make(map[uint32][]string)
	for _, key := range keys {
		serverIndex, err := c.servers.PickServerIndex(key)
		if err != nil {
			return nil, err
		}
		keyMap[serverIndex] = append(keyMap[serverIndex], key)
	}

	var chs []chan *Item
	for addr, keys := range keyMap {
		ch := make(chan *Item)
		chs = append(chs, ch)
		go func(serverIndex uint32, keys []string, ch chan *Item) {
			defer close(ch)
			cn, err := c.servers.GetConnection(serverIndex)
			if err != nil {
				return
			}
			for _, k := range keys {
				if err = sendConnCommand(cn, k, cmdGetKQ, nil, 0, nil); err != nil {
					switch err {
					case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
						_ = c.servers.PutConnection(serverIndex, cn)
					default:
						_ = c.servers.CloseConnection(serverIndex, cn)
					}
					return
				}
			}
			if err = sendConnCommand(cn, "", cmdNoop, nil, 0, nil); err != nil {
				switch err {
				case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
					_ = c.servers.PutConnection(serverIndex, cn)
				default:
					_ = c.servers.CloseConnection(serverIndex, cn)
				}
				return
			}
			var item *Item
			for {
				hdr, k, extras, value, err := parseResponse("", cn)
				if err != nil {
					break
				}
				if len(k) == 0 {
					break
				}

				var flags uint32
				if len(extras) > 0 {
					flags = bUint32(extras)
				}

				item = &Item{
					Key:   string(k),
					Value: value,
					Flags: flags,
					casid: bUint64(hdr[16:24]),
				}
				ch <- item
			}
		}(addr, keys, ch)
	}

	m := make(map[string]*Item)
	for _, ch := range chs {
		for item := range ch {
			m[item.Key] = item
		}
	}
	return m, nil
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.populateOne(cmdSet, item, 0)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.populateOne(cmdAdd, item, 0)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.populateOne(cmdSet, item, item.casid)
}

func (c *Client) populateOne(cmd command, item *Item, casid uint64) error {
	extras := make([]byte, 8)
	putUint32(extras, item.Flags)
	putUint32(extras[4:8], uint32(item.Expiration))

	serverIndex, err := c.servers.PickServerIndex(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.servers.GetConnection(serverIndex)
	if err != nil {
		return err
	}

	err = sendConnCommand(cn, item.Key, cmd, item.Value, casid, extras)
	if err != nil {
		_ = c.servers.CloseConnection(serverIndex, cn)
		return err
	}

	hdr, _, _, _, err := parseResponse(item.Key, cn)
	if err != nil {
		switch err {
		case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
			_ = c.servers.PutConnection(serverIndex, cn)
		default:
			_ = c.servers.CloseConnection(serverIndex, cn)
		}
		return err
	}
	_ = c.servers.PutConnection(serverIndex, cn)
	item.casid = bUint64(hdr[16:24])
	return nil
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key string) error {
	serverIndex, err := c.servers.PickServerIndex(key)
	if err != nil {
		return err
	}
	cn, err := c.servers.GetConnection(serverIndex)
	if err != nil {
		return err
	}
	err = sendConnCommand(cn, key, cmdDelete, nil, 0, nil)
	if err != nil {
		_ = c.servers.CloseConnection(serverIndex, cn)
		return err
	}

	if err != nil {
		return err
	}
	_, _, _, _, err = parseResponse(key, cn)
	switch err {
	case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
		_ = c.servers.PutConnection(serverIndex, cn)
	default:
		_ = c.servers.CloseConnection(serverIndex, cn)
	}
	return err
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdIncr, key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdDecr, key, delta)
}

func (c *Client) incrDecr(cmd command, key string, delta uint64) (uint64, error) {
	extras := make([]byte, 20)
	putUint64(extras, delta)
	// Set expiration to 0xfffffff, so the command fails if the key
	// does not exist.
	for ii := 16; ii < 20; ii++ {
		extras[ii] = 0xff
	}

	serverIndex, err := c.servers.PickServerIndex(key)
	if err != nil {
		return 0, err
	}
	cn, err := c.servers.GetConnection(serverIndex)
	if err != nil {
		return 0, err
	}
	err = sendConnCommand(cn, key, cmd, nil, 0, extras)
	if err != nil {
		_ = c.servers.CloseConnection(serverIndex, cn)
		return 0, err
	}

	_, _, _, value, err := parseResponse(key, cn)
	switch err {
	case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
		_ = c.servers.PutConnection(serverIndex, cn)
	default:
		_ = c.servers.CloseConnection(serverIndex, cn)
	}
	if err != nil {
		return 0, err
	}
	return bUint64(value), nil
}

// Flush removes all the items in the cache after expiration seconds. If
// expiration is <= 0, it removes all the items right now.
func (c *Client) Flush(expiration int) error {
	var failed []string
	var errs []error

	var extras []byte
	if expiration > 0 {
		extras = make([]byte, 4)
		putUint32(extras, uint32(expiration))
	}

	for serverIndex := uint32(0); serverIndex < c.servers.PoolLen(); serverIndex++ {
		connection, err := c.servers.GetConnection(serverIndex)
		if err != nil {
			failed = append(failed, c.servers.Name(serverIndex))
			errs = append(errs, err)
			continue
		}
		cn := connection.(net.Conn)
		if err = sendConnCommand(cn, "", cmdFlush, nil, 0, extras); err == nil {
			_, _, _, _, err = parseResponse("", cn)
		}
		if err != nil {
			failed = append(failed, c.servers.Name(serverIndex))
			errs = append(errs, err)
		}
		switch err {
		case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
			_ = c.servers.PutConnection(serverIndex, cn)
		default:
			_ = c.servers.CloseConnection(serverIndex, cn)
		}
	}
	if len(failed) > 0 {
		var buf bytes.Buffer
		buf.WriteString("failed to flush some servers: ")
		for ii, addr := range failed {
			if ii > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(addr)
			buf.WriteString(": ")
			buf.WriteString(errs[ii].Error())
		}
		return errors.New(buf.String())
	}
	return nil
}
