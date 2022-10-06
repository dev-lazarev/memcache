package memcache

import (
	"fmt"
	"net"
	"strings"

	"github.com/silenceper/pool"
)

func newPool(addr net.Addr, config Config) (pool.Pool, error) {
	factory := func() (interface{}, error) {
		conn, err := net.DialTimeout(addr.Network(), addr.String(), config.ConnectionTimeout)
		if err != nil {
			return nil, err
		}
		if config.User == "" && config.Password == "" {
			return conn, nil
		}
		err = sendConnCommand(conn, "", opAuthList, nil, 0, nil)
		if err != nil {
			return nil, err
		}
		_, _, _, value, err := parseResponse("", conn)
		if err != nil {
			return nil, err
		}
		if strings.Index(string(value), "PLAIN") != -1 {
			err = sendConnCommand(conn, "PLAIN", opAuthStart, []byte(fmt.Sprintf("\x00%s\x00%s", config.User, config.Password)), 0, nil)
			if err != nil {
				return nil, err
			}
			_, _, _, _, err = parseResponse("PLAIN", conn)
			if err != nil {
				fmt.Println("auth3", conn.LocalAddr(), conn.RemoteAddr())
				return nil, err
			}
		}
		return conn, nil
	}

	closeConn := func(v interface{}) error { return v.(net.Conn).Close() }

	poolConfig := &pool.Config{
		InitialCap:  config.InitialCap,
		MaxIdle:     config.MaxIdle,
		MaxCap:      config.MaxCap,
		Factory:     factory,
		Close:       closeConn,
		IdleTimeout: config.IdleTimeout,
	}
	return pool.NewChannelPool(poolConfig)
}
