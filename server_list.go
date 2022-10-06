package memcache

import (
	"fmt"
	"github.com/silenceper/pool"
	"hash/crc32"
	"net"
	"strings"
)

// ServerList is an implementation of the Servers interface.
// To initialize a ServerList use NewServerList.
type ServerList struct {
	pool         []pool.Pool
	poolLen      uint32
	serversNames []string
}

func NewServerList(configs []Config) (*ServerList, error) {
	servers := make([]pool.Pool, len(configs))
	serversLen := 0
	count := 0
	serversName := make([]string, len(configs))
	for i, config := range configs {
		if strings.Contains(config.Server, "/") {
			addr, err := net.ResolveUnixAddr("unix", config.Server)
			if err != nil {
				return nil, err
			}
			servers[i], err = newPool(addr, config)
			if err != nil {
				return nil, err
			}

		} else {
			tcpAddr, err := net.ResolveTCPAddr("tcp", config.Server)
			if err != nil {
				return nil, err
			}
			servers[i], err = newPool(tcpAddr, config)
			if err != nil {
				return nil, err
			}
		}
		serversName = append(serversName, config.Server)
		count += servers[i].Len()
		serversLen++
	}
	return &ServerList{
		pool:         servers,
		poolLen:      uint32(serversLen),
		serversNames: serversName,
	}, nil
}

func (s *ServerList) PickServerIndex(key string) (uint32, error) {
	if len(s.pool) == 0 {
		return 0, ErrNoServers
	}
	cs := crc32.ChecksumIEEE(stobs(key))
	return cs % uint32(len(s.pool)), nil
}

func (s *ServerList) GetConnection(index uint32) (net.Conn, error) {
	if s.poolLen < index {
		return nil, fmt.Errorf("server not found")
	}

	connection, err := s.pool[index].Get()
	if err != nil {
		return nil, err
	}

	return connection.(net.Conn), nil
}

func (s *ServerList) PutConnection(index uint32, conn net.Conn) error {
	if s.poolLen < index {
		return fmt.Errorf("server not found")
	}
	return s.pool[index].Put(conn)
}

func (s *ServerList) CloseConnection(index uint32, conn net.Conn) error {
	if s.poolLen < index {
		return fmt.Errorf("server not found")
	}
	return s.pool[index].Close(conn)
}

func (s *ServerList) Count() int {
	count := 0
	for _, server := range s.pool {
		count += server.Len()
	}
	return count
}

func (s *ServerList) Release() {
	for _, server := range s.pool {
		server.Release()
	}
}

func (s *ServerList) PoolLen() uint32 {
	return s.poolLen
}

func (s *ServerList) Name(index uint32) string {
	return s.serversNames[index]
}
