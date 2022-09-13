package memcache

import (
	"net"
)

type Addr struct {
	net.Addr
	auth *Auth
	s    string
	n    string
}

func (a *Addr) String() string {
	return a.s
}

func NewAddr(addr net.Addr, auth *Auth) *Addr {
	return &Addr{
		Addr: addr,
		auth: auth,
		s:    addr.String(),
		n:    addr.Network(),
	}
}

type Auth struct {
	login    string
	password string
}
