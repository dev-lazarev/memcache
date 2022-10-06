package memcache

import "time"

type Config struct {
	Server   string
	User     string
	Password string

	//The minimum number of connections to have in the connection pool
	InitialCap int

	//Maximum number of concurrent live connections
	MaxCap int

	//Max idle connections
	MaxIdle int

	//The maximum idle time of the connection, if it exceeds this event, it will be invalid
	IdleTimeout time.Duration

	ConnectionTimeout time.Duration
}
