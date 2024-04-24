package client

import (
	"fmt"
	"net"
)

type Command interface {
	Key() string
}

// Client represents a memcached client.
//
// Please note that this is NOT a full fledged client. It is only meant to be
// a sample on how one could use the memdproto package to implement a tool that
// is capable of handling memcached protocol.
type Client struct {
	servers     []string
	selector    ServerSelector
	activeConns map[string]net.Conn
}

func New(servers ...string) *Client {
	return &Client{
		servers:     servers,
		selector:    &ModulusSelector{},
		activeConns: make(map[string]net.Conn),
	}
}

type ServerSelector interface {
	Select(*Client, Command) (string, error)
}

func (c *Client) Servers() []string {
	return c.servers
}

// getConn is responsible for choosing the server to connect, and
// to actually make the connection.
func (c *Client) getConn(cmd Command) (net.Conn, error) {
	addr, err := c.selector.Select(c, cmd)
	if err != nil {
		return nil, fmt.Errorf(`client.getConn: failed to select server: %w`, err)
	}
	if conn, ok := c.activeConns[addr]; ok {
		return conn, nil
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf(`client.getConn: failed to connect to %s: %w`, addr, err)
	}
	c.activeConns[addr] = conn
	return conn, nil
}
