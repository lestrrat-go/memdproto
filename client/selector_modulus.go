package client

import "hash/fnv"

type ModulusSelector struct {
}

func (s *ModulusSelector) Select(c *Client, cmd Command) (string, error) {
	l := len(c.servers)
	h := fnv.New64a()
	h.Write([]byte(cmd.Key()))
	return c.servers[h.Sum64()%uint64(l)], nil
}
