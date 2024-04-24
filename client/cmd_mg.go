package client

import (
	"context"
	"fmt"

	"github.com/lestrrat-go/memdproto"
)

// MetaGetCmd encapsulates the interaction between the server and the client
// for the MetaGet command.
type MetaGetCmd struct {
	client *Client
	proto  *memdproto.MetaGetCmd
}

func (c *Client) MetaGet(key string) *MetaGetCmd {
	return &MetaGetCmd{
		proto:  memdproto.NewMetaGetCmd(key).SetRetrieveValue(true),
		client: c,
	}
}

func (cmd *MetaGetCmd) Key() string {
	return cmd.proto.Key()
}

func (cmd *MetaGetCmd) Cas(v bool) *MetaGetCmd {
	cmd.proto.SetRetrieveCas(v)
	return cmd
}

func (cmd *MetaGetCmd) Opaque(v []byte) *MetaGetCmd {
	cmd.proto.SetOpaque(v)
	return cmd
}

func (cmd *MetaGetCmd) Do(ctx context.Context) (*MetaGetResult, error) {
	conn, err := cmd.client.getConn(cmd)
	if err != nil {
		return nil, fmt.Errorf(`client.MetaGetCmd.Do: failed to connect: %w`, err)
	}

	if _, err := cmd.proto.WriteTo(conn); err != nil {
		return nil, fmt.Errorf(`client.MetaGetCmd.Do: failed to send command: %w`, err)
	}

	var reply memdproto.MetaGetReply
	if _, err := reply.ReadFrom(conn); err != nil {
		return nil, fmt.Errorf(`client.MetaGetCmd.Do: failed to read response: %w`, err)
	}

	return &MetaGetResult{proto: &reply}, nil
}

type MetaGetResult struct {
	proto *memdproto.MetaGetReply
}

func (mr *MetaGetResult) Value() []byte {
	return mr.proto.Value()
}

func (mr *MetaGetResult) Miss() bool {
	return mr.proto.IsMiss()
}

func (mr *MetaGetResult) Hit() bool {
	return !mr.proto.IsMiss()
}
