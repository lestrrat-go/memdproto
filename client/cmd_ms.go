package client

import (
	"context"
	"fmt"

	"github.com/lestrrat-go/memdproto"
)

type MetaSetCmd struct {
	client *Client
	proto  *memdproto.MetaSetCmd
}

func (c *Client) MetaSet(key string, data []byte) *MetaSetCmd {
	return &MetaSetCmd{
		proto:  memdproto.NewMetaSetCmd(key, data),
		client: c,
	}
}

func (cmd *MetaSetCmd) Key() string {
	return cmd.proto.Key()
}

func (cmd *MetaSetCmd) Do(ctx context.Context) (*MetaSetResult, error) {
	conn, err := cmd.client.getConn(cmd)
	if err != nil {
		return nil, err
	}

	if _, err := cmd.proto.WriteTo(conn); err != nil {
		return nil, err
	}

	var reply memdproto.MetaSetReply
	if _, err := reply.ReadFrom(conn); err != nil {
		return nil, fmt.Errorf(`client.MetaSetCmd.Do: failed to read response: %w`, err)
	}

	return &MetaSetResult{proto: &reply}, nil
}

type MetaSetResult struct {
	proto *memdproto.MetaSetReply
}

func (mr *MetaSetResult) Stored() bool {
	return mr.proto.Status() == memdproto.MetaSetCmdStatusStored
}
