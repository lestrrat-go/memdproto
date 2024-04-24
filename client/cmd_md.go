package client

import (
	"context"

	"github.com/lestrrat-go/memdproto"
)

type MetaDeleteCmd struct {
	client *Client
	proto  *memdproto.MetaDeleteCmd
}

func (c *Client) MetaDelete(key string) *MetaDeleteCmd {
	return &MetaDeleteCmd{
		client: c,
		proto:  memdproto.NewMetaDeleteCmd(key),
	}
}

func (cmd *MetaDeleteCmd) Key() string {
	return cmd.proto.Key()
}

func (cmd *MetaDeleteCmd) Do(ctx context.Context) (*MetaDeleteResult, error) {
	conn, err := cmd.client.getConn(cmd)
	if err != nil {
		return nil, err
	}

	if _, err := cmd.proto.WriteTo(conn); err != nil {
		return nil, err
	}

	var result MetaDeleteResult
	if _, err := result.proto.ReadFrom(conn); err != nil {
		return nil, err
	}

	return &result, nil
}

type MetaDeleteResult struct {
	proto memdproto.MetaDeleteReply
}

func (cmd *MetaDeleteResult) Deleted() bool {
	return cmd.proto.Status() == memdproto.MetaDeleteCmdStatusDeleted
}

func (cmd *MetaDeleteResult) NotFound() bool {
	return cmd.proto.Status() == memdproto.MetaDeleteCmdStatusNotFound
}

func (cmd *MetaDeleteResult) Exists() bool {
	return cmd.proto.Status() == memdproto.MetaDeleteCmdStatusExists
}
