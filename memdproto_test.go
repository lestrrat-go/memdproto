package memdproto_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/lestrrat-go/memdproto"
	"github.com/stretchr/testify/require"
)

var MemcachedAddr string

func TestMain(m *testing.M) {
	st, err := testMain(m)
	if err != nil {
		panic(err)
	}
	os.Exit(st)
}

func testMain(m *testing.M) (int, error) {
	if v, ok := os.LookupEnv("MEMCACHED_ADDR"); ok {
		MemcachedAddr = v
		return m.Run(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memdpath, err := exec.LookPath("memcached")
	if err == nil {
		// find an empty port
		localAddr := "127.0.0.1"
	OUTER:
		for i := 31211; i < 65535; i++ {
			port := strconv.Itoa(i)
			addr := net.JoinHostPort(localAddr, port)
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				continue
			}

			ln.Close()
			memdcmd := exec.CommandContext(ctx, memdpath, "-l", "127.0.0.1", "-p", port, "-vvv")
			memdcmd.Start()
			defer memdcmd.Process.Kill()

			t := time.NewTimer(5 * time.Second)
			for {
				select {
				case <-ctx.Done():
					return 0, fmt.Errorf("context anceled while trying to connect to local memcached running on %q", addr)
				case <-t.C:
					return 0, fmt.Errorf("timeout reached while trying to connect to local memcached running on %q", addr)
				default:
					var dialer net.Dialer
					conn, err := dialer.DialContext(ctx, "tcp", addr)
					if err == nil {
						conn.Close()
						MemcachedAddr = addr
						break OUTER
					}
				}
			}
		}
	}

	return m.Run(), nil
}

func TestGetCmdMarshal(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		cmd := memdproto.NewGetCmd("/foo", "/bar")
		var buf bytes.Buffer
		_, err := cmd.WriteTo(&buf)
		require.NoError(t, err, "cmd.WriteTo should succeed")
		t.Logf("cmd = %q", buf.String())

		var cmd2 memdproto.GetCmd
		require.NoError(t, cmd2.UnmarshalText(buf.Bytes()), "cmd2.UnmarshalText should succeed")
		require.Equal(t, cmd, &cmd2, "cmd and cmd2 should be equal")
	})

	t.Run("get reply", func(t *testing.T) {
		reply := memdproto.NewGetReply()

		reply.AddItems(
			memdproto.NewGetReplyItem("/foo", []byte("bar")).
				SetFlags(12345).
				SetCas(12345),
		)

		var buf bytes.Buffer
		_, err := reply.WriteTo(&buf)
		require.NoError(t, err, "reply.WriteTo should succeed")
		t.Logf("reply = %q", buf.String())

		/*
			var reply2 memdproto.GetReply
			require.NoError(t, reply2.UnmarshalText(buf.Bytes()), "reply2.UnmarshalText should succeed")
			require.Equal(t, reply, &reply2, "reply and reply2 should be equal")
		*/
	})
	t.Run("gets", func(t *testing.T) {
		cmd := memdproto.NewGetCmd("/foo", "/bar")
		cmd.SetRetrieveCas(true)
		var buf bytes.Buffer
		_, err := cmd.WriteTo(&buf)
		require.NoError(t, err, "cmd.WriteTo should succeed")
		t.Logf("cmd = %q", buf.String())
		var cmd2 memdproto.GetCmd
		require.NoError(t, cmd2.UnmarshalText(buf.Bytes()), "cmd2.UnmarshalText should succeed")
		require.Equal(t, cmd, &cmd2, "cmd and cmd2 should be equal")
	})
}

func TestSetCmdMarshal(t *testing.T) {
	testcases := []struct {
		Name      string
		Construct func() memdproto.Cmd
	}{
		{
			Name: "set",
			Construct: func() memdproto.Cmd {
				cmd := memdproto.NewSetCmd("/foo", []byte("bar"))
				cmd.SetFlags(6789).
					SetNoReply(true)
				return cmd
			},
		},
		{
			Name: "add",
			Construct: func() memdproto.Cmd {
				cmd := memdproto.NewAddCmd("/foo", []byte("bar"))
				cmd.SetFlags(6789).
					SetNoReply(true)
				return cmd
			},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			cmd := tc.Construct()
			var buf bytes.Buffer
			_, err := cmd.WriteTo(&buf)
			require.NoError(t, err, "cmd.WriteTo should succeed")
			t.Logf("cmd = %q", buf.String())

			cmd2 := tc.Construct()
			reflect.ValueOf(cmd2).MethodByName("Reset").Call(nil)
			require.NotEqual(t, cmd, cmd2, "cmd and cmd2 should not be equal before unmarshal")
			require.NoError(t, cmd2.UnmarshalText(buf.Bytes()), "cmd2.UnmarshalText should succeed")
			require.Equal(t, cmd, cmd2, "cmd and cmd2 should be equal")
		})
	}
}

func TestLive(t *testing.T) {
	if MemcachedAddr == "" {
		t.Skip("memcached not running")
	}

	conn, err := net.Dial("tcp", MemcachedAddr)
	require.NoError(t, err, "net.Dial should succeed")

	payload := []byte("bar")

	setCmd := memdproto.NewMetaSetCmd("/foo", payload)
	setCmd.SetRetrieveKey(true).
		WriteTo(conn)
	setCmd.WriteTo(os.Stdout)

	var setReply memdproto.MetaSetReply
	_, err = setReply.ReadFrom(conn)
	require.NoError(t, err, "setReply.ReadFrom should succeed")
	require.Equal(t, setReply.Key(), "/foo", "setReply.Key should match")

	getCmd := memdproto.NewMetaGetCmd("/foo")
	getCmd.SetRetrieveKey(true).
		SetRetrieveValue(true).
		WriteTo(conn)

	var reply memdproto.MetaGetReply
	_, err = reply.ReadFrom(conn)
	require.NoError(t, err, "reply.ReadFrom should succeed")

	require.Equal(t, "/foo", reply.Key(), "reply.Key should match")
	require.Equal(t, payload, reply.Value(), "reply.Value should match")
}
