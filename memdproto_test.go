package memdproto_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/lestrrat-go/memdproto"
	"github.com/stretchr/testify/require"
)

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

		reply.AddItem(
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
