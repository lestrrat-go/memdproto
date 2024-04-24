package client_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lestrrat-go/memdproto/client"
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

func TestClient(t *testing.T) {
	cl := client.New(MemcachedAddr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, 32)
	_, err := rand.Read(payload)
	require.NoError(t, err, `rand.Read should succeed`)

	prefix := "github.com/lestrrat-go/memdproto/client/test_client/"

	mgres, err := cl.MetaGet(prefix + "non-existent").
		Do(ctx)
	require.NoError(t, err, `client.MetaGet.Do should succeed`)
	require.True(t, mgres.Miss(), `client.MetaGet.Do should result in a miss`)

	mdres, err := cl.MetaDelete(prefix + "foo").
		Do(ctx)
	require.NoError(t, err, `client.MetaDelete.Do should succeed`)
	require.True(t, mdres.Deleted() || mdres.NotFound(), `client.MetaDelete.Do should result in deleted or not found`)

	msres, err := cl.MetaSet(prefix+"foo", payload).
		Do(ctx)
	require.NoError(t, err, `client.MetaSet.Do should succeed`)
	require.True(t, msres.Stored(), `client.MetaSet.Do should result in stored`)

	mgres, err = cl.MetaGet(prefix + "foo").
		Do(ctx)
	require.NoError(t, err, `client.MetaGet.Do should succeed`)
	require.True(t, mgres.Hit(), `client.MetaGet.Do should result in a hit`)
}
