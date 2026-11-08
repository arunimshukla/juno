package remote_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/db/pebblev2"
	"github.com/NethermindEth/juno/db/remote"
	junogrpc "github.com/NethermindEth/juno/grpc"
	"github.com/NethermindEth/juno/grpc/gen"
	"github.com/NethermindEth/juno/utils/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func connectSnapshotDB(t *testing.T, handler gen.KVServer) (*remote.DB, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	gen.RegisterKVServer(server, handler)
	t.Cleanup(server.GracefulStop)
	go func() {
		assert.NoError(t, server.Serve(listener))
	}()

	remoteDB, err := remote.New(
		"passthrough:///snapshot-test",
		ctx,
		log.NewNopZapLogger(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteDB.Close()) })
	return remoteDB, ctx
}

func TestOpenSnapshotPinsRemoteReads(t *testing.T) {
	database, err := pebblev2.New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	key := []byte("key")
	require.NoError(t, database.Put(key, []byte("before")))
	remoteDB, ctx := connectSnapshotDB(t, junogrpc.New(database, "0.0.0"))
	snapshot, err := db.OpenSnapshot(ctx, remoteDB)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snapshot.Close()) })

	require.NoError(t, database.Put(key, []byte("after")))
	require.NoError(t, snapshot.Get(key, func(value []byte) error {
		require.Equal(t, "before", string(value))
		return nil
	}))
	require.NoError(t, remoteDB.Get(key, func(value []byte) error {
		require.Equal(t, "after", string(value))
		return nil
	}))
}

type legacySnapshotServer struct{ gen.UnimplementedKVServer }

func (legacySnapshotServer) Tx(stream gen.KV_TxServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&gen.Pair{})
}

func TestOpenSnapshotRequiresRemoteAcknowledgement(t *testing.T) {
	remoteDB, ctx := connectSnapshotDB(t, legacySnapshotServer{})
	snapshot, err := db.OpenSnapshot(ctx, remoteDB)
	require.Nil(t, snapshot)
	require.ErrorContains(t, err, "remote database does not support consistent snapshots")
}
