package rpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/core/pending"
	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/feed"
	"github.com/NethermindEth/juno/jsonrpc"
	"github.com/NethermindEth/juno/mocks"
	rpcv10 "github.com/NethermindEth/juno/rpc/v10"
	rpcv9 "github.com/NethermindEth/juno/rpc/v9"
	"github.com/NethermindEth/juno/sync"
	"github.com/NethermindEth/juno/sync/preconfirmed"
	"github.com/NethermindEth/juno/utils/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type newHeadsEndpoint struct {
	run       func(context.Context) error
	subscribe func(context.Context) *jsonrpc.Error
}

type newHeadsEndpointFactory func(blockchain.Reader, sync.Reader) newHeadsEndpoint

var newHeadsEndpoints = []struct {
	name string
	new  newHeadsEndpointFactory
}{
	{"v0.9", func(chain blockchain.Reader, syncer sync.Reader) newHeadsEndpoint {
		handler := rpcv9.New(chain, syncer, nil, log.NewNopZapLogger())
		return newHeadsEndpoint{handler.Run, func(ctx context.Context) *jsonrpc.Error {
			_, err := handler.SubscribeNewHeads(ctx, nil)
			return err
		}}
	}},
	{"v0.10", func(chain blockchain.Reader, syncer sync.Reader) newHeadsEndpoint {
		handler := rpcv10.New(chain, syncer, nil, log.NewNopZapLogger())
		return newHeadsEndpoint{handler.Run, func(ctx context.Context) *jsonrpc.Error {
			_, err := handler.SubscribeNewHeads(ctx, nil)
			return err
		}}
	}},
}

type newHeadsConn struct {
	ctx     context.Context
	notices []string
	pauseAt uint64
	resume  <-chan struct{}
}

func (c *newHeadsConn) Context() context.Context      { return c.ctx }
func (c *newHeadsConn) Equal(other jsonrpc.Conn) bool { return c == other }

func (c *newHeadsConn) Write(data []byte) (int, error) {
	var notice struct {
		Method string `json:"method"`
		Params struct {
			Result struct {
				Number uint64 `json:"block_number"`
				Hash   string `json:"block_hash"`
			} `json:"result"`
		} `json:"params"`
	}
	if err := json.Unmarshal(data, &notice); err != nil {
		return 0, err
	}
	if notice.Method == "starknet_subscriptionReorg" {
		c.notices = append(c.notices, "reorg")
		return len(data), nil
	}
	if notice.Method != "starknet_subscriptionNewHeads" {
		return 0, fmt.Errorf("unexpected notification %s", notice.Method)
	}

	head := notice.Params.Result
	if c.resume != nil && head.Number == c.pauseAt {
		select {
		case <-c.resume:
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		}
	}
	c.notices = append(c.notices, fmt.Sprintf("%d/%s", head.Number, head.Hash))
	return len(data), nil
}

type newHeadsFixture struct {
	conn          *newHeadsConn
	height        uint64
	headers       map[uint64]*core.Header
	heads         *feed.Feed[*core.WithBloom[*core.Block]]
	reorgs        *feed.Feed[*sync.ReorgBlockRange]
	subscribe     func(context.Context) *jsonrpc.Error
	afterSnapshot func()
}

func setupNewHeadsFixture(t *testing.T, newEndpoint newHeadsEndpointFactory) *newHeadsFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	f := &newHeadsFixture{
		conn:    &newHeadsConn{ctx: ctx},
		headers: make(map[uint64]*core.Header),
		heads:   feed.New[*core.WithBloom[*core.Block]](),
		reorgs:  feed.New[*sync.ReorgBlockRange](),
	}
	f.store(testNewHead(1, 1, 0))

	ctrl := gomock.NewController(t)
	chain := mocks.NewMockReader(ctrl)
	syncer := mocks.NewMockSyncReader(ctrl)
	chain.EXPECT().Height().AnyTimes().DoAndReturn(func() (uint64, error) {
		snapshot := f.height
		if hook := f.afterSnapshot; hook != nil {
			f.afterSnapshot = nil
			hook()
		}
		return snapshot, nil
	})
	chain.EXPECT().BlockHeaderByNumber(gomock.Any()).AnyTimes().
		DoAndReturn(func(height uint64) (*core.Header, error) {
			if header := f.headers[height]; header != nil {
				return header, nil
			}
			return nil, db.ErrKeyNotFound
		})
	chain.EXPECT().BlockCommitmentsByNumber(gomock.Any()).AnyTimes().
		Return(&core.BlockCommitments{}, nil)
	chain.EXPECT().SubscribeL1Head().Return(blockchain.L1HeadSubscription{
		Subscription: feed.New[*core.L1Head]().Subscribe(),
	})
	syncer.EXPECT().SubscribeNewHeads().
		Return(sync.NewHeadSubscription{Subscription: f.heads.Subscribe()})
	syncer.EXPECT().SubscribeReorg().Return(sync.ReorgSubscription{Subscription: f.reorgs.Subscribe()})
	syncer.EXPECT().SubscribePreConfirmed().Return(preconfirmed.Subscription{
		Subscription: feed.New[*core.WithBloom[*pending.PreConfirmed]]().Subscribe(),
	})

	endpoint := newEndpoint(chain, syncer)
	f.subscribe = endpoint.subscribe
	go func() {
		assert.NoError(t, endpoint.run(ctx))
	}()
	synctest.Wait()
	return f
}

func (f *newHeadsFixture) start(t *testing.T) {
	t.Helper()
	ctx := context.WithValue(f.conn.Context(), jsonrpc.ConnKey{}, f.conn)
	require.Nil(t, f.subscribe(ctx))
	synctest.Wait()
}

func testNewHead(number, hash, parent uint64) *core.Header {
	return &core.Header{
		Number:     number,
		Hash:       felt.NewFromUint64[felt.Felt](hash),
		ParentHash: felt.NewFromUint64[felt.Felt](parent),
	}
}

func (f *newHeadsFixture) store(header *core.Header) {
	f.headers[header.Number] = header
	f.height = header.Number
}

func (f *newHeadsFixture) announce(header *core.Header) {
	f.heads.Send(&core.WithBloom[*core.Block]{Value: &core.Block{Header: header}})
	synctest.Wait()
}

func (f *newHeadsFixture) announceReorg(old *core.Header) {
	f.reorgs.Send(&sync.ReorgBlockRange{
		StartBlockNum: old.Number, EndBlockNum: old.Number,
		StartBlockHash: old.Hash, EndBlockHash: old.Hash,
	})
	synctest.Wait()
}

func TestWebsocketNewHeadsContinuity(t *testing.T) {
	for _, version := range newHeadsEndpoints {
		t.Run(version.name, func(t *testing.T) {
			t.Run("commit between snapshot and registration", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.afterSnapshot = func() {
						head := testNewHead(2, 2, 1)
						f.store(head)
						f.announce(head)
					}
					f.start(t)
					require.Equal(t, []string{"1/0x1", "2/0x2"}, f.conn.notices)
				})
			})

			t.Run("heads accumulate during replay", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					resume := make(chan struct{})
					f.conn.pauseAt, f.conn.resume = 1, resume
					f.start(t)
					for height := uint64(2); height <= 3; height++ {
						head := testNewHead(height, height, height-1)
						f.store(head)
						f.announce(head)
					}
					close(resume)
					synctest.Wait()
					require.Equal(t, []string{"1/0x1", "2/0x2", "3/0x3"}, f.conn.notices)
				})
			})

			t.Run("slow write coalesces live heads", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					resume := make(chan struct{})
					f.conn.pauseAt, f.conn.resume = 2, resume
					f.start(t)
					for height := uint64(2); height <= 4; height++ {
						head := testNewHead(height, height, height-1)
						f.store(head)
						f.announce(head)
					}
					close(resume)
					synctest.Wait()
					require.Equal(t, []string{"1/0x1", "2/0x2", "3/0x3", "4/0x4"}, f.conn.notices)

					f.announce(f.headers[4])
					head := testNewHead(5, 5, 4)
					f.store(head)
					f.announce(head)
					require.Equal(t, []string{"1/0x1", "2/0x2", "3/0x3", "4/0x4", "5/0x5"}, f.conn.notices)
				})
			})

			t.Run("reorg relayed before replacement", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.start(t)
					old := f.headers[1]
					replacement := testNewHead(1, 11, 0)
					f.store(replacement)
					f.announceReorg(old)
					f.announce(replacement)
					require.Equal(t, []string{"1/0x1", "reorg", "1/0xb"}, f.conn.notices)
				})
			})
		})
	}
}
