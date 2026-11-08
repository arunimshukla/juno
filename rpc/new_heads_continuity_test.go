package rpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/blockchain/networks"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/core/pending"
	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/db/memory"
	"github.com/NethermindEth/juno/feed"
	"github.com/NethermindEth/juno/jsonrpc"
	"github.com/NethermindEth/juno/mocks"
	"github.com/NethermindEth/juno/rpc/rpccore"
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
	subscribe func(context.Context, *uint64) *jsonrpc.Error
}

type newHeadsEndpointFactory func(blockchain.Reader, sync.Reader) newHeadsEndpoint

var newHeadsEndpoints = []struct {
	name string
	new  newHeadsEndpointFactory
}{
	{"v0.9", func(chain blockchain.Reader, syncer sync.Reader) newHeadsEndpoint {
		handler := rpcv9.New(chain, syncer, nil, log.NewNopZapLogger())
		return newHeadsEndpoint{handler.Run, func(ctx context.Context, start *uint64) *jsonrpc.Error {
			var blockID *rpcv9.SubscriptionBlockID
			if start != nil {
				id := rpcv9.SubscriptionBlockID(rpcv9.BlockIDFromNumber(*start))
				blockID = &id
			}
			_, err := handler.SubscribeNewHeads(ctx, blockID)
			return err
		}}
	}},
	{"v0.10", func(chain blockchain.Reader, syncer sync.Reader) newHeadsEndpoint {
		handler := rpcv10.New(chain, syncer, nil, log.NewNopZapLogger())
		return newHeadsEndpoint{handler.Run, func(ctx context.Context, start *uint64) *jsonrpc.Error {
			var blockID *rpcv10.SubscriptionBlockID
			if start != nil {
				id := rpcv10.SubscriptionBlockID(rpcv10.BlockIDFromNumber(*start))
				blockID = &id
			}
			_, err := handler.SubscribeNewHeads(ctx, blockID)
			return err
		}}
	}},
}

type newHeadsReorg struct {
	StartHash string `json:"starting_block_hash"`
	Start     uint64 `json:"starting_block_number"`
	EndHash   string `json:"ending_block_hash"`
	End       uint64 `json:"ending_block_number"`
}

type newHeadsConn struct {
	ctx         context.Context
	notices     []string
	commitments []string
	reorgs      []newHeadsReorg
	pauseAt     uint64
	resume      <-chan struct{}
}

func (c *newHeadsConn) Context() context.Context      { return c.ctx }
func (c *newHeadsConn) Equal(other jsonrpc.Conn) bool { return c == other }

func (c *newHeadsConn) Write(data []byte) (int, error) {
	var notice struct {
		Method string `json:"method"`
		Params struct {
			Result struct {
				Number     uint64 `json:"block_number"`
				Hash       string `json:"block_hash"`
				Commitment string `json:"transaction_commitment"`
				newHeadsReorg
			} `json:"result"`
		} `json:"params"`
	}
	if err := json.Unmarshal(data, &notice); err != nil {
		return 0, err
	}
	if notice.Method == "starknet_subscriptionReorg" {
		c.notices = append(c.notices, "reorg")
		c.reorgs = append(c.reorgs, notice.Params.Result.newHeadsReorg)
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
	c.commitments = append(c.commitments, head.Commitment)
	return len(data), nil
}

type newHeadsFixture struct {
	t             *testing.T
	conn          *newHeadsConn
	database      *memory.Database
	headers       map[uint64]*core.Header
	heads         *feed.Feed[*core.WithBloom[*core.Block]]
	reorgs        *feed.Feed[*sync.ReorgBlockRange]
	subscribe     func(context.Context, *uint64) *jsonrpc.Error
	afterSnapshot func()
}

type newHeadsChain struct {
	*blockchain.Blockchain
	afterHeight func()
}

func (c newHeadsChain) Height() (uint64, error) {
	height, err := c.Blockchain.Height()
	c.afterHeight()
	return height, err
}

func setupNewHeadsFixture(t *testing.T, newEndpoint newHeadsEndpointFactory) *newHeadsFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	f := &newHeadsFixture{
		t: t, conn: &newHeadsConn{ctx: ctx}, database: memory.New(),
		headers: make(map[uint64]*core.Header),
		heads:   feed.New[*core.WithBloom[*core.Block]](),
		reorgs:  feed.New[*sync.ReorgBlockRange](),
	}
	f.store(testNewHead(1, 1, 0))

	ctrl := gomock.NewController(t)
	t.Cleanup(func() { require.NoError(t, f.database.Close()) })
	chain := newHeadsChain{blockchain.New(f.database, &networks.Mainnet), func() {
		if hook := f.afterSnapshot; hook != nil {
			f.afterSnapshot = nil
			hook()
		}
	}}
	syncer := mocks.NewMockSyncReader(ctrl)
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
	f.startAt(t, nil)
}

func (f *newHeadsFixture) startAt(t *testing.T, start *uint64) {
	t.Helper()
	ctx := context.WithValue(f.conn.Context(), jsonrpc.ConnKey{}, f.conn)
	require.Nil(t, f.subscribe(ctx, start))
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
	require.NoError(f.t, f.database.Write(func(batch db.Batch) error {
		if err := core.WriteBlockHeader(batch, header); err != nil {
			return err
		}
		commitments := core.BlockCommitments{TransactionCommitment: header.Hash}
		if err := core.BlockCommitmentsBucket.Put(batch, header.Number, &commitments); err != nil {
			return err
		}
		return core.ChainHeightBucket.Put(batch, struct{}{}, &header.Number)
	}))
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

func TestWebsocketNewHeadsReorgInterleavings(t *testing.T) {
	for _, version := range newHeadsEndpoints {
		t.Run(version.name, func(t *testing.T) {
			t.Run("replacement wake before reorg", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.start(t)
					old := f.headers[1]
					replacement := testNewHead(1, 11, 0)
					f.store(replacement)
					f.announce(replacement)
					f.announceReorg(old)
					require.Equal(t, []string{"1/0x1", "reorg", "1/0xb"}, f.conn.notices)
					require.Equal(t, []newHeadsReorg{{"0x1", 1, "0x1", 1}}, f.conn.reorgs)
				})
			})

			t.Run("orphan wake after reorg", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.start(t)
					old := f.headers[1]
					replacement := testNewHead(1, 11, 0)
					f.store(replacement)
					f.announceReorg(old)
					f.announce(old)
					f.announce(replacement)
					require.Equal(t, []string{"1/0x1", "reorg", "1/0xb"}, f.conn.notices)
					require.Equal(t, []newHeadsReorg{{"0x1", 1, "0x1", 1}}, f.conn.reorgs)
				})
			})

			t.Run("canonical branch changes during a blocked backfill", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.start(t)
					resume := make(chan struct{})
					f.conn.pauseAt, f.conn.resume = 2, resume
					for height := uint64(2); height <= 4; height++ {
						f.store(testNewHead(height, height, height-1))
					}
					old2, old4 := f.headers[2], f.headers[4]
					f.announce(old4)

					f.store(testNewHead(2, 12, 1))
					f.store(testNewHead(3, 13, 12))
					f.store(testNewHead(4, 14, 13))
					f.reorgs.Send(&sync.ReorgBlockRange{
						StartBlockNum: 2, EndBlockNum: 4,
						StartBlockHash: old2.Hash, EndBlockHash: old4.Hash,
					})
					f.announce(f.headers[4])
					close(resume)
					synctest.Wait()

					require.Equal(t, []string{
						"1/0x1", "2/0x2", "3/0x3", "4/0x4", "reorg", "2/0xc", "3/0xd", "4/0xe",
					}, f.conn.notices)
					require.Equal(t, []newHeadsReorg{{"0x2", 2, "0x4", 4}}, f.conn.reorgs)
					if version.name == "v0.10" {
						require.Equal(t, []string{
							"0x1", "0x2", "0x3", "0x4", "0xc", "0xd", "0xe",
						}, f.conn.commitments)
					}
				})
			})

			t.Run("rewind below the delivered tip", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := setupNewHeadsFixture(t, version.new)
					f.start(t)
					f.store(testNewHead(2, 2, 1))
					f.store(testNewHead(3, 3, 2))
					f.announce(f.headers[3])
					require.NoError(t, core.ChainHeightBucket.Put(f.database, struct{}{}, new(uint64(1))))
					f.announceReorg(f.headers[2])
					require.Equal(t, []string{"1/0x1", "2/0x2", "3/0x3", "reorg"}, f.conn.notices)
					f.store(testNewHead(2, 12, 1))
					f.announce(f.headers[2])
					require.Equal(t, []string{
						"1/0x1", "2/0x2", "3/0x3", "reorg", "2/0xc",
					}, f.conn.notices)
					require.Equal(t, []newHeadsReorg{{"0x2", 2, "0x3", 3}}, f.conn.reorgs)
				})
			})
		})
	}
}

func TestWebsocketNewHeadsCatchUpLimit(t *testing.T) {
	for _, version := range newHeadsEndpoints {
		t.Run(version.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := setupNewHeadsFixture(t, version.new)
				f.start(t)
				for height := uint64(2); height <= rpccore.MaxBlocksBack+2; height++ {
					f.store(testNewHead(height, height, height-1))
				}
				f.announce(f.headers[rpccore.MaxBlocksBack+2])
				require.Equal(t, []string{"1/0x1"}, f.conn.notices)

				f.store(testNewHead(2, 2, 1))
				f.announce(f.headers[2])
				require.Equal(t, []string{"1/0x1"}, f.conn.notices)
			})
		})
	}
}

func TestWebsocketNewHeadsReorgAcrossRetainedHistoryWrap(t *testing.T) {
	for _, version := range newHeadsEndpoints {
		t.Run(version.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := setupNewHeadsFixture(t, version.new)
				f.start(t)
				tip := uint64(rpccore.MaxBlocksBack + 1)
				for height := uint64(2); height <= tip; height++ {
					f.store(testNewHead(height, height, height-1))
				}
				old := f.headers[tip]
				f.announce(old)
				require.Len(t, f.conn.notices, int(tip))
				newHash := tip + 1
				f.store(testNewHead(tip, newHash, tip-1))
				f.announce(f.headers[tip])
				require.Equal(t, []string{
					fmt.Sprintf("%d/%s", tip, old.Hash.String()),
					"reorg",
					fmt.Sprintf("%d/%s", tip, f.headers[tip].Hash.String()),
				}, f.conn.notices[len(f.conn.notices)-3:])
				wantReorg := newHeadsReorg{old.Hash.String(), tip, old.Hash.String(), tip}
				require.Equal(t, []newHeadsReorg{wantReorg}, f.conn.reorgs)
			})
		})
	}
}

func TestWebsocketNewHeadsFullReplayAtRegistration(t *testing.T) {
	for _, version := range newHeadsEndpoints {
		t.Run(version.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := setupNewHeadsFixture(t, version.new)
				for height := uint64(2); height <= rpccore.MaxBlocksBack; height++ {
					f.store(testNewHead(height, height, height-1))
				}
				tip := uint64(rpccore.MaxBlocksBack + 1)
				f.afterSnapshot = func() {
					head := testNewHead(tip, tip, tip-1)
					f.store(head)
					f.announce(head)
				}
				f.startAt(t, new(uint64(1)))
				require.Len(t, f.conn.notices, int(tip))
				require.Equal(t, "1/0x1", f.conn.notices[0])
				require.Equal(t, fmt.Sprintf("%d/%#x", tip, tip), f.conn.notices[tip-1])
			})
		})
	}
}
