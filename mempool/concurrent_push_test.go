package mempool_test

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/db/memory"
	"github.com/NethermindEth/juno/mempool"
	"github.com/NethermindEth/juno/utils/log"
	"github.com/stretchr/testify/require"
)

type mempoolTestChain struct {
	blockchain.Reader
	beforeStateRead func()
}

func (c mempoolTestChain) HeadState() (core.StateReader, blockchain.StateCloser, error) {
	if c.beforeStateRead != nil {
		c.beforeStateRead()
	}
	return nil, func() error { return nil }, nil
}

func mempoolTestTransaction(n uint64) *mempool.BroadcastedTransaction {
	return &mempool.BroadcastedTransaction{
		Transaction: &core.L1HandlerTransaction{
			TransactionHash: felt.NewFromUint64[felt.Felt](n),
		},
	}
}

func TestConcurrentMempoolPushCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		database := memory.New()
		t.Cleanup(func() { require.NoError(t, database.Close()) })
		release := make(chan struct{})
		chain := mempoolTestChain{beforeStateRead: func() { <-release }}
		pool := mempool.New(database, chain, 2, log.NewNopZapLogger())
		t.Cleanup(pool.Close)

		results := make(chan error, 2)
		for i := range uint64(2) {
			go func() { results <- pool.Push(t.Context(), mempoolTestTransaction(i+1)) }()
		}
		// Both pushes pass the first capacity check before either can enqueue.
		synctest.Wait()
		close(release)
		require.ElementsMatch(t, []error{nil, mempool.ErrTxnPoolFull}, []error{<-results, <-results})
		require.Equal(t, 1, pool.Len())
	})
}

func TestConcurrentMempoolPushAndLen(t *testing.T) {
	database := memory.New()
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	pool := mempool.New(database, mempoolTestChain{}, 4, log.NewNopZapLogger())
	t.Cleanup(pool.Close)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range uint64(2) {
		wg.Go(func() {
			<-start
			results <- pool.Push(t.Context(), mempoolTestTransaction(i+1))
		})
	}
	wg.Go(func() {
		<-start
		for range 32 {
			_ = pool.Len()
		}
	})
	close(start)
	wg.Wait()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Equal(t, 2, pool.Len())
}
