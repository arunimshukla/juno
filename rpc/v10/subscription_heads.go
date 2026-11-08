package rpcv10

import (
	"context"

	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/jsonrpc"
	"github.com/NethermindEth/juno/rpc/rpccore"
	"github.com/NethermindEth/juno/sync"
)

// SubscribeNewHeads creates a WebSocket stream which will fire events when
// a new block header is added.
//
// It follows the specification defined here:
// https://github.com/starkware-libs/starknet-specs/blob/785257f27cdc4ea0ca3b62a21b0f7bf51000f9b1/api/starknet_ws_api.json#L10
//
//nolint:lll,nolintlint // url exceeds line limit, nolintlint because conflicting line limit with other lint rules
func (h *Handler) SubscribeNewHeads(
	ctx context.Context,
	blockID *SubscriptionBlockID,
) (SubscriptionID, *jsonrpc.Error) {
	wsConn, ok := jsonrpc.ConnFromContext(ctx)
	if !ok {
		return "", jsonrpc.Err(jsonrpc.MethodNotFound, nil)
	}

	startBlock, latestBlock, rpcErr := h.resolveBlockRange(blockID)
	if rpcErr != nil {
		return "", rpcErr
	}

	var startHash *felt.Felt
	if blockID != nil && blockID.IsHash() {
		startHash = blockID.Hash()
	}
	stream := rpccore.NewHeadStream(h.bcReader, startBlock, latestBlock, startHash, true)
	catchUp := func(ctx context.Context, id string) error {
		return stream.CatchUp(
			ctx,
			func(header *core.Header, commitments *core.BlockCommitments) error {
				adaptedHeader := AdaptBlockHeader(header, commitments)
				return sendHeader(wsConn, &adaptedHeader, id)
			},
			func(reorg *sync.ReorgBlockRange) error { return sendReorg(wsConn, reorg, id) },
		)
	}
	return h.subscribe(wsConn, subscriber{
		onStart: func(ctx context.Context, id string, _ *subscription, _ any) error {
			return catchUp(ctx, id)
		},
		onReorg: func(ctx context.Context, id string, _ *subscription, _ *sync.ReorgBlockRange) error {
			return catchUp(ctx, id)
		},
		onNewHead: func(
			ctx context.Context, id string, _ *subscription, _ *core.WithBloom[*core.Block],
		) error {
			return catchUp(ctx, id)
		},
	})
}

func sendHeader(wsConn jsonrpc.Conn, header *BlockHeader, id string) error {
	return sendResponse("starknet_subscriptionNewHeads", wsConn, id, header)
}
