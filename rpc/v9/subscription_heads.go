package rpcv9

import (
	"context"

	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/jsonrpc"
	"github.com/NethermindEth/juno/sync"
)

// SubscribeNewHeads creates a WebSocket stream which will fire events when
// a new block header is added.
//
// It follows the specification defined here:
// https://github.com/starkware-libs/starknet-specs/blob/c2e93098b9c2ca0423b7f4d15b201f52f22d8c36/api/starknet_ws_api.json#L10
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

	return h.subscribe(wsConn, newHeadsSubscriber(h, wsConn, startBlock, latestBlock))
}

type headsSubscriberState struct {
	handler   *Handler
	conn      jsonrpc.Conn
	nextBlock uint64 // next canonical header to emit
}

func newHeadsSubscriber(
	h *Handler,
	conn jsonrpc.Conn,
	startBlock,
	latestBlock uint64,
) subscriber {
	state := &headsSubscriberState{handler: h, conn: conn, nextBlock: startBlock}

	return subscriber{
		onStart: func(ctx context.Context, id string, _ *subscription, _ any) error {
			if err := state.sendHistoricalHeaders(ctx, id, startBlock, latestBlock); err != nil {
				return err
			}
			// The feed is already registered. Catch blocks committed after resolveBlockRange
			// but before/during replay, even if the one-element feed coalesced them.
			head, err := h.bcReader.Height()
			if err != nil {
				return err
			}
			if state.nextBlock <= head {
				return state.sendHistoricalHeaders(ctx, id, state.nextBlock, head)
			}
			return nil
		},
		onReorg:   state.onReorg,
		onNewHead: state.onNewHead,
	}
}

func (s *headsSubscriberState) onReorg(
	_ context.Context,
	id string,
	_ *subscription,
	reorg *sync.ReorgBlockRange,
) error {
	if err := sendReorg(s.conn, reorg, id); err != nil {
		return err
	}
	// A reverted block number can be emitted again on the replacement chain.
	if reorg.StartBlockNum < s.nextBlock {
		s.nextBlock = reorg.StartBlockNum
	}
	return nil
}

func (s *headsSubscriberState) onNewHead(
	ctx context.Context,
	id string,
	_ *subscription,
	headWithBloom *core.WithBloom[*core.Block],
) error {
	head := headWithBloom.Value
	if head.Number < s.nextBlock {
		return nil // already emitted by replay or by an earlier feed notification
	}
	if head.Number > s.nextBlock {
		if err := s.sendHistoricalHeaders(ctx, id, s.nextBlock, head.Number-1); err != nil {
			return err
		}
	}
	if err := sendHeader(s.conn, head.Header, id); err != nil {
		return err
	}
	s.nextBlock = head.Number + 1
	return nil
}

func (s *headsSubscriberState) sendHistoricalHeaders(
	ctx context.Context,
	id string,
	startBlock,
	latestBlock uint64,
) error {
	for currentBlock := startBlock; currentBlock <= latestBlock; currentBlock++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentHeader, err := s.handler.bcReader.BlockHeaderByNumber(currentBlock)
		if err != nil {
			return err
		}

		if err = sendHeader(s.conn, currentHeader, id); err != nil {
			return err
		}
		s.nextBlock = currentBlock + 1
	}
	return nil
}

func sendHeader(wsConn jsonrpc.Conn, header *core.Header, id string) error {
	return sendResponse("starknet_subscriptionNewHeads", wsConn, id, AdaptBlockHeader(header))
}
