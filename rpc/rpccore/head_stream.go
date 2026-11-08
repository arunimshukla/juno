package rpccore

import (
	"context"
	"errors"
	"math"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/sync"
)

// HeadStream tracks the canonical headers delivered to a single subscription.
// Call CatchUp serially on startup and when either chain feed wakes the subscription.
type HeadStream struct {
	chain           blockchain.Reader
	withCommitments bool
	startHash       *felt.Felt
	replayEnd       uint64
	replayed        bool
	start           uint64
	next            uint64
	floor           uint64
	hashes          []felt.Felt
}

func NewHeadStream(
	chain blockchain.Reader, start, replayEnd uint64, startHash *felt.Felt, withCommitments bool,
) *HeadStream {
	return &HeadStream{
		chain: chain, withCommitments: withCommitments, startHash: startHash,
		start: start, next: start, floor: start, replayEnd: replayEnd,
	}
}

type snapshotHead struct {
	header      *core.Header
	commitments *core.BlockCommitments
}

// CatchUp reads a bounded batch from a single snapshot and releases it before writing.
// The feeds are only wakeups because their independent relays may lag or coalesce.
func (s *HeadStream) CatchUp(
	ctx context.Context,
	sendHead func(*core.Header, *core.BlockCommitments) error,
	sendReorg func(*sync.ReorgBlockRange) error,
) error {
	if !s.replayed {
		if err := s.catchUp(ctx, s.replayEnd, sendHead, sendReorg); err != nil {
			return err
		}
		s.replayed = true
	}
	return s.catchUp(ctx, math.MaxUint64, sendHead, sendReorg)
}

func (s *HeadStream) catchUp(
	ctx context.Context,
	through uint64,
	sendHead func(*core.Header, *core.BlockCommitments) error,
	sendReorg func(*sync.ReorgBlockRange) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	view, err := s.chain.NewHeaderSnapshot(ctx)
	if err != nil {
		return err
	}
	reorg, heads, err := s.read(ctx, view, through)
	if err = errors.Join(err, view.Close()); err != nil {
		return err
	}

	if reorg != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sendReorg(reorg); err != nil {
			return err
		}
		s.next = reorg.StartBlockNum
	}
	for _, head := range heads {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sendHead(head.header, head.commitments); err != nil {
			return err
		}
		s.record(*head.header.Hash)
		s.startHash = nil
	}
	return nil
}

func (s *HeadStream) read(
	ctx context.Context, view blockchain.HeaderSnapshot, through uint64,
) (*sync.ReorgBlockRange, []snapshotHead, error) {
	latest, err := view.Height()
	hasHead := err == nil
	if err != nil && !errors.Is(err, db.ErrKeyNotFound) {
		return nil, nil, err
	}

	reorg, err := s.detectReorg(ctx, view, latest, hasHead)
	if err != nil {
		return nil, nil, err
	}
	next := s.next
	if reorg != nil {
		next = reorg.StartBlockNum
	}

	latest = min(latest, through)
	if !hasHead || next > latest {
		return reorg, nil, nil
	}
	if latest-next >= MaxBlocksBack {
		return nil, nil, errors.New("new-head subscription exceeded catch-up limit")
	}
	heads, err := s.readHeaders(ctx, view, next, latest-next+1)
	return reorg, heads, err
}

func (s *HeadStream) readHeaders(
	ctx context.Context, view blockchain.HeaderSnapshot, start, count uint64,
) ([]snapshotHead, error) {
	heads := make([]snapshotHead, 0, count)
	for offset := range count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		height := start + offset
		header, err := view.BlockHeaderByNumber(height)
		if err != nil {
			return nil, err
		}
		if header.Hash == nil {
			return nil, errors.New("canonical new-head header has no hash")
		}
		if offset == 0 && s.startHash != nil && !header.Hash.Equal(s.startHash) {
			return nil, errors.New("requested new-head starting block is no longer canonical")
		}

		var commitments *core.BlockCommitments
		if s.withCommitments {
			commitments, err = view.BlockCommitmentsByNumber(height)
			if err != nil {
				return nil, err
			}
		}
		heads = append(heads, snapshotHead{header, commitments})
	}
	return heads, nil
}

// detectReorg finds the first orphan among the headers actually sent to this subscriber.
func (s *HeadStream) detectReorg(
	ctx context.Context, view blockchain.HeaderSnapshot, latest uint64, hasHead bool,
) (*sync.ReorgBlockRange, error) {
	if s.next == s.floor {
		return nil, nil
	}

	next := s.next
	for next > s.floor {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		height := next - 1
		if hasHead && height <= latest {
			hash, err := view.BlockHeaderHashByNumber(height)
			if err != nil {
				return nil, err
			}
			if hash.Equal(s.hashAt(height)) {
				break
			}
		}
		next = height
	}
	if next == s.next {
		return nil, nil
	}
	if next == s.floor && s.floor > s.start {
		return nil, errors.New("new-head reorg exceeded retained history")
	}

	end := s.next - 1
	return &sync.ReorgBlockRange{
		StartBlockNum: next, StartBlockHash: new(*s.hashAt(next)),
		EndBlockNum: end, EndBlockHash: new(*s.hashAt(end)),
	}, nil
}

func (s *HeadStream) hashAt(height uint64) *felt.Felt {
	return &s.hashes[(height-s.start)%MaxBlocksBack]
}

func (s *HeadStream) record(hash felt.Felt) {
	index := (s.next - s.start) % MaxBlocksBack
	if index == uint64(len(s.hashes)) {
		s.hashes = append(s.hashes, hash)
	} else {
		s.hashes[index] = hash
	}
	s.next++
	if s.next-s.floor > MaxBlocksBack {
		s.floor++
	}
}
