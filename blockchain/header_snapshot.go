package blockchain

import (
	"context"

	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/db"
)

// HeaderSnapshot reads canonical headers and commitments from one database version.
type HeaderSnapshot interface {
	Height() (uint64, error)
	BlockHeaderByNumber(uint64) (*core.Header, error)
	BlockHeaderHashByNumber(uint64) (*felt.Felt, error)
	BlockCommitmentsByNumber(uint64) (*core.BlockCommitments, error)
	Close() error
}

type headerSnapshot struct {
	db.Snapshot
}

func (b *Blockchain) NewHeaderSnapshot(ctx context.Context) (HeaderSnapshot, error) {
	snapshot, err := db.OpenSnapshot(ctx, b.database)
	if err != nil {
		return nil, err
	}
	return &headerSnapshot{snapshot}, nil
}

func (s *headerSnapshot) Height() (uint64, error) {
	return core.ChainHeightBucket.Get(s.Snapshot, struct{}{})
}

func (s *headerSnapshot) BlockHeaderByNumber(height uint64) (*core.Header, error) {
	header, err := core.BlockHeadersByNumberBucket.Get(s.Snapshot, height)
	if err != nil {
		return nil, err
	}
	return &header, nil
}

func (s *headerSnapshot) BlockHeaderHashByNumber(height uint64) (*felt.Felt, error) {
	return core.GetBlockHeaderHashByNumber(s.Snapshot, height)
}

func (s *headerSnapshot) BlockCommitmentsByNumber(height uint64) (*core.BlockCommitments, error) {
	commitments, err := core.BlockCommitmentsBucket.Get(s.Snapshot, height)
	if err != nil {
		return nil, err
	}
	return &commitments, nil
}
