package votecounter_test

import (
	"testing"

	"github.com/NethermindEth/juno/consensus/starknet"
	"github.com/NethermindEth/juno/consensus/types"
	"github.com/NethermindEth/juno/consensus/votecounter"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/stretchr/testify/require"
)

type votingPowersByHeight map[types.Height][]types.VotingPower

func (v votingPowersByHeight) TotalVotingPower(height types.Height) types.VotingPower {
	var total types.VotingPower
	for _, power := range v[height] {
		total += power
	}
	return total
}

func (v votingPowersByHeight) ValidatorVotingPower(
	height types.Height, address *starknet.Address,
) types.VotingPower {
	powers := v[height]
	index := address.Uint64()
	if index >= uint64(len(powers)) {
		return 0
	}
	return powers[index]
}

func (v votingPowersByHeight) Proposer(types.Height, types.Round) starknet.Address {
	return starknet.Address{}
}

func TestFuturePrecommitQuorumUsesFutureValidatorSet(t *testing.T) {
	const current, future types.Height = 10, 11
	unitPowers := []types.VotingPower{1, 1, 1, 1, 1, 1, 1}
	tests := []struct {
		name       string
		current    []types.VotingPower
		future     []types.VotingPower
		wantQuorum bool
	}{
		{"higher total", unitPowers[:4], unitPowers, false},
		{"lower total", unitPowers, unitPowers[:4], true},
		{"redistributed", unitPowers, []types.VotingPower{2, 2, 1, 1, 1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validators := votingPowersByHeight{current: tt.current, future: tt.future}
			counter := votecounter.New[starknet.Value](validators, current)
			id := felt.FromUint64[starknet.Hash](1)
			for sender := range uint64(3) {
				vote := starknet.Precommit{
					MessageHeader: types.MessageHeader[starknet.Address]{
						Height: future,
						Sender: felt.FromUint64[starknet.Address](sender),
					},
					ID: &id,
				}
				require.True(t, counter.AddPrecommit(&vote))
				require.False(t, counter.AddPrecommit(&vote))
				if sender < 2 {
					require.False(t, counter.HasFuturePrecommitQuorum(future, 0, &id))
				}
			}
			require.Equal(t, tt.wantQuorum, counter.HasFuturePrecommitQuorum(future, 0, &id))
			require.False(t, counter.HasFuturePrecommitQuorum(future, 1, &id))
			counter.StartNewHeight()
			require.Equal(t, tt.wantQuorum, counter.HasQuorumForVote(0, votecounter.Precommit, &id))
			require.False(t, counter.HasFuturePrecommitQuorum(current, 0, &id))
		})
	}
}

func TestFuturePrecommitQuorumRequiresPositiveTotalVotingPower(t *testing.T) {
	const current, future types.Height = 10, 11
	validators := votingPowersByHeight{current: {1, 1, 1, 1}}
	counter := votecounter.New[starknet.Value](validators, current)
	id := felt.FromUint64[starknet.Hash](1)
	require.False(t, counter.HasFuturePrecommitQuorum(future, 0, &id))
	vote := starknet.Precommit{
		MessageHeader: types.MessageHeader[starknet.Address]{Height: future},
		ID:            &id,
	}
	counter.AddPrecommit(&vote)
	require.False(t, counter.HasFuturePrecommitQuorum(future, 0, &id))
}
