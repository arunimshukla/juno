package rpcv10

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/mocks"
	"github.com/NethermindEth/juno/sync"
	"github.com/NethermindEth/juno/utils/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func assertRecoveredHeadNumbers(t *testing.T, output *bytes.Buffer, want []uint64) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var got []uint64
	for {
		var envelope struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					BlockNumber uint64 `json:"block_number"`
				} `json:"result"`
			} `json:"params"`
		}
		err := decoder.Decode(&envelope)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if envelope.Method == "starknet_subscriptionNewHeads" {
			got = append(got, envelope.Params.Result.BlockNumber)
		}
	}
	require.Equal(t, want, got)
}

func TestSubscribeNewHeadsCatchUpAcrossReplayAndFeedGaps(t *testing.T) {
	tests := []struct {
		name          string
		snapshotHead  uint64
		liveHead      uint64
		expectedHeads []uint64
	}{
		{
			name:          "head advances before feed registration and during replay",
			snapshotHead:  3,
			expectedHeads: []uint64{1, 2, 3},
		},
		{
			name:          "live feed keeps only the latest of several new heads",
			snapshotHead:  1,
			liveHead:      4,
			expectedHeads: []uint64{1, 2, 3, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			reader := mocks.NewMockReader(ctrl)
			handler := New(reader, nil, nil, log.NewNopZapLogger())
			ctx := t.Context()
			var output bytes.Buffer
			conn := &fakeConn{w: &output, ctx: ctx}
			sub := newHeadsSubscriber(handler, conn, 1, 1)

			reader.EXPECT().BlockHeaderByNumber(uint64(1)).
				Return(&core.Header{Number: 1}, nil)
			reader.EXPECT().BlockCommitmentsByNumber(uint64(1)).
				Return(&core.BlockCommitments{}, nil)
			upper := test.snapshotHead
			if test.liveHead > 0 {
				upper = test.liveHead - 1
			}
			for number := uint64(2); number <= upper; number++ {
				reader.EXPECT().BlockHeaderByNumber(number).
					Return(&core.Header{Number: number}, nil)
				reader.EXPECT().BlockCommitmentsByNumber(number).
					Return(&core.BlockCommitments{}, nil)
			}
			reader.EXPECT().Height().Return(test.snapshotHead, nil)
			if test.liveHead != 0 {
				reader.EXPECT().BlockCommitmentsByNumber(test.liveHead).
					Return(&core.BlockCommitments{}, nil)
			}

			require.NoError(t, sub.onStart(ctx, "test", nil, nil))
			if test.liveHead != 0 {
				head := &core.WithBloom[*core.Block]{
					Value: &core.Block{Header: &core.Header{Number: test.liveHead}},
				}
				require.NoError(t, sub.onNewHead(ctx, "test", nil, head))
			}
			assertRecoveredHeadNumbers(t, &output, test.expectedHeads)
		})
	}
}

func TestSubscribeNewHeadsCanReemitReorgedHeight(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := mocks.NewMockReader(ctrl)
	handler := New(reader, nil, nil, log.NewNopZapLogger())
	ctx := context.Background()
	var output bytes.Buffer
	conn := &fakeConn{w: &output, ctx: ctx}
	sub := newHeadsSubscriber(handler, conn, 1, 1)

	reader.EXPECT().BlockHeaderByNumber(uint64(1)).
		Return(&core.Header{Number: 1}, nil)
	reader.EXPECT().BlockCommitmentsByNumber(uint64(1)).
		Return(&core.BlockCommitments{}, nil).Times(2)
	reader.EXPECT().Height().Return(uint64(1), nil)

	require.NoError(t, sub.onStart(ctx, "test", nil, nil))
	reorg := &sync.ReorgBlockRange{StartBlockNum: 1, EndBlockNum: 1}
	require.NoError(t, sub.onReorg(ctx, "test", nil, reorg))
	replacement := &core.WithBloom[*core.Block]{
		Value: &core.Block{Header: &core.Header{Number: 1}},
	}
	require.NoError(t, sub.onNewHead(ctx, "test", nil, replacement))
	assertRecoveredHeadNumbers(t, &output, []uint64{1, 1})
}
