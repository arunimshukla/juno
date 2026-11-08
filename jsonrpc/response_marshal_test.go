package jsonrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NethermindEth/juno/jsonrpc"
	"github.com/NethermindEth/juno/utils/log"
	"github.com/coder/websocket"
	"github.com/sourcegraph/conc/pool"
	"github.com/stretchr/testify/require"
)

type unencodableResult struct{}

func (unencodableResult) MarshalJSON() ([]byte, error) {
	return nil, errors.New("test encoding failure")
}

func TestResponseMarshalFailure(t *testing.T) {
	logger := log.NewNopZapLogger()
	server := jsonrpc.NewServer(pool.New().WithMaxGoroutines(2), logger)
	require.NoError(t, server.RegisterMethods(
		jsonrpc.Method{Name: "bad_result", Handler: func() (unencodableResult, *jsonrpc.Error) {
			return unencodableResult{}, nil
		}},
		jsonrpc.Method{Name: "bad_error_data", Handler: func() (any, *jsonrpc.Error) {
			return nil, jsonrpc.Err(jsonrpc.InvalidParams, unencodableResult{})
		}},
		jsonrpc.Method{Name: "ok", Handler: func() (string, *jsonrpc.Error) {
			return "ok", nil
		}},
	))

	const (
		badResult = `{"jsonrpc":"2.0","method":"bad_result","id":"bad"}`
		badError  = `{"jsonrpc":"2.0","method":"bad_error_data","id":null}`
		good      = `{"jsonrpc":"2.0","method":"ok","id":9007199254740993}`
		notice    = `{"jsonrpc":"2.0","method":"bad_result"}`
		wantBad   = `{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":"bad"}`
		wantNull  = `{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null}`
		wantGood  = `{"jsonrpc":"2.0","result":"ok","id":9007199254740993}`
	)

	httpHandler := jsonrpc.NewHTTP(server, logger)
	post := func(t *testing.T, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
		out := httptest.NewRecorder()
		httpHandler.ServeHTTP(out, req)
		return out
	}
	t.Run("HTTP single", func(t *testing.T) {
		for _, tc := range []struct{ request, want string }{{badResult, wantBad}, {badError, wantNull}} {
			out := post(t, tc.request)
			require.Equal(t, http.StatusOK, out.Code)
			require.JSONEq(t, tc.want, out.Body.String())
		}
	})
	t.Run("HTTP batch", func(t *testing.T) {
		out := post(t, "["+badResult+","+good+","+notice+"]")
		require.Equal(t, http.StatusOK, out.Code)
		var responses []json.RawMessage
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &responses))
		require.Len(t, responses, 2)
		want := []json.RawMessage{json.RawMessage(wantBad), json.RawMessage(wantGood)}
		require.ElementsMatch(t, want, responses)
	})
	t.Run("HTTP notification", func(t *testing.T) {
		require.Empty(t, post(t, notice).Body.String())
	})
	t.Run("WebSocket survives the failed response", func(t *testing.T) {
		wsServer := httptest.NewServer(jsonrpc.NewWebsocket(server, nil, logger))
		t.Cleanup(wsServer.Close)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		//nolint:bodyclose // Dial owns the upgrade response.
		conn, _, err := websocket.Dial(ctx, wsServer.URL, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.CloseNow() })

		for _, tc := range []struct{ request, want string }{{badResult, wantBad}, {good, wantGood}} {
			require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte(tc.request)))
			_, response, err := conn.Read(ctx)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(response))
		}
	})
}
