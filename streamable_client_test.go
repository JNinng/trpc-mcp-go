// Tencent is pleased to support the open source community by making trpc-mcp-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-mcp-go is licensed under the Apache License Version 2.0.

package mcp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"trpc.group/trpc-go/trpc-mcp-go/internal/httputil"
)

// newSessionEchoServer creates a test server that mimics a stateful streamable
// HTTP MCP server: every POST response carries the Mcp-Session-Id header (so
// each client call rewrites the transport session state), and the GET endpoint
// serves an SSE event stream whose events carry IDs (so the GET SSE goroutine
// keeps updating the transport last event ID).
func newSessionEchoServer(t *testing.T, getSSEDelay time.Duration) *httptest.Server {
	var requestID int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set(httputil.ContentTypeHeader, httputil.ContentTypeJSON)
			w.Header().Set(httputil.SessionIDHeader, "session-1")
			w.WriteHeader(http.StatusOK)
			id := atomic.AddInt64(&requestID, 1)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{}}`, id)
		case http.MethodGet:
			// Delay the response so that the GET SSE goroutine is still inside
			// connectGetSSE (reading session state) while POST responses arrive.
			time.Sleep(getSSEDelay)
			w.Header().Set(httputil.ContentTypeHeader, httputil.ContentTypeSSE)
			w.WriteHeader(http.StatusOK)
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "Streaming not supported", http.StatusInternalServerError)
				return
			}
			flusher.Flush()
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			var eventID int64
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					eventID++
					fmt.Fprintf(w, "id: %d\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{}}\n\n", eventID)
					flusher.Flush()
				}
			}
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// newStreamableTestTransport creates a streamable client transport pointed at
// the given test server.
func newStreamableTestTransport(t *testing.T, serverURL string) *streamableHTTPClientTransport {
	parsedURL, err := url.Parse(serverURL)
	require.NoError(t, err)
	config := newDefaultTransportConfig()
	config.serverURL = parsedURL
	transport := newStreamableHTTPClientTransport(config)
	t.Cleanup(func() {
		require.NoError(t, transport.close())
	})
	return transport
}

// TestStreamableClientTransport_ConcurrentPostResponsesDuringGetSSEConnect_NoSessionStateDataRace
// verifies that the transport session state (session ID, stateless flag, last
// event ID) can be read and written concurrently by POST-handling goroutines
// and the GET SSE goroutine without a data race. The race detector fails this
// test when the session state fields are accessed without synchronization.
func TestStreamableClientTransport_ConcurrentPostResponsesDuringGetSSEConnect_NoSessionStateDataRace(t *testing.T) {
	server := newSessionEchoServer(t, 20*time.Millisecond)
	transport := newStreamableTestTransport(t, server.URL)

	// Simulate the post-Initialize state: the session ID is known and the
	// client spawns the GET SSE goroutine, exactly as Client.Initialize does.
	transport.setSessionID("session-1")
	go transport.establishGetSSEConnection(context.Background())

	// Hammer POST requests from multiple goroutines while the GET SSE
	// connection is established and streams events. Every POST response
	// rewrites the session ID; every sent request reads it.
	const goroutines = 8
	const callsPerGoroutine = 100
	var wg sync.WaitGroup
	var failures int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for j := 0; j < callsPerGoroutine; j++ {
				req := newJSONRPCRequest(1, MethodToolsList, nil)
				if _, err := transport.sendRequest(ctx, req); err != nil {
					atomic.AddInt64(&failures, 1)
					return
				}
			}
		}()
	}
	wg.Wait()

	assert.Zero(t, atomic.LoadInt64(&failures), "concurrent requests should not fail")

	// Pin the GET SSE overlap: the server streams events with IDs, so the
	// transport must have recorded at least one event ID by now. Without this,
	// a refactor that silently disables the GET SSE connection would shrink
	// this test to POST-only coverage while staying green.
	require.Eventually(t, func() bool {
		return transport.getLastEventID() != ""
	}, 2*time.Second, 5*time.Millisecond, "GET SSE stream should have delivered at least one event")
}
