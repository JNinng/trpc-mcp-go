// Tencent is pleased to support the open source community by making trpc-mcp-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-mcp-go is licensed under the Apache License Version 2.0.

package mcp

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStdioTransportIsProcessRunningHelper is re-executed as the stdio server
// process by TestStdioTransportIsProcessRunningTracksProcessExit. It stays
// alive until its stdin is closed.
func TestStdioTransportIsProcessRunningHelper(t *testing.T) {
	if os.Getenv("TRPC_MCP_STDIO_LIVENESS_CHILD") != "1" {
		t.Skip("only runs as a spawned stdio server process")
	}

	// The test framework prints a trailing PASS to stdout, which the parent
	// transport would otherwise feed to its JSON-RPC decoder.
	if devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
		os.Stdout = devnull
	}

	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestStdioTransportIsProcessRunningTracksProcessExit(t *testing.T) {
	transport := newStdioClientTransport(StdioServerParameters{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestStdioTransportIsProcessRunningHelper$"},
		Env:     map[string]string{"TRPC_MCP_STDIO_LIVENESS_CHILD": "1"},
	})

	// Before any process is started the transport reports not running.
	require.False(t, transport.isProcessRunning())

	require.NoError(t, transport.startProcess())

	// The helper blocks on stdin, so the process is alive for sure.
	require.True(t, transport.isProcessRunning())

	// Closing stdin makes the helper exit; once processWatcher observes the
	// exit the transport must report not running.
	require.NoError(t, transport.stdin.Close())
	require.Eventually(t, func() bool {
		select {
		case <-transport.done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.False(t, transport.isProcessRunning())
}
