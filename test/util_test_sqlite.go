//go:build !dqlite

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rancher/kine/pkg/endpoint"
)

func makeEndpointConfig(_ context.Context, tb testing.TB) endpoint.Config {
	dir := tb.TempDir()

	return endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/listen.sock", dir),
		Endpoint: fmt.Sprintf("sqlite://%s/data.db", dir),
	}
}
