//go:build !dqlite
// +build !dqlite

package dqlite

import (
	"context"
	"fmt"

	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
)

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config) (server.Backend, error) {
	return nil, fmt.Errorf("dqlite is not support, compile with \"-tags dqlite\"")
}
