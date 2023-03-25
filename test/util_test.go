package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rancher/kine/pkg/endpoint"
	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// testWatchEventPollTimeout is the timeout for waiting to receive an event.
	testWatchEventPollTimeout = 50 * time.Millisecond

	// testWatchEventIdleTimeout is the amount of time to wait to ensure that no events
	// are received when they should not.
	testWatchEventIdleTimeout = 100 * time.Millisecond

	// testExpirePollPeriod is the polling period for waiting for lease expiration
	testExpirePollPeriod = 100 * time.Millisecond
)

// newKine spins up a new instance of kine. it also registers cleanup functions for temporary data
//
// newKine is currently hardcoded to using sqlite and a unix socket listener, but might be extended in the future
//
// newKine will panic in case of error
//
// newKine will return a context as well as a configured etcd client for the kine instance
func newKine(tb testing.TB) (*clientv3.Client, server.Backend) { // NEW-COMPACT: added "server-Backend"
	logrus.SetLevel(logrus.ErrorLevel)

	dir, err := os.MkdirTemp("testdata", "dir-*")
	if err != nil {
		panic(err)
	}
	tb.Cleanup(func() {
		os.RemoveAll(dir)
	})
	listener := fmt.Sprintf("unix://%s/listen.sock", dir)
	ep := fmt.Sprintf("sqlite://%s/data.db", dir)
	config, backend, err := endpoint.ListenAndReturnBackend(context.Background(), endpoint.Config{ // NEW-COMPACT: added "backend"
		Listener: listener,
		Endpoint: ep,
	})
	if err != nil {
		panic(err)
	}
	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		panic(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listener},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		panic(err)
	}
	return client, backend // NEW-COMPACT: added backend
}
