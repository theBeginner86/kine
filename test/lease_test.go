package test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLease is unit testing for the lease operation.
func TestLease(t *testing.T) {
	ctx := context.Background()
	client := newKine(t)

	t.Run("LeaseGrant", func(t *testing.T) {
		g := NewWithT(t)
		var ttl int64 = 300
		resp, err := client.Lease.Grant(ctx, ttl)

		g.Expect(err).To(BeNil())
		g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
		g.Expect(resp.TTL).To(Equal(ttl))
	})
}

// BenchmarkLease is a benchmark for the lease operation.
func BenchmarkLease(b *testing.B) {
	ctx := context.Background()
	client := newKine(b)

	g := NewWithT(b)
	for i := 0; i < b.N; i++ {
		var ttl int64 = int64(i * 10)
		resp, err := client.Lease.Grant(ctx, ttl)

		g.Expect(err).To(BeNil())
		g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
		g.Expect(resp.TTL).To(Equal(ttl))
	}
}
