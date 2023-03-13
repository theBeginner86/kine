package test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestGet is unit testing for the Get operation.
func TestGet(t *testing.T) {
	ctx := context.Background()
	client := newKine(t)

	t.Run("FailMissing", func(t *testing.T) {
		g := NewWithT(t)
		resp, err := client.Get(ctx, "testKey", clientv3.WithRange(""))

		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(BeEmpty())
	})

	t.Run("Success", func(t *testing.T) {
		g := NewWithT(t)

		// Create a key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
				Then(clientv3.OpPut("testKey", "testValue")).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// Get key
		{
			resp, err := client.Get(ctx, "testKey", clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("testKey")))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
		}
	})
}

// BenchmarkGet is a benchmark for the Get operation.
func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	client := newKine(b)
	g := NewWithT(b)

	// create a kv
	{
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
			Then(clientv3.OpPut("testKey", "testValue")).
			Commit()
		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}

	b.Run("LatestRevision", func(b *testing.B) {
		g := NewWithT(b)
		for i := 0; i < b.N; i++ {
			resp, err := client.Get(ctx, "testKey", clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
		}
	})
}
