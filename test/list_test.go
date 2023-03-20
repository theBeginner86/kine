package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestList is the unit test for List operation.
func TestList(t *testing.T) {
	ctx := context.Background()
	client := newKine(t)

	t.Run("ListSuccess", func(t *testing.T) {
		g := NewWithT(t)

		// Create some keys
		keys := []string{"key1", "key2", "key3"}
		for _, key := range keys {
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, "value")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		t.Run("ListAll", func(t *testing.T) {
			// Get a list of all the keys
			resp, err := client.Get(ctx, "", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(0))
		})

		t.Run("ListPrefix", func(t *testing.T) {
			// Get a list of all the keys sice they have same prefix
			resp, err := client.Get(ctx, "key", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(0))
		})

		t.Run("ListRange", func(t *testing.T) {
			// Get a list of with key1, as only key1 falls within the specified range.
			resp, err := client.Get(ctx, "key1", clientv3.WithRange("key2"))

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(0))
		})
	})
}

// BenchmarkList is a benchmark for the Get operation.
func BenchmarkList(b *testing.B) {
	ctx := context.Background()
	client := newKine(b)
	g := NewWithT(b)

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "benchValue")).
			Else(clientv3.OpGet(key, clientv3.WithRange(""))).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}
}
