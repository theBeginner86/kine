package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestDelete is unit testing for the delete operation.
func TestDelete(t *testing.T) {
	ctx := context.Background()
	client := newKine(t)

	t.Run("DeleteNotSupportedFails", func(t *testing.T) {
		g := NewWithT(t)
		resp, err := client.Delete(ctx, "missingKey")

		g.Expect(err).NotTo(BeNil())
		g.Expect(err.Error()).To(ContainSubstring("delete is not supported"))
		g.Expect(resp).To(BeNil())
	})

	t.Run("DeleteNonExistentKeys", func(t *testing.T) {
		g := NewWithT(t)
		// The Get before the Delete is to trick kine to accept the transaction
		resp, err := client.Txn(ctx).
			Then(clientv3.OpGet("alsoNonExistentKey"), clientv3.OpDelete("alsoNonExistentKey")).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	})

	// Add a key, make sure it exists, then delete it, make sure it got deleted
	t.Run("DeleteSuccess", func(t *testing.T) {
		g := NewWithT(t)

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("testKeyToDelete"), "=", 0)).
				Then(clientv3.OpPut("testKeyToDelete", "testValue")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		{
			resp, err := client.Get(ctx, "testKeyToDelete")

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("testKeyToDelete")))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
		}

		{
			// The Get before the Delete is to trick kine to accept the transaction
			resp, err := client.Txn(ctx).
				Then(clientv3.OpGet("testKeyToDelete"), clientv3.OpDelete("testKeyToDelete")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		{
			resp, err := client.Get(ctx, "testKeyToDelete")

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(0))
		}

	})
}

// BenchmarkDelete is a benchmark for the delete operation.
func BenchmarkDelete(b *testing.B) {
	ctx := context.Background()
	client := newKine(b)

	g := NewWithT(b)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value)).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := client.Txn(ctx).
			Then(clientv3.OpGet(key), clientv3.OpDelete(key)).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}

}
