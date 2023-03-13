package test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatch is unit testing for the Watch operation.
func TestWatch(t *testing.T) {
	ctx := context.Background()
	client := newKine(t)

	key := "testKey"
	value := "testValue"
	watchCh := client.Watch(ctx, key)

	t.Run("ReceiveNothingUntilActivity", func(t *testing.T) {
		g := NewWithT(t)
		g.Consistently(watchCh, "100ms").ShouldNot(Receive())
	})

	var latestRevision int64

	t.Run("Create", func(t *testing.T) {
		g := NewWithT(t)

		// create a key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, value)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// receive event
		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)
			g.Eventually(watchCh, "50ms").Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				g.Expect(v.Events).To(HaveLen(1))
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypePut))
				g.Expect(v.Events[0].PrevKv).To(BeNil())
				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(value)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))

				latestRevision = v.Events[0].Kv.ModRevision

				return true
			})))
		})

		t.Run("ReceiveNothingUntilNewActivity", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, "100ms").ShouldNot(Receive())
		})
	})

	t.Run("Update", func(t *testing.T) {
		g := NewWithT(t)

		newValue := "testUpdatedValue"
		// update key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", latestRevision)).
				Then(clientv3.OpPut(key, string(newValue))).
				Else(clientv3.OpGet(key)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)

			// receive event
			g.Eventually(watchCh, "50ms").Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				g.Expect(v.Events).To(HaveLen(1))
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypePut))
				g.Expect(v.Events[0].PrevKv).NotTo(BeNil())
				g.Expect(v.Events[0].PrevKv.Value).To(Equal([]byte(value)))
				g.Expect(v.Events[0].PrevKv.ModRevision).To(Equal(latestRevision))

				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(newValue)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))
				g.Expect(v.Events[0].Kv.ModRevision).To(BeNumerically(">", latestRevision))

				latestRevision = v.Events[0].Kv.ModRevision

				return true
			})))
		})

		t.Run("ReceiveNothingUntilNewActivity", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, "100ms").ShouldNot(Receive())
		})
	})
}
