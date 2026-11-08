package garden

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// listHook runs f once before the first List.
type listHook struct {
	store.Store
	once sync.Once
	f    func()
}

func (s *listHook) List(ctx context.Context, prefix, after string, limit int) ([]store.Entry, bool, error) {
	s.once.Do(s.f)
	return s.Store.List(ctx, prefix, after, limit)
}

// TestConcurrentFirstOpen: node c finds no namespace.json, and before it
// lists entities node b creates the namespace and an entity. c must join
// b's namespace, not report state_missing (found on Scaleway, where the
// window between the GET and the LIST is tens of milliseconds).
func TestConcurrentFirstOpen(t *testing.T) {
	mem := storetest.NewMem()
	var b *Node
	hook := &listHook{Store: mem, f: func() {
		b = testNode(t, mem, "b", nil)
		mustCreate(t, b, "k", newLedger())
	}}
	cfg, _ := Config{S3: s3.New(s3.Options{}), NodeID: "c", Bucket: "b", Prefix: "p", CacheDir: t.TempDir()}.validate()
	c, err := open(bg, cfg, hook)
	if err != nil {
		t.Fatalf("open beside a node creating the namespace: %v", err)
	}
	defer c.Close(bg)
	if c.ns != b.ns {
		t.Fatal("c opened another namespace")
	}
}
