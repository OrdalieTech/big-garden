package garden

import (
	"bytes"
	"context"
	"io"
	"slices"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

const fanout = 8

// maintainLater runs one maintenance step for e after the caller released
// its mutex (PROTOCOL §6 step 10, best effort).
func (n *Node) maintainLater(e *entity) {
	if n.skipMaintenance {
		return
	}
	n.wg.Go(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if !n.isClosed() && e.unresolved == nil && e.db != nil && e.h != nil && n.mine(e.h) {
			n.maintain(e)
		}
	})
}

// maintain publishes a snapshot when the replay pages exceed page_count+256,
// else compacts the oldest run of fanout same-level deltas (PROTOCOL §9).
// A failure abandons the step: nothing acknowledged depends on it.
func (n *Node) maintain(e *entity) {
	ctx, cancel := n.commitContext()
	defer cancel()
	h := e.h
	next := *h
	next.Revision, next.Nonce = h.Revision+1, head.NewID()
	c := &candidate{base: h, baseETag: e.etag}
	var buf bytes.Buffer
	var ref head.Ref
	place := func(r head.Ref) head.Ref {
		r.Key, r.BaseRevision = head.ObjectKey(h.Key, h.Incarnation, h.Revision, head.NewID(), r.SHA256), h.Revision
		return r
	}
	if e.replay() > uint64(h.PageCount)+256 {
		snap, err := e.db.Snapshot(&buf)
		if err != nil || snap.Ref.PostChecksum != h.DatabaseChecksum || snap.Ref.MaxSequence != h.Sequence {
			e.discard()
			return
		}
		ref = place(snap.Ref)
		next.Snapshot, next.Deltas, next.Tail, c.pages = ref, []head.Ref{}, nil, []uint32{snap.Pages}
	} else {
		i := 0
		for ; i+fanout <= len(h.Deltas); i++ {
			l := h.Deltas[i].Level
			if l < head.MaxLevel && !slices.ContainsFunc(h.Deltas[i:i+fanout], func(r head.Ref) bool { return r.Level != l }) {
				break
			}
		}
		if i+fanout > len(h.Deltas) {
			return
		}
		run := h.Deltas[i : i+fanout]
		objs := make([]io.ReaderAt, fanout)
		for j, r := range run {
			obj, err := n.st.Get(ctx, r.Key, "", int64(r.Bytes))
			if err != nil {
				return
			}
			objs[j] = bytes.NewReader(obj.Body)
		}
		out, err := engine.Compact(ctx, &buf, run, objs) // checks lengths, SHA-256, the chain and the span
		if err != nil {
			return
		}
		ref = place(out.Ref)
		ref.Level = run[0].Level + 1
		next.Deltas = slices.Concat(h.Deltas[:i], []head.Ref{ref}, h.Deltas[i+fanout:])
		c.pages = slices.Concat(e.pages[:1+i], []uint32{out.Pages}, e.pages[1+i+fanout:])
	}
	if n.putObject(ctx, ref.Key, buf.Bytes()) != nil {
		return
	}
	c.next, c.object = next, &ref
	n.publish(e, c) // not committed: the object is abandoned to collection
}

// Collect deletes unreachable immutable artifacts of one entity (PLAN §9.2,
// PROTOCOL §10): objects of ref's incarnation whose base revision is below
// the current head's revision and that the head does not reference. It runs
// on any node and opens no SQLite.
func (n *Node) Collect(ctx context.Context, ref Ref) error {
	if err := head.ValidKey(ref.Key); err != nil {
		return invalid(err.Error())
	}
	if n.isClosed() {
		return newError(CodeClosed, "", nil)
	}
	obj, err := n.st.Get(ctx, head.HeadKey(ref.Key), "", maxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
	case store.NotFound:
		return n.missing(ctx, ref.Key)
	default:
		return storeError(err)
	}
	h, err := n.decodeHead(obj.Body, ref.Key, ref.Incarnation)
	if err != nil {
		return err
	}
	live := map[string]bool{h.Snapshot.Key: true}
	for _, d := range h.Deltas {
		live[d.Key] = true
	}
	prefix := head.ObjectPrefix(ref.Key, h.Incarnation)
	for after := ""; ; {
		page, more, err := n.st.List(ctx, prefix, after, 1000)
		if err != nil {
			return storeError(err)
		}
		for _, o := range page {
			after = o.Key
			if base, _, _, err := head.ParseObjectKey(prefix, o.Key); err == nil && base < h.Revision && !live[o.Key] {
				if err := n.st.Delete(ctx, o.Key); store.OutcomeOf(err) == store.Failed {
					return storeError(err)
				} // Unknown: left for a later pass
			}
		}
		if !more || len(page) == 0 {
			return nil
		}
	}
}
