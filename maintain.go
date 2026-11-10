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
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.skipMaintenance {
		return
	}
	n.wg.Add(1) // under n.mu with closed false: never concurrent with Close's Wait
	e.refs++
	go func() {
		defer n.wg.Done()
		e.mu.Lock()
		var ref *Ref
		if !n.isClosed() && e.unresolved == nil && e.db != nil && e.h != nil && n.mine(e.h) && n.maintain(e) {
			ref = &Ref{e.key, e.h.Incarnation}
		}
		n.leave(e)
		if ref != nil { // the step superseded objects: collect them now, outside the gate (best effort)
			ctx, cancel := n.commitContext()
			defer cancel()
			n.Collect(ctx, *ref)
		}
	}()
}

// maintain publishes a snapshot when the replay pages exceed page_count+256,
// else compacts the oldest run of fanout same-level deltas (PROTOCOL §9),
// and reports whether it committed one. A failure abandons the step:
// nothing acknowledged depends on it.
func (n *Node) maintain(e *entity) bool {
	ctx, cancel := n.commitContext()
	defer cancel()
	h := e.h
	next := *h
	next.Revision, next.Nonce = h.Revision+1, head.NewID()
	c := &candidate{base: h, baseETag: e.etag}
	var buf bytes.Buffer
	var ref head.Ref
	if e.replay() > uint64(h.PageCount)+256 {
		snap, err := e.db.Snapshot(&buf)
		if err != nil || snap.Ref.PostChecksum != h.DatabaseChecksum || snap.Ref.MaxSequence != h.Sequence {
			e.discard()
			return false
		}
		ref = placed(h, snap.Ref)
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
			return false
		}
		run := h.Deltas[i : i+fanout]
		objs := make([]io.ReaderAt, fanout)
		for j, r := range run {
			obj, err := n.st.Get(ctx, r.Key, "", int64(r.Bytes))
			if err != nil {
				return false
			}
			objs[j] = bytes.NewReader(obj.Body)
		}
		out, err := engine.Compact(ctx, &buf, run, objs) // checks lengths, SHA-256, the chain and the span
		if err != nil {
			return false
		}
		ref = placed(h, out.Ref)
		ref.Level = run[0].Level + 1
		next.Deltas = slices.Concat(h.Deltas[:i], []head.Ref{ref}, h.Deltas[i+fanout:])
		c.pages = slices.Concat(e.pages[:1+i], []uint32{out.Pages}, e.pages[1+i+fanout:])
	}
	failpoint("maint.put")
	if n.putObject(ctx, ref.Key, buf.Bytes()) != nil {
		return false
	}
	failpoint("maint.cas")
	c.next, c.object = next, &ref
	_, err := n.publish(e, c) // not committed: the object is abandoned to collection
	failpoint("maint.done")
	return err == nil
}

// placed keys r as a new object based on h's revision.
func placed(h *head.Head, r head.Ref) head.Ref {
	r.Key, r.BaseRevision = head.ObjectKey(h.Key, h.Incarnation, h.Revision, head.NewID(), r.SHA256), h.Revision
	return r
}

// Collect deletes unreachable immutable artifacts of one entity (PLAN §9.2,
// PROTOCOL §10): objects of ref's incarnation whose base revision is below
// the current head's revision and that the head does not reference. It runs
// on any node and opens no SQLite.
func (n *Node) Collect(ctx context.Context, ref Ref) error {
	h, err := n.current(ctx, ref.Key, ref.Incarnation)
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
				switch err := n.st.Delete(ctx, o.Key); store.OutcomeOf(err) {
				case store.OK:
					n.counters.collected.Add(1)
					n.counters.collectedBytes.Add(uint64(o.Size))
				case store.Failed:
					return storeError(err)
				} // Unknown: left for a later pass
			}
		}
		if !more || len(page) == 0 {
			n.counters.collections.Add(1)
			return nil
		}
	}
}
