package garden

import (
	"context"
	"os"
	"slices"
	"sync/atomic"
	"time"

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
		var ref, sweep *Ref
		var job *snapJob
		if !n.isClosed() && e.unresolved == nil && e.db != nil && e.h != nil && n.mine(e.h) {
			if e.db.SweepDue {
				sweep = &Ref{e.key, e.h.Incarnation}
			}
			had := e.snap
			if n.maintain(e, false) {
				ref = &Ref{e.key, e.h.Incarnation}
			}
			if e.snap != had {
				job = e.snap
			}
		}
		if job != nil { // off the mutex, still holding e (D35)
			e.mu.Unlock()
			if n.runSnapshot(e, job) {
				ref = &Ref{e.key, job.inc}
			}
			e.mu.Lock()
		}
		n.leave(e)
		ctx, cancel := n.commitContext()
		defer cancel()
		if ref != nil { // the step superseded objects: collect them now, outside the gate (best effort)
			n.Collect(ctx, *ref)
		}
		if sweep != nil {
			n.sweep(ctx, *sweep)
		}
	}()
}

// maintain publishes a snapshot when the replay pages exceed page_count+256
// (or always with snapshot), else compacts the oldest run of fanout
// same-level deltas (PROTOCOL §9), and reports whether it committed one. A
// failure abandons the step: nothing acknowledged depends on it.
func (n *Node) maintain(e *entity, snapshot bool) bool {
	ctx, cancel := n.commitContext()
	defer cancel()
	h := e.h
	next := *h
	next.Revision, next.Nonce = h.Revision+1, head.NewID()
	c := &candidate{base: h, baseETag: e.etag}
	buf := n.spool()
	defer buf.Close()
	var ref head.Ref
	switch {
	case h.Pending != nil && e.snap == nil && !snapshot: // a snapshot nobody here runs (a crash, another session): drop the intent
		next.Pending = nil
		c.next, c.pages = next, e.pages
		_, err := n.publish(e, c)
		return err == nil
	case h.Pending != nil && !snapshot: // no compaction across the snapshot's sequence
		return false
	case !snapshot && e.replay() > uint64(h.PageCount)+256 && h.PageCount >= n.offGate && len(h.Deltas) < head.MaxDeltas:
		return n.startSnapshot(ctx, e)
	}
	if snapshot || e.replay() > uint64(h.PageCount)+256 {
		snap, err := e.db.Snapshot(buf)
		if err != nil || snap.Ref.PostChecksum != h.DatabaseChecksum || snap.Ref.MaxSequence != h.Sequence {
			e.discard()
			return false
		}
		ref = placed(h, snap.Ref)
		next.Snapshot, next.Deltas, next.Tail, next.Pending, c.pages = ref, []head.Ref{}, nil, nil, []uint32{snap.Pages}
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
		objs, done, err := n.objects(ctx, run, h, new(atomic.Uint64)) // four at a time: the gate is held
		defer done()
		if err != nil {
			return false
		}
		out, err := engine.Compact(ctx, buf, run, objs) // checks lengths, SHA-256, the chain and the span
		if err != nil {
			return false
		}
		ref = placed(h, out.Ref)
		ref.Level = run[0].Level + 1
		next.Deltas = slices.Concat(h.Deltas[:i], []head.Ref{ref}, h.Deltas[i+fanout:])
		c.pages = slices.Concat(e.pages[:1+i], []uint32{out.Pages}, e.pages[1+i+fanout:])
	}
	failpoint("maint.put")
	if n.putObject(ctx, ref.Key, buf, ref.SHA256) != nil {
		return false
	}
	failpoint("maint.cas")
	c.next, c.object = next, &ref
	_, err := n.publish(e, c) // not committed: the object is abandoned to collection
	failpoint("maint.done")
	return err == nil
}

// snapJob is an off-mutex snapshot (D35): announced in the head of revision
// rev, encoded from file, a copy of the image img.
type snapJob struct {
	attempt head.ID
	inc     head.ID
	rev     uint64
	file    *os.File
	img     engine.Image
}

// startSnapshot announces an off-mutex snapshot and copies the image, under
// the mutex: the tail first becomes a delta, so that no later write merges
// across the snapshot's sequence. It reports whether it committed a head.
func (n *Node) startSnapshot(ctx context.Context, e *entity) bool {
	h, attempt := e.h, head.NewID()
	next := *h
	next.Revision, next.Nonce = h.Revision+1, head.NewID()
	next.Pending = &head.Pending{Attempt: attempt, Revision: next.Revision}
	c := &candidate{base: h, baseETag: e.etag, pages: e.pages}
	if h.Tail != nil {
		tail := n.spool()
		defer tail.Close()
		tail.Write(h.Tail.Data)
		ref := placed(h, h.Tail.Ref())
		if n.putObject(ctx, ref.Key, tail, ref.SHA256) != nil {
			return false
		}
		next.Deltas, next.Tail, c.object = append(slices.Clip(h.Deltas), ref), nil, &ref
	}
	c.next = next
	if _, err := n.publish(e, c); err != nil {
		return false
	}
	f, err := os.CreateTemp(n.dir, "image-*")
	if err != nil {
		return true // the intent is dropped by a later step
	}
	img, err := e.db.CopyImage(f)
	if err != nil || img.Sequence != e.h.Sequence {
		f.Close()
		os.Remove(f.Name())
		return true
	}
	e.snap = &snapJob{attempt, e.h.Incarnation, next.Revision, f, img}
	n.staging.Add(int64(img.PageCount) * head.PageSize) // the copy counts in CacheBytes until the job ends (D37)
	return true
}

// runSnapshot encodes and uploads job's snapshot without the mutex, then
// takes it to replace the head's snapshot and the deltas it covers, keeping
// those written since; anything else (the intent gone, ownership lost, a
// newer snapshot, an object straddling its sequence) abandons it: the
// intent is dropped and the object deleted. It reports whether it
// committed a head.
func (n *Node) runSnapshot(e *entity, job *snapJob) bool {
	defer os.Remove(job.file.Name())
	defer job.file.Close()
	defer n.staging.Add(-int64(job.img.PageCount) * head.PageSize)
	ctx, cancel := context.WithCancel(context.Background()) // the upload may outlast CommitTimeout; Close ends it
	defer cancel()
	go func() {
		select {
		case <-n.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	buf := n.spool()
	defer buf.Close()
	snap, err := engine.SnapshotFile(buf, job.file, job.img)
	ref := snap.Ref
	ref.Key, ref.BaseRevision = head.ObjectKey(e.key, job.inc, job.rev, job.attempt, ref.SHA256), job.rev
	failpoint("snap.put")
	if err == nil {
		err = n.putObject(ctx, ref.Key, buf, ref.SHA256)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snap = nil
	h := e.h
	abandon := func() bool {
		committed := false
		if h != nil && h.Pending != nil && h.Pending.Attempt == job.attempt && e.unresolved == nil && n.mine(h) {
			next := *h
			next.Revision, next.Nonce, next.Pending = h.Revision+1, head.NewID(), nil
			_, perr := n.publish(e, &candidate{base: h, baseETag: e.etag, next: next, pages: e.pages})
			committed = perr == nil
		}
		dctx, dcancel := n.commitContext()
		n.st.Delete(dctx, ref.Key) // never referenced: no head took it
		dcancel()
		return committed
	}
	if err != nil || n.isClosed() || e.unresolved != nil || e.db == nil || h == nil || !n.mine(h) ||
		h.Pending == nil || h.Pending.Attempt != job.attempt || h.Snapshot.MaxSequence > job.img.Sequence {
		return abandon()
	}
	s0 := job.img.Sequence
	next := *h
	next.Revision, next.Nonce, next.Snapshot, next.Deltas, next.Pending = h.Revision+1, head.NewID(), ref, []head.Ref{}, nil
	pages := []uint32{snap.Pages}
	for i, d := range h.Deltas {
		switch {
		case d.MaxSequence <= s0: // covered by the snapshot
		case d.MinSequence > s0:
			next.Deltas, pages = append(next.Deltas, d), append(pages, e.pages[1+i])
		default:
			return abandon()
		}
	}
	if h.Tail != nil {
		if h.Tail.MinSequence <= s0 {
			return abandon()
		}
		pages = append(pages, e.pages[len(e.pages)-1])
	}
	failpoint("snap.cas")
	_, err = n.publish(e, &candidate{base: h, baseETag: e.etag, next: next, object: &ref, pages: pages})
	return err == nil // not committed: resolution decided, the object is collected unless a head holds it
}

// placed keys r as a new object based on h's revision.
func placed(h *head.Head, r head.Ref) head.Ref {
	r.Key, r.BaseRevision = head.ObjectKey(h.Key, h.Incarnation, h.Revision, head.NewID(), r.SHA256), h.Revision
	return r
}

// Snapshot publishes the entity's current image as a new snapshot and
// collects everything it supersedes, for erasure: afterwards no object holds
// a page older than the image, and with secure_delete the image holds no
// deleted bytes (D22). It requires ownership; it uploads the whole database.
func (n *Node) Snapshot(ctx context.Context, ref Ref) (Version, error) {
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return Version{}, err
	}
	if err := n.owned(ctx, e, ref, time.Time{}); err != nil { // a zero t0: a write, owner only
		n.leave(e)
		return Version{}, err
	}
	if !n.maintain(e, true) {
		n.leave(e)
		return Version{}, newError(CodeUnavailable, "the snapshot was not published", nil)
	}
	v := Version{e.h.Incarnation, e.h.Sequence}
	n.leave(e)
	return v, n.Collect(ctx, ref)
}

// Collect deletes unreachable immutable artifacts of one entity (PLAN §9.2,
// PROTOCOL §10): objects of ref's incarnation whose base revision is below
// the current head's revision and that the head does not reference. It runs
// on any node and opens no SQLite.
func (n *Node) Collect(ctx context.Context, ref Ref) error {
	h, err := n.current(ctx, ref.Key, head.ID{})
	if err != nil {
		return err
	}
	if ref.Incarnation != (head.ID{}) && h.Incarnation != ref.Incarnation {
		// The key was recreated: ref's incarnation ended in a tombstone below
		// h's revision (revisions are monotonic across incarnations), so all
		// of it is dead (D25).
		h = &head.Head{Key: ref.Key, Incarnation: ref.Incarnation, Revision: h.Revision, Deleted: true}
	}
	return n.reap(ctx, h)
}

// reap is Collect against h. Nothing of a tombstone is live: one pass erases
// its incarnation (D25).
func (n *Node) reap(ctx context.Context, h *head.Head) error {
	live := map[string]bool{}
	for _, r := range links(h) {
		live[r.Key] = !h.Deleted
	}
	pending := func(k string) bool { // an off-mutex snapshot's object, maybe not uploaded yet (D35)
		_, attempt, _, err := head.ParseObjectKey(head.ObjectPrefix(h.Key, h.Incarnation), k)
		return err == nil && h.Pending != nil && attempt == h.Pending.Attempt
	}
	prefix, blobs := head.ObjectPrefix(h.Key, h.Incarnation), head.BlobPrefix(h.Key, h.Incarnation)
	for after := ""; ; {
		page, more, err := n.st.List(ctx, prefix, after, 1000)
		if err != nil {
			return storeError(err)
		}
		for _, o := range page {
			after = o.Key
			base, _, _, err := head.ParseObjectKey(prefix, o.Key)
			if prefix == blobs || err == nil && base < h.Revision && !live[o.Key] && !pending(o.Key) { // a tombstone's blobs all go (D33)
				failpoint("collect.del")
				switch err := n.st.Delete(ctx, o.Key); store.OutcomeOf(err) {
				case store.OK:
					n.counters.collected.Add(1)
					n.counters.collectedBytes.Add(uint64(o.Size))
				case store.Failed:
					return storeError(err)
				} // Unknown: left for a later pass
			}
		}
		if (!more || len(page) == 0) && h.Deleted && prefix != blobs {
			prefix, after, more = blobs, "", true
			continue
		}
		if !more || len(page) == 0 {
			n.counters.collections.Add(1)
			return nil
		}
	}
}
