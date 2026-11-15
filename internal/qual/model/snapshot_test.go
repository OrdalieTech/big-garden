package model

import (
	"fmt"
	"testing"
)

// The off-mutex snapshot of DECISIONS D35, checked exhaustively on its own:
// one head, two Nodes acting on their cached copy of it (a CAS succeeds only
// against the current head; a refused one refreshes it), writes adding delta objects, compactions,
// takeovers, crashes, an off-mutex snapshot (announce, upload, final CAS or
// abandon) and a collector that reads the head, lists, then deletes one
// object at a time. Invariants: every object the head names exists, and its
// chain covers 1..sequence without a gap or an overlap.

const (
	sObjs   = 6 // object ids 1..sObjs
	sDeltas = 3
	sSeq    = 3
	sRev    = 7
)

type sobj struct {
	Exists   bool
	Base     int8 // revision of the head it was produced under
	Attempt  int8 // a pending snapshot's attempt, 0 otherwise
	Min, Max int8 // sequences covered
}

type shead struct {
	Rev, Owner, Seq int8
	Snap            int8
	Deltas          [sDeltas]int8
	ND              int8
	PendA, PendRev  int8
}

type snode struct {
	H                         shead // cached
	Job                       int8  // 0 none, 1 copied, 2 uploaded
	JobA, JobS0, JobRev, JobO int8
}

type sstate struct {
	H        shead
	O        [sObjs + 1]sobj
	N        [2]snode
	GPhase   int8 // 0 idle, 1 head read, 2 listed
	GH       shead
	GListed  [sObjs + 1]bool
	Attempts int8
}

type smutant int

const (
	ssound      smutant = iota
	sNoProtect          // collection ignores the pending attempt
	sCompact            // compaction while a snapshot is pending and no straddle check: either defense alone suffices
	sNoPendWait         // the final CAS does not require the intent still in the head
	sBaseBefore         // the snapshot object is produced under the revision before the intent

)

func (h *shead) refs() []int8 {
	r := []int8{h.Snap}
	return append(r, h.Deltas[:h.ND]...)
}

func (s *sstate) newObj(o sobj) int8 {
	for id := int8(1); id <= sObjs; id++ {
		if !s.O[id].Exists && s.O[id] == (sobj{}) {
			o.Exists = true
			s.O[id] = o
			return id
		}
	}
	return 0
}

func (s sstate) check() error {
	next := int8(1)
	for _, id := range s.H.refs() {
		o := s.O[id]
		if !o.Exists {
			return fmt.Errorf("head rev %d names object %d, which the store does not have", s.H.Rev, id)
		}
		if o.Min != next {
			return fmt.Errorf("head rev %d: object %d starts at %d, want %d", s.H.Rev, id, o.Min, next)
		}
		next = o.Max + 1
	}
	if next-1 != s.H.Seq {
		return fmt.Errorf("head rev %d: chain ends at %d, sequence %d", s.H.Rev, next-1, s.H.Seq)
	}
	return nil
}

func (s sstate) next(m smutant) []sstate {
	var out []sstate
	add := func(t sstate) { out = append(out, t) }
	if s.H.Rev >= sRev {
		return nil
	}
	for ni := range 2 {
		n := s.N[ni]
		fresh := n.H == s.H
		mine := n.H.Owner == int8(ni)
		cas := func(t *sstate, h shead) bool { // publish h against the cached head
			if !fresh {
				t.N[ni].H = s.H // refused: refreshed
				return false
			}
			h.Rev = s.H.Rev + 1
			t.H, t.N[ni].H = h, h
			return true
		}
		{ // takeover (a CAS on the current head, so always fresh)
			t := s
			h := s.H
			h.Owner, h.Rev = int8(ni), s.H.Rev+1
			t.H, t.N[ni].H = h, h
			add(t)
		}
		if n.Job != 0 { // crash: the job is lost, its object may stay
			t := s
			t.N[ni].Job = 0
			add(t)
		}
		if !mine {
			continue
		}
		if s.H.Seq < sSeq && n.H.ND < sDeltas { // write: PUT the delta, then CAS
			t := s
			if id := t.newObj(sobj{Base: n.H.Rev, Min: n.H.Seq + 1, Max: n.H.Seq + 1}); id != 0 {
				h := n.H
				h.Deltas[h.ND], h.ND, h.Seq = id, h.ND+1, h.Seq+1
				cas(&t, h)
				add(t)
			}
		}
		if ni == 0 && n.H.ND >= 2 && (n.H.PendA == 0 || m == sCompact) { // compaction of the first two deltas
			t := s
			a, b := s.O[n.H.Deltas[0]], s.O[n.H.Deltas[1]]
			if id := t.newObj(sobj{Base: n.H.Rev, Min: a.Min, Max: b.Max}); id != 0 {
				h := n.H
				h.Deltas = [sDeltas]int8{id, h.Deltas[2]}
				h.ND--
				cas(&t, h)
				add(t)
			}
		}
		if ni == 0 && n.H.PendA == 0 && n.Job == 0 && s.Attempts < 2 { // announce, then copy the image under the mutex (Node 0 only: Node 1 takes over, writes and drops intents)
			t := s
			t.Attempts++
			h := n.H
			h.PendA, h.PendRev = t.Attempts, n.H.Rev+1
			if cas(&t, h) {
				t.N[ni].Job, t.N[ni].JobA, t.N[ni].JobS0, t.N[ni].JobRev = 1, h.PendA, h.Seq, h.Rev
			}
			add(t)
		}
		if n.PendA0() && n.Job == 0 { // drop an intent nobody here runs
			t := s
			h := n.H
			h.PendA, h.PendRev = 0, 0
			cas(&t, h)
			add(t)
		}
	}
	for ni := range 2 { // the off-mutex part of a job runs whoever owns
		n := s.N[ni]
		if n.Job == 1 { // upload
			t := s
			base := n.JobRev
			if m == sBaseBefore {
				base--
			}
			if id := t.newObj(sobj{Base: base, Attempt: n.JobA, Min: 1, Max: n.JobS0}); id != 0 {
				t.N[ni].Job, t.N[ni].JobO = 2, id
				add(t)
			}
		}
		if n.Job == 2 { // final, on the cached head
			t := s
			t.N[ni].Job = 0
			h := n.H
			ok := h.Owner == int8(ni) && (h.PendA == n.JobA || m == sNoPendWait) && s.O[h.Snap].Max <= n.JobS0
			var kept []int8
			for _, d := range h.Deltas[:h.ND] {
				switch o := s.O[d]; {
				case o.Max <= n.JobS0:
				case o.Min > n.JobS0 || m == sCompact:
					kept = append(kept, d)
				default:
					ok = false
				}
			}
			if ok {
				h.Snap, h.Deltas, h.ND, h.PendA, h.PendRev = n.JobO, [sDeltas]int8{}, int8(len(kept)), 0, 0
				copy(h.Deltas[:], kept)
				if n.H == s.H {
					h.Rev = s.H.Rev + 1
					t.H, t.N[ni].H = h, h
				} else {
					t.N[ni].H = s.H // refused: the object is left to collection
				}
			} else {
				if h.Owner == int8(ni) && h.PendA == n.JobA && n.H == s.H { // abandon: drop the intent
					h.PendA, h.PendRev, h.Rev = 0, 0, s.H.Rev+1
					t.H, t.N[ni].H = h, h
				}
				t.O[n.JobO].Exists = false // and delete the object no head took
			}
			add(t)
		}
	}
	switch s.GPhase { // the collector
	case 0:
		t := s
		t.GPhase, t.GH = 1, s.H
		add(t)
	case 1:
		t := s
		t.GPhase = 2
		for id := range sObjs + 1 {
			t.GListed[id] = s.O[id].Exists
		}
		add(t)
	case 2:
		live := map[int8]bool{}
		for _, id := range s.GH.refs() {
			live[id] = true
		}
		t := s // delete everything the listing and the head read allow, at once
		t.GPhase, t.GListed = 0, [sObjs + 1]bool{}
		for id := int8(1); id <= sObjs; id++ {
			o := s.O[id]
			protected := s.GH.PendA != 0 && o.Attempt == s.GH.PendA && m != sNoProtect
			if s.GListed[id] && o.Exists && o.Base < s.GH.Rev && !live[id] && !protected {
				t.O[id].Exists = false
			}
		}
		add(t)
	}
	return out
}

// PendA0 reports whether the cached head carries an intent.
func (n snode) PendA0() bool { return n.H.PendA != 0 }

func searchSnapshots(m smutant) (int, error) {
	start := sstate{}
	start.O[1] = sobj{Exists: true, Min: 1, Max: 1}
	start.H = shead{Rev: 1, Owner: 0, Seq: 1, Snap: 1}
	start.N[0].H, start.N[1].H = start.H, start.H
	seen := map[sstate]bool{start: true}
	queue := []sstate{start}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if err := s.check(); err != nil {
			return len(seen), err
		}
		for _, t := range s.next(m) {
			if !seen[t] {
				seen[t] = true
				queue = append(queue, t)
			}
		}
	}
	return len(seen), nil
}

func TestSnapshotProtocol(t *testing.T) {
	n, err := searchSnapshots(ssound)
	if err != nil {
		t.Fatalf("sound protocol: %v after %d states", err, n)
	}
	t.Logf("sound: %d states, no violation", n)
	for _, c := range []struct {
		name string
		m    smutant
	}{{"collection ignores the intent", sNoProtect}, {"compaction while pending, straddling deltas kept", sCompact},
		{"final CAS without the intent", sNoPendWait}, {"object under the previous revision", sBaseBefore}} {
		if k, err := searchSnapshots(c.m); err == nil {
			t.Errorf("control %q found no violation in %d states", c.name, k)
		} else {
			t.Logf("control %q: %v (%d states)", c.name, err, k)
		}
	}
}
