package model

import (
	"fmt"
	"testing"
)

// The blob protocol of DECISIONS D33, checked exhaustively on its own: two
// Nodes, takeovers up to epoch maxEpoch, up to maxObj uploads, two names.
// Everything a Node knows is its own view: a stale owner keeps acting on
// its cached head until a CAS refuses it. The PUT of an upload may land at
// any time after it starts, also after its caller saw a failure. The
// invariant: every object a committed name holds exists in the store.

const (
	maxEpoch = 3
	maxObj   = 2
	names    = 2
	nodes    = 2
)

// Upload states in a Node's table (Node.uploads).
const (
	upNone     = iota
	upInflight // registered before the PUT; never fresh
	upFresh    // PUT done, window open
	upExpired  // failed or past its window: kept until a sweep prunes it
)

type obj struct {
	Started bool
	Epoch   int8 // the uploader's epoch, part of the object's name
	Landed  bool // the PUT reached the store (it may still be in flight)
	Present bool // in the store now
}

type bnode struct {
	Epoch  int8          // the epoch of the head this Node caches
	Owner  bool          // the cached head names it
	Image  [names]int8   // the cached image's names (object index + 1, 0 = none)
	Up     [maxObj]uint8 // its uploads
	Listed [maxObj]bool  // a sweep's listing, between list and judge
	Swept  bool          // listing taken
	Dead   [maxObj]bool  // judged dead, to delete
}

type bstate struct {
	Epoch int8        // the committed head's epoch
	Image [names]int8 // the committed image
	Objs  [maxObj]obj
	N     [nodes]bnode
}

// mutant selects a negative control: each must find a violation.
type mutant int

const (
	sound      mutant = iota
	judgeFirst        // judge on the image before listing (Aldo's ordering hole)
	anyEpoch          // delete objects of epochs above the sweeper's
	staleLink         // link an object of another epoch or past its window
	noInflight        // a judge does not protect uploads in flight
	noFresh           // a judge does not protect fresh uploads
)

func linked(img [names]int8, o int) bool {
	for _, v := range img {
		if int(v) == o+1 {
			return true
		}
	}
	return false
}

func (s bstate) check() error {
	for i, v := range s.Image {
		if v != 0 && !s.Objs[v-1].Present {
			return fmt.Errorf("name %d holds object %d, which the store does not have", i, v-1)
		}
	}
	return nil
}

// next returns every successor of s.
func (s bstate) next(m mutant) []bstate {
	var out []bstate
	add := func(t bstate) { out = append(out, t) }
	for o := range maxObj {
		if ob := s.Objs[o]; ob.Started && !ob.Landed { // the PUT lands
			t := s
			t.Objs[o].Landed, t.Objs[o].Present = true, true
			add(t)
		}
	}
	for ni := range nodes {
		n := s.N[ni]
		if s.Epoch < maxEpoch { // ni takes over: a CAS on the current head, so it learns it
			t := s
			t.Epoch++
			t.N[ni].Epoch, t.N[ni].Owner, t.N[ni].Image = t.Epoch, true, s.Image
			// the other Node, if it owned, keeps believing it does until a CAS refuses it
			add(t)
		}
		if !n.Owner {
			continue
		}
		cas := func(t *bstate, img [names]int8) { // a publication: commits only at the current epoch
			if n.Epoch == s.Epoch {
				t.Image, t.N[ni].Image = img, img
			} else {
				t.N[ni].Owner = false // refused: the head moved
			}
		}
		for o := range maxObj {
			switch {
			case !s.Objs[o].Started: // PutBlob: register, then PUT
				t := s
				t.Objs[o] = obj{Started: true, Epoch: n.Epoch}
				t.N[ni].Up[o] = upInflight
				add(t)
				goto started // one fresh object per step keeps the search small
			case n.Up[o] == upInflight && s.Objs[o].Landed: // the PUT answered OK
				t := s
				t.N[ni].Up[o] = upFresh
				add(t)
			}
			if n.Up[o] == upInflight { // the PUT answered an error (it may still land)
				t := s
				t.N[ni].Up[o] = upExpired
				add(t)
			}
			if n.Up[o] == upFresh { // the window passes
				t := s
				t.N[ni].Up[o] = upExpired
				add(t)
			}
		}
	started:
		for name := range names {
			for o := range maxObj { // SetFile(name, o)
				ok := linked(n.Image, o) || n.Up[o] == upFresh && s.Objs[o].Epoch == n.Epoch
				if m == staleLink {
					ok = ok || s.Objs[o].Started && s.Objs[o].Landed
				}
				if ok && n.Image[name] != int8(o+1) {
					t := s
					img := n.Image
					img[name] = int8(o + 1)
					cas(&t, img)
					add(t)
				}
			}
			if n.Image[name] != 0 { // SetFile(name, nil)
				t := s
				img := n.Image
				img[name] = 0
				cas(&t, img)
				add(t)
			}
		}
		protected := func(o int) bool {
			up := n.Up[o]
			return linked(n.Image, o) || up == upFresh && m != noFresh || up == upInflight && m != noInflight
		}
		if !n.Swept { // sweep, step 1: list (judgeFirst: take the protected set first instead)
			t := s
			t.N[ni].Swept = true
			for o := range maxObj {
				t.N[ni].Listed[o] = s.Objs[o].Present
				if m == judgeFirst {
					t.N[ni].Listed[o] = protected(o)
				}
			}
			add(t)
		}
		if n.Swept { // step 2, under the entity mutex on the cached image: judge (judgeFirst: list now)
			t := s
			for o := range maxObj {
				dead := n.Listed[o] && !protected(o)
				if m == judgeFirst {
					dead = s.Objs[o].Present && !n.Listed[o]
				}
				t.N[ni].Dead[o] = dead && (s.Objs[o].Epoch <= n.Epoch || m == anyEpoch)
				if up := n.Up[o]; up == upExpired || up == upFresh && linked(n.Image, o) {
					t.N[ni].Up[o] = upNone // pruned: a committed link protects it from now on
				}
			}
			t.N[ni].Swept, t.N[ni].Listed = false, [maxObj]bool{}
			add(t)
		}
		for o := range maxObj { // delete one judged object
			if n.Dead[o] {
				t := s
				t.N[ni].Dead[o], t.Objs[o].Present = false, false
				add(t)
			}
		}
	}
	return out
}

// search explores every state reachable from the genesis (epoch 1, owned by
// Node 0) and returns the first violation, with the number of states.
// Two objects suffice for every ordering hole the controls below exhibit;
// three, with stale owners, outgrow a 4 GiB search.
func searchBlobs(m mutant) (int, error) {
	start := bstate{Epoch: 1}
	start.N[0] = bnode{Epoch: 1, Owner: true}
	seen := map[bstate]bool{start: true}
	queue := []bstate{start}
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

func TestBlobProtocol(t *testing.T) {
	n, err := searchBlobs(sound)
	if err != nil {
		t.Fatalf("sound protocol: %v after %d states", err, n)
	}
	t.Logf("sound: %d states, no violation", n)
	for _, c := range []struct {
		name string
		m    mutant
	}{{"judge before list", judgeFirst}, {"delete a later epoch", anyEpoch}, {"link a stale blob", staleLink},
		{"no in-flight protection", noInflight}, {"no fresh protection", noFresh}} {
		if k, err := searchBlobs(c.m); err == nil {
			t.Errorf("control %q found no violation in %d states", c.name, k)
		} else {
			t.Logf("control %q: %v (%d states)", c.name, err, k)
		}
	}
}
