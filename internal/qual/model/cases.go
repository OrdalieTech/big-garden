package model

import "fmt"

// Case is a bounded configuration: the nodes, entities and client requests, the
// environment budgets and the exploration depth (PLAN §14 M1).
type Case struct {
	Name        string
	Nodes, Ents int
	Reqs        []Req
	Msgs        int // storage requests in flight
	Active      int // concurrent operations
	Depth       int
	Restarts    int // node crash/restart events (cache loss, new session)
	Jumps       int // arbitrary clock changes
	Losses      int // administrative head deletions (G6)
	Hedges      int // duplicate CAS sends (D3)
	ClockMax    int8
	Retention   int8 // ReceiptRetention in clock units
	Age         int8 // age threshold of the age-only GC control
	// SplitReads delivers read replies (GET, LIST) as separate transitions.
	// Bounded cases fuse a read's delivery into its apply: the delivery step
	// reads nothing from the store (the result is fixed at apply), so moving it
	// back to the apply point only makes the reader act earlier on the same
	// information, and every later request it sends can still be delayed. Writes
	// always keep independent apply, delivery, loss and late application.
	SplitReads bool
	Init       State
}

const all = 0xff

// Cases returns the M1 bounded cases P, O, G, I (PLAN §14) and C, which covers
// Create, the D5 resume and head loss.
func Cases() []*Case {
	return []*Case{caseP(), caseO(), caseG(), caseI(), caseC()}
}

// CaseByName returns one bounded case, or the random case "R<nodes><entities><active>".
func CaseByName(name string) (*Case, error) {
	for _, c := range Cases() {
		if c.Name == name {
			return c, nil
		}
	}
	var n, e, a int
	if _, err := fmt.Sscanf(name, "R%1d%1d%1d", &n, &e, &a); err == nil {
		return CaseR(n, e, a), nil
	}
	return nil, fmt.Errorf("unknown case %q", name)
}

func genesisReq(ts int8) Req { return Req{Kind: kCreate, ID: 1, Hash: 1, TS: ts, Schema: 1} }

// P: two nodes, one entity, two mutations, one takeover, four messages, depth 32.
func caseP() *Case {
	c := &Case{Name: "P", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 32,
		Restarts: 1, Hedges: 1, ClockMax: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, Nodes: 3, Tries: 2, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1, Input: 2},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kRead, Nodes: 3, Tries: 1, Schema: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, nil, 0, true)
	return c
}

// O: three nodes, one entity, three mutations, two takeovers, eight messages, depth 64.
func caseO() *Case {
	c := &Case{Name: "O", Nodes: 3, Ents: 1, Msgs: 8, Active: 3, Depth: 64,
		Restarts: 1, Hedges: 1, ClockMax: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, Nodes: all, Tries: 2, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: all, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1, Input: 2},
		{Kind: kMigrate, Nodes: all, Tries: 1, ID: 4, Hash: 1, TS: 1, Schema: 1, To: 2},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kTakeover, Nodes: 4, Tries: 1},
		{Kind: kRead, Nodes: all, Tries: 1, Schema: 1},
		{Kind: kLookup, Nodes: all, Tries: 1, ID: 2, TS: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, nil, 0, true)
	return c
}

// G: two nodes, one entity with an initial two-delta root, one compaction, one
// mutation and one collector. The second node needs a takeover to mutate, which
// is also what can defeat the compaction's CAS.
func caseG() *Case {
	c := &Case{Name: "G", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 64,
		Hedges: 1, ClockMax: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCompact, Nodes: 1, Tries: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 4, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCollect, Nodes: 3, Tries: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1, 2}, 0, true)
	return c
}

// I: two IDs, one floor advancement and a delayed retry. Clocks may jump to any
// value in 0..4 twice; retention is 2.
func caseI() *Case {
	c := &Case{Name: "I", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 32,
		Jumps: 2, ClockMax: 4, Retention: 2, Age: 1}
	c.Reqs = []Req{
		genesisReq(3),
		{Kind: kUpdate, Nodes: 3, Tries: 2, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 1, Tries: 1, ID: 3, Hash: 1, TS: 3, Schema: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, nil, 0, true)
	return c
}

// C: creation races on one key: two creates with different IDs, a retry, a
// crash, one administrative head deletion and a mutation that leaves base > 0
// artifacts.
func caseC() *Case {
	c := &Case{Name: "C", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 40,
		Restarts: 1, Losses: 1, Hedges: 1, ClockMax: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		{Kind: kCreate, Nodes: 3, Tries: 2, ID: 1, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCreate, Nodes: 3, Tries: 1, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
	}
	c.Init = c.newState()
	return c
}

// CaseR is the random-trace configuration: entity 0 exists with a two-delta
// root, entity 1 does not; every operation kind is in the pool.
func CaseR(nodes, ents, active int) *Case {
	c := &Case{Name: fmt.Sprintf("R%d%d%d", nodes, ents, active), Nodes: nodes, Ents: ents, Msgs: maxMsgs, Active: active, Depth: 150,
		Restarts: 2, Jumps: 2, Losses: 1, Hedges: 2, ClockMax: 4, Retention: 2, Age: 1, SplitReads: true}
	c.Reqs = []Req{
		genesisReq(3),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 3, Schema: 1},
		{Kind: kUpdate, ID: 3, Hash: 1, TS: 3, Schema: 1},
		{Kind: kUpdate, Nodes: all, Tries: 2, ID: 4, Hash: 1, TS: 2, Schema: 1},
		{Kind: kUpdate, Nodes: all, Tries: 2, ID: 5, Hash: 1, TS: 2, Schema: 1, Input: 2},
		{Kind: kUpdate, Nodes: all, Tries: 1, ID: 5, Hash: 2, TS: 2, Schema: 1},
		{Kind: kMigrate, Nodes: all, Tries: 1, ID: 6, Hash: 1, TS: 3, Schema: 1, To: 2},
		{Kind: kRead, Nodes: all, Tries: 3, Schema: 1},
		{Kind: kLookup, Nodes: all, Tries: 2, ID: 4, TS: 2},
		{Kind: kTakeover, Nodes: all &^ 1, Tries: 2},
		{Kind: kRelease, Nodes: all, Tries: 1},
		{Kind: kCompact, Nodes: all, Tries: 2},
		{Kind: kCollect, Nodes: all, Tries: 2},
	}
	if ents > 1 {
		c.Reqs = append(c.Reqs,
			Req{Kind: kCreate, Ent: 1, Nodes: all, Tries: 2, ID: 7, Hash: 1, TS: 3, Schema: 1},
			Req{Kind: kCreate, Ent: 1, Nodes: all, Tries: 1, ID: 7, Hash: 2, TS: 3, Schema: 1},
			Req{Kind: kUpdate, Ent: 1, Nodes: all, Tries: 2, ID: 8, Hash: 1, TS: 3, Schema: 1})
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1, 2}, 0, true)
	return c
}

func (c *Case) newState() State {
	var s State
	for n := 0; n < c.Nodes; n++ {
		s.Fresh++
		s.Nodes[n] = Node{Sess: s.Fresh, Clock: 1}
	}
	return s
}

// existing makes entity e exist as if request cr created it and the requests
// ups were then applied one delta each, owned by node owner.
func (c *Case) existing(s *State, e, cr int, ups []int, owner int, warm bool) {
	fresh := func() ID { s.Fresh++; return s.Fresh }
	en := &s.Ents[e]
	inc := fresh()
	img := Image{Seq: 1, Schema: c.Reqs[cr].Schema, Kept: 1}
	img.Ops[0] = int8(cr + 1)
	en.Birth = Birth{Present: true, Req: int8(cr + 1), Inc: inc, Att: fresh()}
	snap := Obj{Att: fresh(), Inc: inc, Min: 1, Max: 1, Snap: true, Present: true, Post: img}
	en.Objs[0] = snap
	en.NObj = 1
	h := Head{Inc: inc, Rev: 1, Nonce: fresh(), Owner: int8(owner + 1), Sess: s.Nodes[owner].Sess,
		Epoch: 1, Img: img, Snap: ref(snap)}
	for _, u := range ups {
		pre := img
		img.Ops[img.Seq] = int8(u + 1)
		img.Kept |= 1 << img.Seq
		img.Seq++
		d := Obj{Att: fresh(), Inc: inc, Base: h.Rev, Min: img.Seq, Max: img.Seq, Present: true, Pre: pre, Post: img}
		en.Objs[en.NObj] = d
		en.NObj++
		h.Deltas[h.ND] = ref(d)
		h.ND++
		h.Rev++
		h.Nonce = fresh()
		h.Img = img
	}
	en.HasHead, en.Head = true, h
	s.Ghost[e].EverHead = true
	if warm {
		s.Mem[owner][e] = Mem{HasH: true, H: h, HasImg: true, Img: img}
	}
}

func ref(o Obj) Ref { return Ref{Att: o.Att, Base: o.Base, Min: o.Min, Max: o.Max, Level: o.Level} }
