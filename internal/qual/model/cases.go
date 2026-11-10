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
	// LossAtRest allows a deletion only while no create of the key is in
	// flight, the scope in which DECISIONS D14 keeps G6's missing-head clause.
	LossAtRest bool
	Hedges     int    // duplicate CAS sends (D3)
	Clocks     []int8 // values a clock jump may set
	Retention  int8   // ReceiptRetention in clock units
	Age        int8   // age threshold of the age-only GC control
	// Split delivers replies as separate transitions. Bounded cases deliver a
	// reply at its request's apply point instead, with per-request client
	// abandonment and orphans that may still apply later. That is complete: a
	// delivery reads only the reply fixed at apply, so delaying it only delays
	// the client's own local step, and "applied, reply delayed, then lost" is
	// "abandoned, then applied as an orphan" (including an original CAS applying
	// while its hedge answers 412). Fused traces are valid split traces, so no
	// false alarm is possible; the random traces keep every reply split.
	Split bool
	Init  State
}

const all = 0xff

// Cases returns the M1 bounded cases P, O, G, I (PLAN §14) and three creation
// cases: C (Create, the D5 resume and an administrative head deletion at any
// time: finding F1, accepted by D14), C0 (no deletion) and CD (deletion only
// while no create of the key is in flight, D14's scope), and the group-commit
// case B (D18) and its read-member variant BR (C11).
func Cases() []*Case {
	return []*Case{caseP(), caseO(), caseG(), caseI(), caseC(), caseC0(), caseCD(), caseB(), caseBR()}
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
		Restarts: 1, Hedges: 1, Retention: 8, Age: 1}
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
		Restarts: 1, Hedges: 1, Retention: 8, Age: 1}
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
		Hedges: 1, Retention: 8, Age: 1}
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

// I: two IDs, one floor advancement and a delayed retry. Retention is 2 and
// the request timestamps are 1 and 3, so clock values fall in two classes,
// {0,1,2} (nothing expires) and {3,4} (timestamp 1 expires, the floor reaches
// it); clocks start at 1 and may jump twice between 1 and 3.
func caseI() *Case {
	c := &Case{Name: "I", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 32,
		Jumps: 2, Clocks: []int8{1, 3}, Retention: 2, Age: 1}
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
		Restarts: 1, Losses: 1, Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		{Kind: kCreate, Nodes: 3, Tries: 2, ID: 1, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCreate, Nodes: 3, Tries: 1, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
	}
	c.Init = c.newState()
	return c
}

// C0 is C without the head deletion: creation, D5 and the races around them
// within the store contract (see .work finding F1 for C itself).
func caseC0() *Case {
	c := caseC()
	c.Name, c.Losses = "C0", 0
	return c
}

// CD is C with the head deletion restricted to D14's scope: no create of the
// key in flight. G6 must hold; D5's LIST check and D16's object-before-tail
// rule are what make it hold.
func caseCD() *Case {
	c := caseC()
	c.Name, c.LossAtRest = "CD", true
	return c
}

// B: group commit (D18). Two nodes, one entity; r1 may run alone or as the
// leader of a batch whose member r2 is queued behind it; both may be retried
// alone on either node; one takeover by n1, one crash, one hedge; replies may
// be delayed or lost (abandoned writes still apply). A batch retried after
// its member committed holds the member's replay.
func caseB() *Case {
	c := &Case{Name: "B", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 40,
		Restarts: 1, Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, Nodes: 3, Tries: 2, ID: 2, Hash: 1, TS: 1, Schema: 1, More: 3},
		{Kind: kUpdate, Nodes: 3, Tries: 2, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, nil, 0, true)
	return c
}

// BR: reads ride a batch's publication (C11). B with the queued member a
// read: r1 may run alone or with the read r2 queued behind it, which then
// answers from the state r1's committed publication proved; r2 may also run
// alone on either node; one takeover by n1, one crash, one hedge.
func caseBR() *Case {
	c := caseB()
	c.Name = "BR"
	c.Reqs[2] = Req{Kind: kRead, Nodes: 3, Tries: 2, Schema: 1}
	return c
}

// CaseR is the random-trace configuration: entity 0 exists with a two-delta
// root, entity 1 does not; every operation kind is in the pool.
func CaseR(nodes, ents, active int) *Case {
	c := &Case{Name: fmt.Sprintf("R%d%d%d", nodes, ents, active), Nodes: nodes, Ents: ents, Msgs: maxMsgs, Active: active, Depth: 150,
		Restarts: 2, Jumps: 2, Losses: 1, Hedges: 2, Clocks: []int8{0, 1, 2, 3, 4}, Retention: 2, Age: 1, Split: true,
		LossAtRest: true}
	c.Reqs = []Req{
		genesisReq(3),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 3, Schema: 1},
		{Kind: kUpdate, ID: 3, Hash: 1, TS: 3, Schema: 1},
		{Kind: kUpdate, Nodes: all, Tries: 2, ID: 4, Hash: 1, TS: 2, Schema: 1, More: 5}, // D18 leader, r4 queued behind it
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
