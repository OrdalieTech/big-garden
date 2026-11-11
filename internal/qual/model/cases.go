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
// case B (D18), its read-member variant BR (D23), the follower-read case
// F (D24), the deletion cases D, DT, DR and DU (D25), and the aged cases CA
// and DA (D31).
func Cases() []*Case {
	return []*Case{caseP(), caseO(), caseG(), caseI(), caseC(), caseC0(), caseCD(), caseB(), caseBR(), caseF(),
		caseD(), caseDT(), caseDR(), caseDU(), caseCA(), caseDA()}
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

// CA is CD aged (D31): retention 2 and one clock jump to 3, so every
// request (timestamp 1) may be admitted, resumed or answered after its ID
// expired, including the D5 resume over a genesis whose head was deleted.
func caseCA() *Case {
	c := caseCD()
	c.Name, c.Jumps, c.Clocks, c.Retention = "CA", 1, []int8{3}, 2
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

// BR: reads ride a batch's publication (D23). B with the queued member a
// read: r1 may run alone or with the read r2 queued behind it, which then
// answers from the state r1's committed publication proved; r2 may also run
// alone on either node; one takeover by n1, one crash, one hedge.
func caseBR() *Case {
	c := caseB()
	c.Name = "BR"
	c.Reqs[2] = Req{Kind: kRead, Nodes: 3, Tries: 2, Schema: 1}
	return c
}

// F: strong reads on a node that owns nothing (D24). Three nodes, one entity
// with a two-delta root owned by n0; n2 only reads (twice, so the second may
// reuse its local image), while n0 or n1 updates, n1 takes over, the owner
// compacts and n0 collects. One hedge, no crash.
func caseF() *Case {
	c := &Case{Name: "F", Nodes: 3, Ents: 1, Msgs: 4, Active: 3, Depth: 64,
		Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCompact, Nodes: 3, Tries: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 4, Hash: 1, TS: 1, Schema: 1},
		{Kind: kRead, Nodes: 4, Tries: 2, Schema: 1},
		{Kind: kCollect, Nodes: 1, Tries: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1, 2}, 0, true)
	return c
}

// D: delete vs update (D25). Two nodes, one entity with a one-delta root
// owned by n0; an update and a delete (retried once, so the retry finds the
// tombstone) on either node, a read on n1, a takeover by n1; one crash, one
// hedge. The delete's Collect runs inline.
func caseD() *Case {
	c := &Case{Name: "D", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 48,
		Restarts: 1, Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 3, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kDelete, Nodes: 3, Tries: 2},
		{Kind: kRead, Nodes: 2, Tries: 1, Schema: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1}, 0, true)
	return c
}

// DT: delete vs takeover. n0 owns and deletes (retried once) while n1 takes
// over and updates, and a lookup reads the update's receipt on either node;
// one hedge, one crash. A lost delete must not be called done.
func caseDT() *Case {
	c := &Case{Name: "DT", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 48,
		Restarts: 1, Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kDelete, Nodes: 1, Tries: 2},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kUpdate, Nodes: 2, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kLookup, Nodes: 3, Tries: 1, ID: 3, TS: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1}, 0, true)
	return c
}

// DR: two recreations. n0 owns the entity and deletes it; two creates with
// different IDs race to recreate it (one retried on either node, the other
// on n1); n0 updates
// the new incarnation and n1 the old Ref (incarnation_mismatch or
// not_found); one hedge, no crash.
func caseDR() *Case {
	c := &Case{Name: "DR", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 48,
		Hedges: 1, Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kDelete, Nodes: 1, Tries: 1},
		{Kind: kCreate, Nodes: 3, Tries: 2, ID: 5, Hash: 1, TS: 1, Schema: 1},
		{Kind: kCreate, Nodes: 2, Tries: 1, ID: 6, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 1, Tries: 1, ID: 7, Hash: 1, TS: 1, Schema: 1, Gen: 2},
		{Kind: kUpdate, Nodes: 2, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1}, 0, true)
	return c
}

// DA: a stale create over a tombstone (D31). n0 owns the entity and deletes
// it; the genesis request is retried on either node and another create
// recreates the entity, both possibly after their IDs expired (retention 2,
// one clock jump to 3); n0 updates the new incarnation. One hedge, no crash.
func caseDA() *Case {
	c := &Case{Name: "DA", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 48,
		Hedges: 1, Jumps: 1, Clocks: []int8{3}, Retention: 2, Age: 1}
	c.Reqs = []Req{
		{Kind: kCreate, Nodes: 3, Tries: 1, ID: 1, Hash: 1, TS: 1, Schema: 1},
		{Kind: kDelete, Nodes: 1, Tries: 1},
		{Kind: kCreate, Nodes: 3, Tries: 2, ID: 5, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 1, Tries: 1, ID: 7, Hash: 1, TS: 1, Schema: 1, Gen: 2},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, nil, 0, true)
	return c
}

// DU: an update's outcome across a deletion. n0's update may apply while n0
// stops waiting for it; n1 takes over, deletes and recreates the entity, and
// the update's own resolve then reads another incarnation: its receipt cannot
// be absent there for a reason of its own. No crash, no hedge.
func caseDU() *Case {
	c := &Case{Name: "DU", Nodes: 2, Ents: 1, Msgs: 4, Active: 3, Depth: 40,
		Retention: 8, Age: 1}
	c.Reqs = []Req{
		genesisReq(1),
		{Kind: kUpdate, ID: 2, Hash: 1, TS: 1, Schema: 1},
		{Kind: kUpdate, Nodes: 1, Tries: 1, ID: 3, Hash: 1, TS: 1, Schema: 1},
		{Kind: kTakeover, Nodes: 2, Tries: 1},
		{Kind: kDelete, Nodes: 2, Tries: 1},
		{Kind: kCreate, Nodes: 2, Tries: 1, ID: 5, Hash: 1, TS: 1, Schema: 1},
	}
	c.Init = c.newState()
	c.existing(&c.Init, 0, 0, []int{1}, 0, true)
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
		{Kind: kDelete, Nodes: all, Tries: 2},
		{Kind: kCreate, Nodes: all, Tries: 2, ID: 9, Hash: 1, TS: 3, Schema: 1}, // recreation after the delete
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
	h := Head{Inc: inc, Gen: 1, Rev: 1, Nonce: fresh(), Owner: int8(owner + 1), Sess: s.Nodes[owner].Sess,
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
