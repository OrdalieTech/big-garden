package model

import "fmt"

// world steps one state of one case under one variant.
type world struct {
	c       *Case
	v       Variant
	s       *State
	viol    string   // first invariant violation
	blocked uint8    // nonzero: the transition exceeded this model bound and is discarded
	fin     [2]uint8 // kind and code of the operation that finished in this step
}

// coverage counts client outcomes by operation kind and code (random traces).
type coverage [kCollect + 1][cSkip + 1]uint64

// Model bounds. Messages in flight is a case parameter (PLAN §14); the others
// are capacities of the model's fixed arrays.
const (
	boundMsgs uint8 = iota + 1
	boundObjs
	boundDel
	boundSeen
	boundFailed
	boundOps
	numBounds
)

var boundNames = [numBounds]string{"", "messages_in_flight", "objects", "collect_batch", "seen_images", "failed_candidates", "ledger"}

func (w *world) bound(b uint8) {
	if w.blocked == 0 {
		w.blocked = b
	}
}

func (w *world) fail(f string, a ...any) {
	if w.viol == "" {
		w.viol = fmt.Sprintf(f, a...)
	}
}

func (w *world) fresh() ID {
	w.s.Fresh++
	if w.s.Fresh == 0 {
		panic("model: ID space exhausted")
	}
	return w.s.Fresh
}

// Process events.
const (
	evStart uint8 = iota
	evEnter
	evReply
	evTimeout
)

// Subroutine results (publish, resolve).
const (
	sCommitted uint8 = iota + 1
	sDead            // every proposal is dead and the candidate did not commit
	sConflict        // the operation ID committed with another bound hash
	sMissing         // head absent while the candidate has a base
	sUnknown         // deadline: outcome_unknown, candidate stays unresolved
)

// Outcome codes returned to the client.
const (
	cOK uint8 = iota + 1
	cReplayed
	cNotOwner
	cRouteChanged
	cSchemaMismatch
	cIDConflict
	cIDExpired
	cAbsent
	cUnknown
	cUnavailable
	cStateMissing
	cInvalid
	cCallback
	cSkip
)

var codeNames = [...]string{"", "ok", "replayed", "not_owner", "route_changed", "schema_mismatch",
	"id_conflict", "id_expired", "absent_at_version", "outcome_unknown", "unavailable",
	"state_missing", "invalid_argument", "callback_error", "skipped"}

var kindNames = [...]string{"", "create", "update", "migrate", "read", "lookup", "takeover",
	"release", "compact", "collect"}

// Program counters. Each operation is a straight sequence of storage requests;
// a PC names the reply a process is waiting for.
const (
	pcStart uint8 = iota
	pcResGet
	pcResCas
	pcResHyd
	pcPubCheck
	pcPubCas
	pcActObjs
	pcActReget
	pcUpdBegin
	pcUpdGot
	pcUpdTook
	pcUpdAdmit
	pcUpdBarrier
	pcUpdPut
	pcUpdDone
	pcRdBegin
	pcRdGot
	pcRdTook
	pcRdExec
	pcTkBegin
	pcTkGot
	pcTkPub
	pcTkDone
	pcRelBegin
	pcRelGot
	pcRelDone
	pcCmpGot
	pcCmpPut
	pcCmpDone
	pcColGot
	pcColList
	pcColDel
	pcCrBegin
	pcCrHead
	pcCrBirthX
	pcCrReplay
	pcCrBirth
	pcCrList
	pcCrPutBirth
	pcCrBirthCheck
	pcCrPutObj
	pcCrDone
)

// out is what an operation returns to its client, with the image it was
// computed from when it depends on entity state.
type out struct {
	code        uint8
	seq, req    int8
	img         Image
	hasImg, def bool // def: the operation's own candidate is definitely not committed
}

// px is one process in the context of its node and entity.
type px struct {
	*world
	n, e, l int
	p       *Proc
	m       *Mem
	r       Req
}

func (w *world) px(n, e, l int) px {
	p := &w.s.Procs[n][e][l]
	return px{world: w, n: n, e: e, l: l, p: p, m: &w.s.Mem[n][e], r: w.c.Reqs[p.Req]}
}

func (x *px) self() ID    { return x.s.Nodes[x.n].Sess }
func (x *px) clock() int8 { return x.s.Nodes[x.n].Clock }
func (x *px) mine(h Head) bool {
	return h.Owner == int8(x.n+1) && h.Sess == x.self()
}

// owned reports whether the node holds a published, owned, activated image.
func (x *px) owned() bool {
	m := x.m
	return m.HasH && m.HasImg && m.Img == m.H.Img && x.mine(m.H)
}

// discard drops the cached head and local image (D10).
func (x *px) discard() {
	x.m.HasH, x.m.HasImg, x.m.H, x.m.Img = false, false, Head{}, Image{}
}

func (x *px) resolved() { x.m.Unres, x.m.C = false, Cand{} }

func (x *px) send(m Msg) int8 {
	for i := 0; i < x.c.Msgs; i++ {
		if x.s.Msgs[i].Kind == 0 {
			m.Ent, m.Node, m.Lane = int8(x.e), int8(x.n), int8(x.l)
			m.Waited, m.Sess = true, x.self()
			x.s.Msgs[i] = m
			return int8(i + 1)
		}
	}
	x.bound(boundMsgs)
	return 0
}

func (x *px) wait(pc uint8, m Msg) { x.p.PC, x.p.W = pc, [2]int8{x.send(m), 0} }
func (x *px) getHead(pc uint8)     { x.p.Cur = Head{}; x.wait(pc, Msg{Kind: mGetHead}) }
func (x *px) enter(pc uint8)       { x.p.PC = pc; x.run(evEnter, 0) }
func (x *px) subret(res uint8)     { x.p.Res = res; x.ret() }

// ret returns from a subroutine; the return address is dead afterwards.
func (x *px) ret() {
	pc := x.p.Ret
	x.p.Ret = 0
	x.enter(pc)
}

func (x *px) obj(att ID) *Obj {
	en := &x.s.Ents[x.e]
	for i := int8(0); i < en.NObj; i++ {
		if en.Objs[i].Att == att {
			return &en.Objs[i]
		}
	}
	return nil
}

// receipt returns the ledger index of the stored receipt for an operation ID, or
// -1. This is the only way protocol code reads receipts.
func receipt(img *Image, reqs []Req, id int8) int8 {
	for i := int8(0); i < img.Seq; i++ {
		if img.Kept&(1<<i) != 0 && reqs[img.Ops[i]-1].ID == id {
			return i
		}
	}
	return -1
}

// fails is the application callback's deterministic, state-dependent rejection.
func fails(img *Image, r Req) bool { return r.Input == 2 && img.Seq%2 == 0 }

func refs(h *Head, att ID) bool {
	if h.Snap.Att == att {
		return true
	}
	for i := int8(0); i < h.ND; i++ {
		if h.Deltas[i].Att == att {
			return true
		}
	}
	return false
}

func isMutation(k uint8) bool { return k == kCreate || k == kUpdate || k == kMigrate }

// run advances a process on one event. Every branch either sends one storage
// request and waits, enters another PC, or finishes the operation.
func (x *px) run(ev, res uint8) {
	p := x.p
	switch p.PC {
	case pcStart:
		x.start()

	// §2 publish and resolve.
	case pcPubCheck: // control: owner check, then unconditional publish
		if res == rOK && x.mine(p.Cur) {
			m := x.cas(&x.m.C)
			m.Uncond = true
			x.wait(pcPubCas, m)
			return
		}
		x.dead(p.Cur)
	case pcPubCas, pcResCas:
		c := &x.m.C
		if ev == evReply && res == rOK {
			x.commit(c.Next, c.Next.Img.Seq)
			return
		}
		if p.PC == pcPubCas && ev == evReply && res == rPreFailed && x.v == HedgeNoResolve {
			x.discard()
			x.resolved()
			p.Code = cRouteChanged
			x.subret(sDead)
			return
		}
		x.getHead(pcResGet)
	case pcResGet:
		x.resolveGot(res)
	case pcResHyd:
		if res != rOK {
			x.getHead(pcResGet)
			return
		}
		cur, c := p.Cur, x.m.C
		x.m.HasH, x.m.H, x.m.HasImg, x.m.Img = true, cur, true, cur.Img
		i := receipt(&cur.Img, x.c.Reqs, x.c.Reqs[c.Req-1].ID)
		switch {
		case i >= 0 && cur.Img.Ops[i] == c.Req:
			x.commit(cur, i+1)
		case i >= 0:
			x.resolved()
			x.subret(sConflict)
		case x.c.Reqs[c.Req-1].TS <= cur.Img.Floor: // the receipt may have been pruned: unknown for good
			x.discard()
			x.resolved()
			x.p.Cur, x.p.Code = cur, cUnknown
			x.subret(sDead)
		default:
			x.dead(cur)
		}

	// §4 activation of exactly one head.
	case pcActObjs:
		if res == rOK {
			x.m.HasH, x.m.H, x.m.HasImg, x.m.Img = true, p.Cur, true, p.Cur.Img
			p.Cur = Head{}
			x.ret()
			return
		}
		p.Act = p.Cur.Nonce
		x.getHead(pcActReget)
	case pcActReget:
		if res != rOK || p.Cur.Nonce == p.Act {
			x.finish(out{code: cStateMissing})
			return
		}
		p.Act, p.Ret = 0, 0
		switch p.Kind { // the head changed (collection race): restart with it
		case kRead, kLookup:
			x.enter(pcRdGot)
		case kTakeover:
			x.enter(pcTkGot)
		default:
			x.enter(pcUpdGot)
		}

	// §6 Update and Migrate.
	case pcUpdBegin:
		if !x.settled() {
			return
		}
		if x.owned() {
			x.admit()
			return
		}
		x.getHead(pcUpdGot)
	case pcUpdGot:
		if ev == evReply && res == rNotFound {
			x.finish(out{code: cStateMissing})
			return
		}
		cur := p.Cur
		x.dropStale(cur)
		switch {
		case x.mine(cur):
			x.activate(cur, pcUpdAdmit)
		case cur.Owner == int8(x.n+1): // D4: an earlier session of this node
			x.takeover(cur, pcUpdTook)
		default:
			x.finish(out{code: cNotOwner, def: true})
		}
	case pcUpdTook:
		if p.Res == sCommitted {
			x.activate(p.Cur, pcUpdAdmit)
			return
		}
		x.subFail()
	case pcUpdAdmit:
		x.admit()
	case pcUpdBarrier:
		if res == rOK && p.Cur.Nonce == x.m.H.Nonce {
			x.finish(out{code: p.Pend, seq: p.PendSeq, img: x.m.Img, hasImg: true, def: true})
			return
		}
		if res == rOK && !p.Redo { // head changed: redo from activation once
			p.Redo = true
			x.enter(pcUpdGot)
			return
		}
		x.finish(out{code: cRouteChanged, def: true})
	case pcUpdPut:
		if ev == evTimeout {
			if p.Put < 1 {
				p.Put++
				x.wait(pcUpdPut, Msg{Kind: mPutObj, Obj: p.O})
				return
			}
			x.discard()
			x.finish(out{code: cUnavailable, def: true})
			return
		}
		x.publishUpdate(false)
	case pcUpdDone, pcCrDone:
		if p.Res == sCommitted {
			x.finish(out{code: cOK, seq: p.Seq})
			return
		}
		x.subFail()

	// §3 Read and Lookup.
	case pcRdBegin:
		if !x.settled() {
			return
		}
		if x.v == CachedRead && x.owned() {
			x.readExec()
			return
		}
		x.getHead(pcRdGot)
	case pcRdGot:
		if ev == evReply && res == rNotFound {
			x.finish(out{code: cStateMissing})
			return
		}
		cur := p.Cur
		x.dropStale(cur)
		switch {
		case x.mine(cur):
			x.activate(cur, pcRdExec)
		case cur.Owner == int8(x.n+1):
			x.takeover(cur, pcRdTook)
		default:
			x.finish(out{code: cNotOwner})
		}
	case pcRdTook:
		if p.Res == sCommitted {
			x.activate(p.Cur, pcRdExec)
			return
		}
		x.subFail()
	case pcRdExec:
		x.readExec()

	// §7 Takeover.
	case pcTkBegin:
		if !x.settled() {
			return
		}
		x.getHead(pcTkGot)
	case pcTkGot:
		if ev == evReply && res == rNotFound {
			x.finish(out{code: cStateMissing})
			return
		}
		cur := p.Cur
		if !p.Flag { // the first fresh GET is the observed Route
			p.Flag, p.ObsOwner, p.ObsSess, p.ObsEpoch = true, cur.Owner, cur.Sess, cur.Epoch
		}
		x.dropStale(cur)
		switch {
		case x.mine(cur):
			x.activate(cur, pcTkDone)
		case cur.Owner != p.ObsOwner || cur.Sess != p.ObsSess || cur.Epoch != p.ObsEpoch:
			x.finish(out{code: cRouteChanged, def: true})
		default:
			x.takeover(cur, pcTkPub)
		}
	case pcTkPub:
		switch p.Res {
		case sCommitted:
			x.activate(p.Cur, pcTkDone)
		case sDead:
			cur := p.Cur
			if cur.Owner == p.ObsOwner && cur.Sess == p.ObsSess && cur.Epoch == p.ObsEpoch && p.Tk < 2 {
				p.Tk++
				x.getHead(pcTkGot)
				return
			}
			x.finish(out{code: cRouteChanged, def: true})
		default:
			x.subFail()
		}
	case pcTkDone:
		x.finish(out{code: cOK})

	// §7 Release.
	case pcRelBegin:
		if !x.settled() {
			return
		}
		if x.m.HasH && x.mine(x.m.H) {
			x.release(x.m.H)
			return
		}
		x.getHead(pcRelGot)
	case pcRelGot:
		if res == rNotFound {
			x.finish(out{code: cStateMissing})
			return
		}
		if !x.mine(p.Cur) {
			x.finish(out{code: cOK})
			return
		}
		x.release(p.Cur)
	case pcRelDone:
		if p.Res == sCommitted {
			x.discard()
			x.finish(out{code: cOK})
			return
		}
		if p.Res == sDead {
			x.finish(out{code: cRouteChanged, def: true})
			return
		}
		x.subFail()

	// §9 Compaction at the exact head.
	case pcCmpGot:
		if res != rOK {
			x.finish(out{code: cSkip})
			return
		}
		h, i := x.m.H, int(p.Loops)
		first, last := x.obj(h.Deltas[i].Att), x.obj(h.Deltas[i+fanout-1].Att)
		p.O = Obj{Att: x.fresh(), Inc: h.Inc, Base: h.Rev, Min: first.Min, Max: last.Max,
			Level: h.Deltas[i].Level + 1, Stamp: x.clock(), Present: true, Pre: first.Pre, Post: last.Post}
		x.wait(pcCmpPut, Msg{Kind: mPutObj, Obj: p.O})
	case pcCmpPut:
		if ev == evTimeout {
			x.finish(out{code: cSkip})
			return
		}
		h := x.m.H
		next := h
		next.Rev++
		next.Nonce = x.fresh()
		replaceRun(&next, int(p.Loops), ref(p.O))
		x.publish(Cand{Kind: kCompact, Att: p.O.Att, Base: h, Next: next}, pcCmpDone)
	case pcCmpDone:
		switch {
		case p.Res == sCommitted:
			x.finish(out{code: cOK})
		case p.Res == sDead && x.v == RetargetCompaction && p.Tk == 0:
			x.retarget()
		case p.Res == sDead:
			x.finish(out{code: cSkip, def: true})
		default:
			x.subFail()
		}

	// §10 Collect.
	case pcColGot:
		if res != rOK {
			x.finish(out{code: cOK})
			return
		}
		x.wait(pcColList, Msg{Kind: mList, List: listCollect, Head: p.Cur})
	case pcColList, pcColDel:
		if p.NDel == 0 {
			x.finish(out{code: cOK})
			return
		}
		p.NDel--
		att := p.Del[p.NDel]
		p.Del[p.NDel] = 0
		x.wait(pcColDel, Msg{Kind: mDel, Obj: Obj{Att: att}})

	// §5 Create.
	case pcCrBegin:
		if x.settled() {
			x.getHead(pcCrHead)
		}
	case pcCrHead:
		if res == rOK {
			x.wait(pcCrBirthX, Msg{Kind: mGetBirth})
			return
		}
		x.wait(pcCrBirth, Msg{Kind: mGetBirth})
	case pcCrBirthX:
		b := p.RBirth
		switch {
		case res != rOK:
			x.finish(out{code: cStateMissing})
		case b.Req == p.Req+1:
			x.wait(pcCrReplay, Msg{Kind: mGetObjs, Head: p.Cur})
		case x.c.Reqs[b.Req-1].ID == x.r.ID:
			x.finish(out{code: cIDConflict, def: true})
		default:
			x.finish(out{code: cInvalid, def: true})
		}
	case pcCrReplay:
		if res != rOK {
			x.finish(out{code: cUnavailable})
			return
		}
		img := p.Cur.Img
		i := receipt(&img, x.c.Reqs, x.r.ID)
		switch {
		case i >= 0 && img.Ops[i] == p.Req+1:
			x.finish(out{code: cReplayed, seq: i + 1, img: img, hasImg: true})
		case i >= 0:
			x.finish(out{code: cIDConflict, img: img, hasImg: true, def: true})
		case x.r.TS <= img.Floor:
			x.finish(out{code: cIDExpired, img: img, hasImg: true, def: true})
		default:
			x.finish(out{code: cAbsent, img: img, hasImg: true, def: true})
		}
	case pcCrBirth:
		x.crBirth(res == rOK, p.RBirth)
	case pcCrList:
		if p.Flag {
			x.finish(out{code: cStateMissing, def: true})
			return
		}
		if !p.Resume {
			p.Inc = x.fresh()
		}
		img := Image{Seq: 1, Schema: x.r.Schema, Kept: 1}
		img.Ops[0] = p.Req + 1
		x.discard()
		x.m.HasImg, x.m.Img = true, img
		if p.Resume {
			x.crUpload()
			return
		}
		p.BAtt = x.fresh()
		x.wait(pcCrPutBirth, x.birthMsg())
	case pcCrPutBirth:
		switch {
		case ev == evTimeout && p.Put < 1:
			p.Put++
			x.wait(pcCrPutBirth, x.birthMsg())
		case ev == evTimeout: // birth.json may exist: only this request can resume it (D5)
			x.discard()
			x.finish(out{code: cUnknown})
		case res == rOK:
			x.crUpload()
		default:
			x.wait(pcCrBirthCheck, Msg{Kind: mGetBirth})
		}
	case pcCrBirthCheck:
		b := p.RBirth
		switch {
		case res == rOK && b.Att == p.BAtt:
			x.crUpload()
		case res == rOK && b.Req == p.Req+1 && b.Inc == p.Inc:
			x.crUpload()
		case res == rOK && b.Req == p.Req+1 && p.Loops < 1: // restart at step 3 with its incarnation
			p.Loops++
			x.discard()
			x.crBirth(true, b)
		default:
			x.discard()
			x.finish(out{code: cStateMissing, def: true})
		}
	case pcCrPutObj:
		if ev == evTimeout {
			if p.Put < 1 {
				p.Put++
				x.wait(pcCrPutObj, Msg{Kind: mPutObj, Obj: p.O})
				return
			}
			x.discard()
			x.finish(out{code: cUnknown}) // after the birth, as above
			return
		}
		next := Head{Inc: p.Inc, Rev: 1, Nonce: x.fresh(), Owner: int8(x.n + 1), Sess: x.self(),
			Epoch: 1, Img: x.m.Img, Snap: ref(p.O)}
		x.publish(Cand{Kind: kCreate, Genesis: true, Req: p.Req + 1, Att: p.O.Att, Next: next}, pcCrDone)
	default:
		panic(fmt.Sprintf("model: bad pc %d", p.PC))
	}
}

func (x *px) start() {
	p, r := x.p, x.r
	switch r.Kind {
	case kUpdate, kMigrate:
		if r.TS <= x.clock()-x.c.Retention { // wall-clock admission, no state involved
			x.finish(out{code: cIDExpired, def: true})
			return
		}
		x.settle(pcUpdBegin)
	case kRead, kLookup:
		x.settle(pcRdBegin)
	case kTakeover:
		x.settle(pcTkBegin)
	case kRelease:
		x.settle(pcRelBegin)
	case kCompact: // maintenance: owner, under the gate, nothing unresolved
		i := -1
		if !x.m.Unres && x.owned() {
			i = oldestRun(&x.m.H)
		}
		if i < 0 {
			x.finish(out{code: cSkip})
			return
		}
		h := x.m.H
		q := Head{Inc: h.Inc, ND: fanout}
		for k := 0; k < fanout; k++ {
			q.Deltas[k] = h.Deltas[i+k]
			p.Del[k] = h.Deltas[i+k].Att
		}
		p.Loops = int8(i)
		x.wait(pcCmpGot, Msg{Kind: mGetObjs, Head: q})
	case kCollect:
		x.getHead(pcColGot)
	case kCreate:
		if r.TS <= x.clock()-x.c.Retention {
			x.finish(out{code: cIDExpired, def: true})
			return
		}
		x.settle(pcCrBegin) // §2 rule 5: an earlier unresolved candidate first
	}
}

// settle resolves a candidate left unresolved by an earlier operation.
func (x *px) settle(next uint8) {
	x.p.Res = 0
	if x.m.Unres {
		x.p.Ret = next
		x.getHead(pcResGet)
		return
	}
	x.enter(next)
}

// settled finishes the operation when settling did not reach a decision.
func (x *px) settled() bool {
	switch x.p.Res {
	case sUnknown:
		code := cUnavailable
		if x.m.C.Req == x.p.Req+1 {
			code = cUnknown
		}
		x.finish(out{code: code})
		return false
	case sMissing:
		x.finish(out{code: cStateMissing})
		return false
	}
	x.p.Res = 0
	return true
}

// subFail finishes an operation whose publication did not commit.
func (x *px) subFail() {
	switch x.p.Res {
	case sDead:
		x.finish(out{code: x.p.Code, def: x.p.Code != cUnknown})
	case sConflict:
		x.finish(out{code: cIDConflict, img: x.p.Cur.Img, hasImg: true, def: true})
	case sUnknown:
		x.finish(out{code: cUnknown})
	default:
		x.finish(out{code: cStateMissing})
	}
}

// dropStale discards a local image that differs from the head just read.
func (x *px) dropStale(cur Head) {
	if x.m.HasImg && x.m.Img != cur.Img {
		x.discard()
	}
}

func (x *px) activate(h Head, ret uint8) {
	x.p.Res, x.p.Code, x.p.Pend, x.p.PendSeq = 0, 0, 0, 0 // read by the caller already
	if x.m.HasImg && x.m.Img == h.Img {
		x.m.HasH, x.m.H = true, h
		x.p.Cur = Head{}
		x.enter(ret)
		return
	}
	x.discard()
	x.p.Cur, x.p.Ret = h, ret
	x.wait(pcActObjs, Msg{Kind: mGetObjs, Head: h})
}

func (x *px) cas(c *Cand) Msg {
	m := Msg{Kind: mCAS, Head: c.Next, Uncond: x.v == UncondHead}
	if !c.Genesis {
		m.Cond = c.Base.Nonce
	}
	return m
}

// publish is PROTOCOL §2: mark the candidate unresolved, then one CAS.
func (x *px) publish(c Cand, ret uint8) {
	c.Next.Cand = x.fresh()
	c.Nonces, c.NN = [3]ID{c.Next.Nonce}, 1
	x.m.Unres, x.m.C = true, c
	if c.Kind == x.p.Kind {
		x.p.CID, x.p.Applied = c.Next.Cand, false
	}
	x.p.Hedged, x.p.Ret, x.p.Cur = false, ret, Head{}
	if c.Kind != kCompact { // the uploaded object is referenced from the candidate now
		x.p.O = Obj{}
	}
	if x.v == CheckThenPublish && !c.Genesis && c.Kind != kTakeover {
		x.getHead(pcPubCheck)
		return
	}
	x.wait(pcPubCas, x.cas(&c))
}

// resolveGot is PROTOCOL §2 resolve steps 1-4 on a fresh head read.
func (x *px) resolveGot(res uint8) {
	c := &x.m.C
	if res == rNotFound {
		if !c.Genesis {
			x.subret(sMissing)
			return
		}
		x.repropose()
		return
	}
	cur := x.p.Cur
	for i := int8(0); i < c.NN; i++ {
		if c.Nonces[i] == cur.Nonce {
			x.commit(cur, c.Next.Img.Seq)
			return
		}
	}
	if !c.Genesis && cur.Rev <= c.Base.Rev {
		x.repropose()
		return
	}
	switch { // every proposal is dead
	case c.Att != 0 && refs(&cur, c.Att):
		x.commit(cur, c.Next.Img.Seq)
	case c.Req != 0:
		x.wait(pcResHyd, Msg{Kind: mGetObjs, Head: cur})
	case c.Kind == kTakeover && x.mine(cur):
		x.commit(cur, 0)
	case c.Kind == kRelease && !x.mine(cur):
		x.commit(cur, 0)
	default:
		x.dead(cur)
	}
}

// repropose is the D12 fence: same head, fresh nonce, same condition.
func (x *px) repropose() {
	c := &x.m.C
	if c.NN < 3 {
		c.Next.Nonce = x.fresh()
		c.Nonces[c.NN] = c.Next.Nonce
		c.NN++
		x.p.Hedged = false
		x.wait(pcResCas, x.cas(c))
		return
	}
	x.getHead(pcResGet)
}

func (x *px) commit(cur Head, seq int8) {
	x.resolved()
	x.p.Cur, x.p.Seq = cur, seq
	if x.m.HasImg && x.m.Img == cur.Img {
		x.m.HasH, x.m.H = true, cur
	} else {
		x.discard()
	}
	x.subret(sCommitted)
}

func (x *px) dead(cur Head) {
	x.discard()
	x.resolved()
	x.p.Cur, x.p.Code = cur, cRouteChanged
	if !x.mine(cur) {
		x.p.Code = cNotOwner
	}
	x.subret(sDead)
}

func (x *px) takeover(cur Head, ret uint8) {
	next := cur
	next.Owner, next.Sess = int8(x.n+1), x.self()
	next.Epoch++
	next.Rev++
	next.Nonce = x.fresh()
	x.publish(Cand{Kind: kTakeover, Base: cur, Next: next}, ret)
}

func (x *px) release(base Head) {
	next := base
	next.Owner, next.Sess = 0, 0
	next.Epoch++
	next.Rev++
	next.Nonce = x.fresh()
	x.publish(Cand{Kind: kRelease, Base: base, Next: next}, pcRelDone)
}

// admit is PROTOCOL §6 steps 3-9 on the owned, activated image.
func (x *px) admit() {
	p, r := x.p, x.r
	img := x.m.Img
	var code uint8
	var seq int8
	if r.TS <= img.Floor {
		code = cIDExpired
	} else if i := receipt(&img, x.c.Reqs, r.ID); i >= 0 {
		code, seq = cIDConflict, 0
		if img.Ops[i] == p.Req+1 {
			code, seq = cReplayed, i+1
		}
	} else if img.Schema != r.Schema {
		code = cSchemaMismatch
	} else if fails(&img, r) {
		code = cCallback
	}
	if code != 0 { // state-dependent answer: barrier first
		p.Pend, p.PendSeq = code, seq
		x.getHead(pcUpdBarrier)
		return
	}
	if img.Seq == maxOps {
		x.bound(boundOps)
		return
	}
	ni := img
	bound := x.clock() - x.c.Retention
	if x.v != FloorlessPrune {
		if bound > ni.Floor {
			ni.Floor = bound
		}
		bound = ni.Floor
	}
	for i := int8(0); i < ni.Seq; i++ { // bounded pruning: at most one receipt
		if ni.Kept&(1<<i) != 0 && x.c.Reqs[ni.Ops[i]-1].TS <= bound {
			ni.Kept &^= 1 << i
			break
		}
	}
	ni.Ops[ni.Seq] = p.Req + 1
	ni.Kept |= 1 << ni.Seq
	ni.Seq++
	if r.Kind == kMigrate {
		ni.Schema = r.To
	}
	x.m.Img = ni
	if x.v == PrematureAck {
		x.report(out{code: cOK, seq: ni.Seq})
		p.Acked = true
	}
	h := x.m.H
	t := tailOf(&h, &ni)
	switch {
	case h.ND == maxDeltas: // cap: publish a snapshot instead
		p.O = Obj{Snap: true, Min: 1, Max: ni.Seq, Post: ni}
	case t.Max-t.Min < tailCap && (hasBaseRef(&h) || x.v == TailWithoutObject):
		x.publishUpdate(true) // D16: single CAS carrying the inline tail
		return
	default: // upload tail+delta as one ordinary delta
		p.O = Obj{Min: t.Min, Max: t.Max, Pre: t.Pre, Post: t.Post}
	}
	p.O.Att, p.O.Inc, p.O.Base, p.O.Stamp, p.O.Present = x.fresh(), h.Inc, h.Rev, x.clock(), true
	p.Put = 0
	x.wait(pcUpdPut, Msg{Kind: mPutObj, Obj: p.O})
}

// tailOf is compact(H.tail, delta) for the mutation producing ni (D16).
func tailOf(h *Head, ni *Image) Tail {
	t := Tail{Min: ni.Seq, Max: ni.Seq, Pre: h.Img, Post: *ni}
	if h.Tail.Max != 0 {
		t.Min, t.Pre = h.Tail.Min, h.Tail.Pre
	}
	return t
}

func hasBaseRef(h *Head) bool {
	for _, r := range headRefs(h) {
		if r.Base >= 1 {
			return true
		}
	}
	return false
}

// publishUpdate is §6 step 9: the next head with the inline tail, or with the
// uploaded delta appended (or the cap snapshot) and no tail.
func (x *px) publishUpdate(inline bool) {
	p := x.p
	h := x.m.H
	next := h
	next.Rev++
	next.Nonce = x.fresh()
	next.Img = x.m.Img
	c := Cand{Kind: p.Kind, Req: p.Req + 1, Base: h}
	switch {
	case inline:
		next.Tail = tailOf(&h, &x.m.Img)
	case p.O.Snap:
		next.Snap, next.ND, next.Deltas, next.Tail = ref(p.O), 0, [maxDeltas]Ref{}, Tail{}
		c.Att = p.O.Att
	default:
		next.Deltas[next.ND] = ref(p.O)
		next.ND++
		next.Tail, c.Att = Tail{}, p.O.Att
	}
	c.Next = next
	x.publish(c, pcUpdDone)
}

func (x *px) readExec() {
	img, r := x.m.Img, x.r
	if r.Kind == kLookup {
		i := receipt(&img, x.c.Reqs, r.ID)
		switch {
		case i >= 0:
			x.finish(out{code: cReplayed, seq: i + 1, req: img.Ops[i] - 1, img: img, hasImg: true})
		case r.TS <= img.Floor:
			x.finish(out{code: cIDExpired, img: img, hasImg: true})
		default:
			x.finish(out{code: cAbsent, img: img, hasImg: true})
		}
		return
	}
	switch {
	case img.Schema != r.Schema:
		x.finish(out{code: cSchemaMismatch, img: img, hasImg: true})
	case fails(&img, r):
		x.finish(out{code: cCallback, img: img, hasImg: true})
	default:
		x.finish(out{code: cOK, seq: img.Seq, img: img, hasImg: true})
	}
}

func oldestRun(h *Head) int {
	for i := 0; i+fanout <= int(h.ND); i++ {
		ok := true
		for k := 1; k < fanout; k++ {
			ok = ok && h.Deltas[i+k].Level == h.Deltas[i].Level
		}
		if ok {
			return i
		}
	}
	return -1
}

func replaceRun(h *Head, i int, r Ref) {
	var d [maxDeltas]Ref
	n := copy(d[:], h.Deltas[:i])
	d[n] = r
	n++
	n += copy(d[n:], h.Deltas[i+fanout:h.ND])
	h.Deltas, h.ND = d, int8(n)
}

// retarget is the broken control: reuse a dead compaction output on a later head.
func (x *px) retarget() {
	p := x.p
	cur := p.Cur
	for j := 0; j+fanout <= int(cur.ND); j++ {
		same := true
		for k := 0; k < fanout; k++ {
			same = same && cur.Deltas[j+k].Att == p.Del[k]
		}
		if same {
			next := cur
			next.Rev++
			next.Nonce = x.fresh()
			replaceRun(&next, j, ref(p.O))
			p.Tk = 1
			x.publish(Cand{Kind: kCompact, Att: p.O.Att, Base: cur, Next: next}, pcCmpDone)
			return
		}
	}
	x.finish(out{code: cSkip, def: true})
}

func (x *px) crBirth(present bool, b Birth) {
	p := x.p
	switch {
	case !present:
		p.Resume = false
		x.wait(pcCrList, Msg{Kind: mList, List: listFresh})
	case b.Req == p.Req+1 || x.v == D5NoIdentity: // D5 resume
		p.Resume, p.Inc = true, b.Inc
		x.wait(pcCrList, Msg{Kind: mList, List: listResume, Head: Head{Inc: b.Inc}})
	case x.c.Reqs[b.Req-1].ID == x.r.ID:
		x.finish(out{code: cIDConflict, def: true})
	default:
		x.finish(out{code: cStateMissing, def: true})
	}
}

func (x *px) birthMsg() Msg {
	return Msg{Kind: mPutBirth, Birth: Birth{Present: true, Req: x.p.Req + 1, Inc: x.p.Inc, Att: x.p.BAtt}}
}

func (x *px) crUpload() {
	p := x.p
	p.O = Obj{Att: x.fresh(), Inc: p.Inc, Min: 1, Max: 1, Snap: true, Present: true, Post: x.m.Img, Stamp: x.clock()}
	p.Put = 0
	x.wait(pcCrPutObj, Msg{Kind: mPutObj, Obj: p.O})
}
