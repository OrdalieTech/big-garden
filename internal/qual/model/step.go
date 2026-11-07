package model

import "fmt"

// Tr is one transition. Every nondeterministic choice of the environment and the
// clients is a transition: invoking a request on a node, a storage request
// applying, its reply being delivered, a client giving up on a write (the
// request stays in flight and may still apply), a lost request, a hedge, a
// commit deadline, a crash with restart, a clock change and a head deletion.
type Tr struct{ K, A, B uint8 }

const (
	tInvoke   uint8 = iota + 1 // A request, B node
	tApply                     // A message slot
	tDeliver                   // A message slot
	tDrop                      // A orphaned unapplied message slot: lost for ever
	tTimeout                   // A message slot: the client gives up on this write
	tHedge                     // A node, B entity*2+lane: identical duplicate CAS (D3)
	tDeadline                  // A node, B entity*2+lane: CommitTimeout, outcome_unknown
	tRestart                   // A node: crash, cache loss, new session
	tClock                     // A node, B value
	tLoss                      // A entity: administrative deletion of head.json
)

func deadlinePC(pc uint8) bool {
	switch pc {
	case pcPubCheck, pcPubCas, pcResGet, pcResCas, pcResHyd:
		return true
	}
	return false
}

// reportsState: the operation can return an answer computed from entity state,
// so it keeps the ghost set of images that were the head during the call.
func reportsState(k uint8) bool {
	return k != kTakeover && k != kRelease && k != kCompact && k != kCollect
}

func lane(k uint8) int {
	if k == kCollect {
		return 1
	}
	return 0
}

// Enabled appends every enabled transition of s.
func Enabled(c *Case, s *State, buf []Tr) []Tr {
	active := 0
	for n := 0; n < c.Nodes; n++ {
		for e := 0; e < c.Ents; e++ {
			for l := 0; l < 2; l++ {
				if s.Procs[n][e][l].Kind != 0 {
					active++
				}
			}
		}
	}
	if active < c.Active {
		for r, q := range c.Reqs {
			if s.Used[r] >= q.Tries {
				continue
			}
			for n := 0; n < c.Nodes; n++ {
				if q.Nodes&(1<<n) != 0 && s.Procs[n][q.Ent][lane(q.Kind)].Kind == 0 {
					buf = append(buf, Tr{tInvoke, uint8(r), uint8(n)})
				}
			}
		}
	}
	for i := 0; i < c.Msgs; i++ {
		m := &s.Msgs[i]
		switch {
		case m.Kind == 0:
		case !m.Applied:
			buf = append(buf, Tr{tApply, uint8(i), 0})
			if !m.Waited {
				buf = append(buf, Tr{tDrop, uint8(i), 0})
			}
		case m.Waited:
			buf = append(buf, Tr{tDeliver, uint8(i), 0})
		}
		if m.Kind >= mPutBirth && m.Waited {
			buf = append(buf, Tr{tTimeout, uint8(i), 0})
		}
	}
	for n := 0; n < c.Nodes; n++ {
		for e := 0; e < c.Ents; e++ {
			for l := 0; l < 2; l++ {
				p := &s.Procs[n][e][l]
				if p.Kind == 0 || p.W[0] == 0 {
					continue
				}
				b := uint8(e*2 + l)
				if (p.PC == pcPubCas || p.PC == pcResCas) && p.W[1] == 0 && !p.Hedged && int(s.Hedges) < c.Hedges {
					buf = append(buf, Tr{tHedge, uint8(n), b})
				}
				if deadlinePC(p.PC) {
					buf = append(buf, Tr{tDeadline, uint8(n), b})
				}
			}
		}
	}
	for n := 0; n < c.Nodes; n++ {
		if int(s.Restarts) < c.Restarts {
			buf = append(buf, Tr{tRestart, uint8(n), 0})
		}
		if int(s.Jumps) < c.Jumps {
			for v := int8(0); v <= c.ClockMax; v++ {
				if v != s.Nodes[n].Clock {
					buf = append(buf, Tr{tClock, uint8(n), uint8(v)})
				}
			}
		}
	}
	for e := 0; e < c.Ents; e++ {
		if int(s.Losses) < c.Losses && s.Ents[e].HasHead {
			buf = append(buf, Tr{tLoss, uint8(e), 0})
		}
	}
	return buf
}

// Step applies t to s in place. ok is false when the transition exceeds a model
// bound (message slots, objects, ghost capacity): the caller discards s. viol is
// the first invariant violation, if any.
func Step(c *Case, v Variant, s *State, t Tr) (ok bool, viol string) {
	w := &world{c: c, v: v, s: s}
	switch t.K {
	case tInvoke:
		q := c.Reqs[t.A]
		n, e, l := int(t.B), int(q.Ent), lane(q.Kind)
		p := &s.Procs[n][e][l]
		*p = Proc{Kind: q.Kind, Req: int8(t.A)}
		s.Used[t.A]++
		if s.Ents[e].HasHead && reportsState(q.Kind) {
			p.Seen[0], p.NSeen = s.Ents[e].Head.Img, 1
		}
		x := w.px(n, e, l)
		x.run(evStart, 0)
	case tApply:
		m := &s.Msgs[t.A]
		waited := m.Waited
		w.apply(int(t.A))
		if waited && !c.Split && !w.blocked {
			w.deliver(int(t.A))
		}
	case tDeliver:
		w.deliver(int(t.A))
	case tDrop:
		s.Msgs[t.A] = Msg{}
	case tTimeout:
		m := &s.Msgs[t.A]
		x := w.px(int(m.Node), int(m.Ent), int(m.Lane))
		w.orphan(int(t.A))
		switch slot := int8(t.A) + 1; {
		case x.p.W[1] == slot:
			x.p.W[1] = 0
		case x.p.W[1] != 0: // still waiting on the hedged twin
			x.p.W = [2]int8{x.p.W[1], 0}
		default:
			x.p.W = [2]int8{}
			x.run(evTimeout, 0)
		}
	case tDeadline:
		x := w.px(int(t.A), int(t.B)/2, int(t.B)%2)
		for _, sl := range x.p.W {
			if sl != 0 {
				w.orphan(int(sl - 1))
			}
		}
		x.p.W = [2]int8{}
		x.subret(sUnknown)
	case tHedge:
		x := w.px(int(t.A), int(t.B)/2, int(t.B)%2)
		m := s.Msgs[x.p.W[0]-1]
		m.Applied, m.Res = false, 0
		x.p.Hedged = true
		x.p.W[1] = x.send(m)
		s.Hedges++
	case tRestart:
		n := int(t.A)
		for e := range s.Procs[n] {
			s.Procs[n][e] = [2]Proc{}
			s.Mem[n][e] = Mem{}
		}
		for i := range s.Msgs {
			if s.Msgs[i].Kind != 0 && int(s.Msgs[i].Node) == n {
				w.orphan(i)
			}
		}
		s.Nodes[n].Sess = w.fresh()
		s.Restarts++
	case tClock:
		s.Nodes[t.A].Clock = int8(t.B)
		s.Jumps++
	case tLoss:
		en, g := &s.Ents[t.A], &s.Ghost[t.A]
		g.Lost, g.LostImg = true, en.Head.Img
		en.HasHead, en.Head = false, Head{}
		s.Losses++
	}
	if w.blocked {
		return false, ""
	}
	if w.viol == "" {
		w.checkState()
	}
	return true, w.viol
}

// deliver hands message i's reply to its waiting process; a hedged twin still
// in flight becomes an orphan.
func (w *world) deliver(i int) {
	m := w.s.Msgs[i]
	w.s.Msgs[i] = Msg{}
	x := w.px(int(m.Node), int(m.Ent), int(m.Lane))
	for _, sl := range x.p.W {
		if sl != 0 && sl != int8(i)+1 {
			w.orphan(int(sl - 1))
		}
	}
	x.p.W = [2]int8{}
	x.run(evReply, m.Res)
}

// Describe renders t in s (before it is taken) for counterexample traces.
func Describe(c *Case, s *State, t Tr) string {
	switch t.K {
	case tInvoke:
		q := c.Reqs[t.A]
		d := fmt.Sprintf("client invokes r%d %s on n%d (entity %d", t.A, kindNames[q.Kind], t.B, q.Ent)
		if q.ID != 0 {
			d += fmt.Sprintf(", id %d hash %d ts %d", q.ID, q.Hash, q.TS)
		}
		return d + ")"
	case tApply, tDeliver, tDrop:
		m := &s.Msgs[t.A]
		verb := map[uint8]string{tApply: "store applies", tDeliver: "reply delivered for", tDrop: "store loses"}[t.K]
		d := fmt.Sprintf("%s m%d %s from n%d", verb, t.A, msgString(m), m.Node)
		if t.K == tDeliver {
			d += " -> " + [...]string{"", "OK", "NotFound", "PreconditionFailed"}[m.Res]
		}
		if t.K == tApply && m.Waited && !c.Split {
			d += " (reply delivered)"
		}
		return d
	case tTimeout:
		m := &s.Msgs[t.A]
		return fmt.Sprintf("n%d gives up on m%d %s (the request stays in flight)", m.Node, t.A, msgString(m))
	case tHedge:
		return fmt.Sprintf("n%d entity %d sends an identical hedge of its CAS", t.A, t.B/2)
	case tDeadline:
		return fmt.Sprintf("n%d entity %d hits CommitTimeout", t.A, t.B/2)
	case tRestart:
		return fmt.Sprintf("n%d crashes and restarts with a new session and an empty cache", t.A)
	case tClock:
		return fmt.Sprintf("n%d clock jumps to %d", t.A, t.B)
	case tLoss:
		return fmt.Sprintf("an administrator deletes head.json of entity %d", t.A)
	}
	return "?"
}

func msgString(m *Msg) string {
	switch m.Kind {
	case mGetHead:
		return "GET head"
	case mGetBirth:
		return "GET birth"
	case mGetObjs:
		return fmt.Sprintf("GET objects of head rev %d", m.Head.Rev)
	case mList:
		return "LIST"
	case mPutBirth:
		return fmt.Sprintf("PUT birth (r%d)", m.Birth.Req-1)
	case mPutObj:
		return fmt.Sprintf("PUT object #%d base %d seq %d..%d", m.Obj.Att, m.Obj.Base, m.Obj.Min, m.Obj.Max)
	case mCAS:
		cond := fmt.Sprintf("If-Match #%d", m.Cond)
		if m.Uncond {
			cond = "unconditional"
		} else if m.Cond == 0 {
			cond = "If-None-Match *"
		}
		owner := "none"
		if m.Head.Owner != 0 {
			owner = fmt.Sprintf("n%d", m.Head.Owner-1)
		}
		return fmt.Sprintf("CAS head rev %d nonce #%d owner %s epoch %d %s, %s", m.Head.Rev, m.Head.Nonce, owner, m.Head.Epoch, imgString(&m.Head.Img), cond)
	case mDel:
		return fmt.Sprintf("DELETE object #%d", m.Obj.Att)
	}
	return "?"
}
