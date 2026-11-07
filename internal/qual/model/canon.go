package model

import (
	"encoding/binary"
	"hash/maphash"
)

// canon builds a canonical encoding of a state: fresh IDs are renamed in order
// of first appearance, messages and objects are sorted by their ID-free fields.
// Two states with equal encodings are isomorphic, so deduplicating on it is
// sound; an imperfect canonical form only costs duplicate exploration.
type canon struct {
	b     []byte
	gen   []uint32
	ren   []ID
	g     uint32
	next  ID
	pos   [maxMsgs]int8
	seeds [2]maphash.Seed
}

func newCanon() *canon {
	return &canon{gen: make([]uint32, 1<<16), ren: make([]ID, 1<<16),
		seeds: [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}}
}

func (k *canon) u8(v uint8)  { k.b = append(k.b, v) }
func (k *canon) i8(v int8)   { k.b = append(k.b, uint8(v)) }
func (k *canon) i16(v int16) { k.b = binary.LittleEndian.AppendUint16(k.b, uint16(v)) }
func (k *canon) bit(v bool) {
	if v {
		k.u8(1)
	} else {
		k.u8(0)
	}
}

func (k *canon) id(x ID) {
	if x != 0 && k.gen[x] != k.g {
		k.gen[x], k.next = k.g, k.next+1
		k.ren[x] = k.next
	}
	if x == 0 {
		k.i16(0)
	} else {
		k.i16(int16(k.ren[x]))
	}
}

func (k *canon) img(m *Image) {
	k.b = append(k.b, uint8(m.Seq), uint8(m.Schema), uint8(m.Floor), m.Kept)
	for _, o := range m.Ops {
		k.i8(o)
	}
}

func (k *canon) ref(r *Ref) {
	k.id(r.Att)
	k.i16(r.Base)
	k.b = append(k.b, uint8(r.Min), uint8(r.Max), uint8(r.Level))
}

func (k *canon) head(h *Head) {
	k.id(h.Inc)
	k.id(h.Nonce)
	k.id(h.Sess)
	k.id(h.Cand)
	k.i16(h.Rev)
	k.i16(h.Epoch)
	k.i8(h.Owner)
	k.i8(h.ND)
	k.img(&h.Img)
	k.ref(&h.Snap)
	for i := range h.Deltas {
		k.ref(&h.Deltas[i])
	}
}

func (k *canon) obj(o *Obj) {
	k.id(o.Att)
	k.id(o.Inc)
	k.i16(o.Base)
	k.b = append(k.b, uint8(o.Min), uint8(o.Max), uint8(o.Level), uint8(o.Stamp))
	k.bit(o.Snap)
	k.bit(o.Present)
	k.img(&o.Pre)
	k.img(&o.Post)
}

func (k *canon) birth(b *Birth) {
	k.bit(b.Present)
	k.i8(b.Req)
	k.id(b.Inc)
	k.id(b.Att)
}

func (k *canon) cand(c *Cand) {
	k.u8(c.Kind)
	k.bit(c.Genesis)
	k.i8(c.Req)
	k.i8(c.NN)
	k.id(c.Att)
	for _, n := range c.Nonces {
		k.id(n)
	}
	k.head(&c.Base)
	k.head(&c.Next)
}

func (k *canon) proc(p *Proc) {
	if p.Kind == 0 {
		k.u8(0)
		return
	}
	k.b = append(k.b, p.Kind, p.PC, p.Ret, p.Res, p.Code, uint8(p.Req))
	for _, w := range p.W {
		if w == 0 {
			k.i8(-1)
		} else {
			k.i8(k.pos[w-1])
		}
	}
	for _, f := range []bool{p.Hedged, p.Redo, p.Applied, p.Acked, p.Resume, p.Flag, p.CurOK} {
		k.bit(f)
	}
	k.b = append(k.b, uint8(p.Put), uint8(p.Tk), uint8(p.Loops), uint8(p.Seq), uint8(p.Pend), uint8(p.PendSeq), uint8(p.ObsOwner))
	k.id(p.Act)
	k.id(p.CID)
	k.id(p.BAtt)
	k.id(p.Inc)
	k.id(p.ObsSess)
	k.i16(p.ObsEpoch)
	k.head(&p.Cur)
	k.birth(&p.RBirth)
	k.obj(&p.O)
	k.i8(p.NDel)
	for _, a := range p.Del {
		k.id(a)
	}
	// Seen is a set: encode it sorted.
	ss := p.Seen
	n := int(p.NSeen)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && imgLess(&ss[j], &ss[j-1]); j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
	k.i8(p.NSeen)
	for i := 0; i < n; i++ {
		k.img(&ss[i])
	}
}

func imgLess(a, b *Image) bool {
	if a.Seq != b.Seq {
		return a.Seq < b.Seq
	}
	if a.Ops != b.Ops {
		for i := range a.Ops {
			if a.Ops[i] != b.Ops[i] {
				return a.Ops[i] < b.Ops[i]
			}
		}
	}
	if a.Schema != b.Schema {
		return a.Schema < b.Schema
	}
	if a.Floor != b.Floor {
		return a.Floor < b.Floor
	}
	return a.Kept < b.Kept
}

// order sorts idx by key (insertion sort; ties keep slot order).
func order(idx []int, key func(int) uint64) {
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && key(idx[j]) < key(idx[j-1]); j-- {
			idx[j], idx[j-1] = idx[j-1], idx[j]
		}
	}
}

func (k *canon) msg(m *Msg) {
	if m.Kind == 0 {
		k.u8(0)
		return
	}
	k.b = append(k.b, m.Kind, uint8(m.Ent), uint8(m.Node), uint8(m.Lane), m.Res, m.List)
	k.bit(m.Waited)
	k.bit(m.Applied)
	k.bit(m.Uncond)
	switch m.Kind { // only the payload the kind uses
	case mCAS:
		k.id(m.Cond)
		k.id(m.Sess)
		k.head(&m.Head)
	case mGetObjs, mList:
		k.head(&m.Head)
	case mPutObj, mDel:
		k.obj(&m.Obj)
	case mPutBirth:
		k.birth(&m.Birth)
	}
}

// msgKey orders messages by ID-free fields.
func msgKey(m *Msg) uint64 {
	if m.Kind == 0 {
		return 1 << 63
	}
	return uint64(m.Kind)<<40 | uint64(m.Ent)<<36 | uint64(m.Node)<<32 | uint64(m.Lane)<<30 |
		uint64(m.Res)<<26 | b2u(m.Waited)<<25 | b2u(m.Applied)<<24 | uint64(uint16(m.Head.Rev))<<8 | uint64(uint8(m.Obj.Base))
}

func objKey(o *Obj) uint64 {
	return uint64(uint16(o.Base))<<40 | b2u(o.Snap)<<39 | b2u(o.Present)<<38 | uint64(uint8(o.Min))<<24 |
		uint64(uint8(o.Max))<<16 | uint64(uint8(o.Level))<<8 | uint64(uint8(o.Stamp))
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// Hash returns the 128-bit hash of the canonical encoding of s.
func (k *canon) Hash(c *Case, s *State) [2]uint64 {
	k.encode(c, s)
	return [2]uint64{maphash.Bytes(k.seeds[0], k.b), maphash.Bytes(k.seeds[1], k.b)}
}

func (k *canon) encode(c *Case, s *State) {
	k.b = k.b[:0]
	k.g++
	k.next = 0
	var mo [maxMsgs]int
	for i := range mo {
		mo[i] = i
	}
	order(mo[:c.Msgs], func(i int) uint64 { return msgKey(&s.Msgs[i]) })
	for p, i := range mo[:c.Msgs] {
		k.pos[i] = int8(p)
	}
	for n := 0; n < c.Nodes; n++ {
		k.id(s.Nodes[n].Sess)
		k.i8(s.Nodes[n].Clock)
	}
	for e := 0; e < c.Ents; e++ {
		en := &s.Ents[e]
		k.birth(&en.Birth)
		k.bit(en.HasHead)
		k.head(&en.Head)
		var oo [maxObjs]int
		for i := range oo {
			oo[i] = i
		}
		os := oo[:en.NObj]
		order(os, func(i int) uint64 { return objKey(&en.Objs[i]) })
		k.i8(en.NObj)
		for _, i := range os {
			k.obj(&en.Objs[i])
		}
		for n := 0; n < c.Nodes; n++ {
			m := &s.Mem[n][e]
			k.bit(m.HasH)
			k.bit(m.HasImg)
			k.bit(m.Unres)
			k.head(&m.H)
			k.img(&m.Img)
			if m.Unres {
				k.cand(&m.C)
			}
			k.proc(&s.Procs[n][e][0])
			k.proc(&s.Procs[n][e][1])
		}
	}
	for _, i := range mo[:c.Msgs] {
		k.msg(&s.Msgs[i])
	}
	for e := 0; e < c.Ents; e++ {
		g := &s.Ghost[e]
		k.bit(g.EverHead)
		k.bit(g.Lost)
		k.img(&g.LostImg)
		k.i8(g.NFailed)
		for _, f := range g.Failed[:g.NFailed] {
			k.id(f)
		}
	}
	for r := range c.Reqs {
		k.i8(s.Used[r])
		k.i8(s.Acked[r])
	}
	k.b = append(k.b, uint8(s.Restarts), uint8(s.Jumps), uint8(s.Losses), uint8(s.Hedges))
}
