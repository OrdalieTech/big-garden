package model

import (
	"fmt"
	"strings"
)

// finish returns an operation's outcome to its client and frees the process.
func (x *px) finish(o out) {
	x.report(o)
	for _, w := range x.p.W {
		if w != 0 {
			x.orphan(int(w - 1))
		}
	}
	*x.p = Proc{}
}

// report checks one client-observed outcome against the history (the
// acknowledgment checks): what was acknowledged is durable and unique, every
// state-dependent answer was computed from an image that was the head at some
// instant of the call (G2, G4), and a definite failure never commits.
func (x *px) report(o out) {
	p, r := x.p, x.r
	if p.Acked {
		return
	}
	g := &x.s.Ghost[x.e]
	// A definite failure of an operation with an ID claims its effect never
	// happens: the client may give up on it. (Takeover, release and compaction
	// failures describe the current route, not their own candidate.)
	if o.def && isMutation(r.Kind) && o.code != cOK && o.code != cReplayed && p.CID != 0 {
		if p.Applied {
			x.fail("definite failure: %s returned %s although its candidate #%d committed", x.name(), codeNames[o.code], p.CID)
		}
		if g.NFailed == maxFailed {
			x.blocked = true
		} else {
			g.Failed[g.NFailed] = p.CID
			g.NFailed++
		}
	}
	if o.hasImg && !p.saw(o.img) {
		x.fail("G2/G4: %s returned %s from image %s, which was never the head during the call", x.name(), codeNames[o.code], imgString(&o.img))
		return
	}
	switch o.code {
	case cOK:
		if isMutation(r.Kind) {
			x.ack(int(p.Req), o.seq)
		}
		if r.Kind == kRead && o.seq != o.img.Seq {
			x.fail("G2: read returned version %d for an image at sequence %d", o.seq, o.img.Seq)
		}
	case cReplayed:
		if r.Kind != kLookup {
			x.ack(int(p.Req), o.seq)
			break
		}
		q := int(o.req)
		if x.c.Reqs[q].ID != r.ID || o.img.Ops[o.seq-1] != int8(q+1) {
			x.fail("G5: lookup of ID %d returned the receipt of r%d", r.ID, q)
		} else if a := x.s.Acked[q]; a != 0 && a != o.seq {
			x.fail("G5: lookup returned sequence %d for r%d acknowledged at %d", o.seq, q, a)
		}
	case cAbsent:
		for i := int8(0); i < o.img.Seq; i++ {
			if x.c.Reqs[o.img.Ops[i]-1].ID == r.ID {
				x.fail("G5: %s answered absent_at_version for ID %d, which is in that image", x.name(), r.ID)
			}
		}
	case cIDExpired:
		if o.hasImg && r.TS > o.img.Floor {
			x.fail("G5: id_expired above the floor")
		}
	case cIDConflict:
		if o.hasImg {
			if i := receipt(&o.img, x.c.Reqs, r.ID); i < 0 || o.img.Ops[i] == p.Req+1 {
				x.fail("G5: id_conflict without a conflicting receipt")
			}
		}
	case cCallback:
		if !fails(&o.img, r) {
			x.fail("G4: callback error not produced by that image")
		}
	case cSchemaMismatch:
		if o.img.Schema == r.Schema {
			x.fail("G4: schema_mismatch on a matching image")
		}
	}
}

// ack records a successful mutation result (G1, G5).
func (x *px) ack(q int, seq int8) {
	en := &x.s.Ents[x.e]
	if !en.HasHead && x.s.Ghost[x.e].Lost {
		return // an administrator deleted the head: outside the durability contract
	}
	img := &en.Head.Img
	if !en.HasHead || seq < 1 || seq > img.Seq || img.Ops[seq-1] != int8(q+1) {
		x.fail("G1: %s acknowledged at sequence %d but the current head %s does not contain it", x.name(), seq, imgString(img))
		return
	}
	if a := x.s.Acked[q]; a != 0 && a != seq {
		x.fail("G5: r%d acknowledged with sequence %d and %d", q, a, seq)
	}
	for q2, a := range x.s.Acked {
		if q2 != q && a != 0 && x.c.Reqs[q2].ID == x.c.Reqs[q].ID {
			x.fail("G5: two inputs acknowledged for operation ID %d", x.c.Reqs[q].ID)
		}
	}
	x.s.Acked[q] = seq
}

func (x *px) name() string {
	return fmt.Sprintf("%s r%d at n%d", kindNames[x.r.Kind], x.p.Req, x.n)
}

func (w *world) orphan(i int) {
	m := &w.s.Msgs[i]
	m.Waited = false
	if m.Applied {
		*m = Msg{}
	}
}

func imgString(img *Image) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := int8(0); i < img.Seq; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "r%d", img.Ops[i]-1)
		if img.Kept&(1<<i) == 0 {
			b.WriteByte('-')
		}
	}
	fmt.Fprintf(&b, "] schema=%d floor=%d", img.Schema, img.Floor)
	return b.String()
}
