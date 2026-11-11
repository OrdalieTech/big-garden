package model

// apply is a storage request's linearization point: the store's register for
// that key changes (or is read) atomically. A waiting process gets read results
// in its reply buffer now; it sees them only when the reply is delivered.
func (w *world) apply(i int) {
	m := &w.s.Msgs[i]
	en := &w.s.Ents[m.Ent]
	var p *Proc
	if m.Waited {
		p = &w.s.Procs[m.Node][m.Ent][m.Lane]
	}
	res := rOK
	switch m.Kind {
	case mGetHead:
		if !en.HasHead {
			res = rNotFound
		} else if p != nil {
			p.Cur = en.Head
		}
	case mGetBirth:
		if !en.Birth.Present {
			res = rNotFound
		} else if p != nil {
			p.RBirth = en.Birth
		}
	case mGetObjs:
		if !w.present(int(m.Ent), &m.Head) {
			res = rNotFound
		}
	case mList:
		if p != nil {
			w.list(m, p)
		}
	case mPutBirth:
		if en.Birth.Present {
			res = rPreFailed
		} else {
			en.Birth = m.Birth
		}
	case mPutObj:
		res = w.putObj(en, m.Obj)
	case mCAS:
		ok := m.Uncond || (m.Cond == 0 && !en.HasHead) || (m.Cond != 0 && en.HasHead && en.Head.Nonce == m.Cond)
		if !ok {
			res = rPreFailed
		} else {
			w.writeHead(int(m.Ent), m.Head, m.Node, m.Sess, m.Cond)
		}
	case mDel:
		w.del(int(m.Ent), m.Obj.Att)
	}
	m.Applied, m.Res = true, res
	if !m.Waited {
		*m = Msg{}
	}
}

func (w *world) objIndex(e int, att ID) int {
	en := &w.s.Ents[e]
	for i := 0; i < int(en.NObj); i++ {
		if en.Objs[i].Att == att {
			return i
		}
	}
	return -1
}

// present reports whether every object a head names is stored (Att 0 = unused).
func (w *world) present(e int, h *Head) bool {
	ok := func(att ID) bool {
		i := w.objIndex(e, att)
		return att == 0 || (i >= 0 && w.s.Ents[e].Objs[i].Present)
	}
	if !ok(h.Snap.Att) {
		return false
	}
	for i := int8(0); i < h.ND; i++ {
		if !ok(h.Deltas[i].Att) {
			return false
		}
	}
	return true
}

// putObj is PUT If-None-Match: * of an immutable object. A deleted key can be
// recreated by a stale retry of the same upload (same key, same bytes).
func (w *world) putObj(en *Ent, o Obj) uint8 {
	for i := int8(0); i < en.NObj; i++ {
		if en.Objs[i].Att == o.Att {
			if en.Objs[i].Present {
				return rPreFailed
			}
			en.Objs[i].Present = true
			return rOK
		}
	}
	if en.NObj == maxObjs {
		w.bound(boundObjs)
		return rOK
	}
	en.Objs[en.NObj] = o
	en.NObj++
	return rOK
}

func (w *world) list(m *Msg, p *Proc) {
	en := &w.s.Ents[m.Ent]
	switch m.List {
	case listFresh:
		p.Flag = en.Birth.Present || en.HasHead
		for i := int8(0); i < en.NObj; i++ {
			p.Flag = p.Flag || en.Objs[i].Present
		}
	case listResume:
		p.Flag = false
		for i := int8(0); i < en.NObj; i++ {
			o := &en.Objs[i]
			p.Flag = p.Flag || (o.Present && (o.Inc != m.Head.Inc || o.Base > 0))
		}
	case listEmpty:
		p.Flag = false
		for i := int8(0); i < en.NObj; i++ {
			p.Flag = p.Flag || en.Objs[i].Present
		}
	case listCollect:
		cur := &m.Head
		p.NDel = 0
		for i := int8(0); i < en.NObj; i++ {
			o := &en.Objs[i]
			ok := o.Inc == cur.Inc && o.Base < cur.Rev && (cur.Deleted || !refs(cur, o.Att))
			if w.v == AgeGC {
				ok = o.Stamp+w.c.Age <= w.s.Nodes[m.Node].Clock
			}
			if o.Present && ok {
				if p.NDel == maxDel {
					w.bound(boundDel)
					return
				}
				p.Del[p.NDel] = o.Att
				p.NDel++
			}
		}
	}
}

// del deletes an object. G8: a collector never deletes an object the current
// head references, nor one a valid next head could newly reference (base at or
// above the current revision).
func (w *world) del(e int, att ID) {
	i := w.objIndex(e, att)
	if i < 0 || !w.s.Ents[e].Objs[i].Present {
		return
	}
	en := &w.s.Ents[e]
	o := &en.Objs[i]
	if en.HasHead {
		if refs(&en.Head, att) && !en.Head.Deleted { // a tombstone's refs are not live (D25)
			w.fail("G8: deleted object #%d (base %d) referenced by the current head (revision %d)", att, o.Base, en.Head.Rev)
		} else if o.Inc == en.Head.Inc && o.Base >= en.Head.Rev {
			w.fail("G8: deleted object #%d with base %d >= current revision %d, which a valid next head may reference", att, o.Base, en.Head.Rev)
		}
	}
	o.Present = false
}

// writeHead installs a new head after checking the transition (G1, G3, G5-G8).
func (w *world) writeHead(e int, nh Head, node int8, sess ID, cond ID) {
	en, g := &w.s.Ents[e], &w.s.Ghost[e]
	w.checkWrite(e, nh, node, sess, cond)
	en.HasHead, en.Head = true, nh
	if nh.Deleted { // the incarnation's effects end here (G1 scope)
		for i := int8(0); i < nh.Img.Seq; i++ {
			g.Gone |= 1 << (nh.Img.Ops[i] - 1)
		}
	}
	g.EverHead, g.MaxGen = true, max(g.MaxGen, nh.Gen)
	for _, f := range g.Failed[:g.NFailed] {
		if f == nh.Cand {
			w.fail("definite failure: candidate #%d committed after its operation reported it not committed", nh.Cand)
		}
	}
	for n := range w.s.Procs {
		for l := range w.s.Procs[n][e] {
			p := &w.s.Procs[n][e][l]
			if p.Kind == 0 {
				continue
			}
			if p.CID == nh.Cand {
				p.Applied = true
			}
			if nh.Deleted { // a tombstone names no state a read may answer from
				p.SawTomb = p.SawTomb || nh.Gen == want(w.c.Reqs[p.Req])
			} else if reportsState(p.Kind) {
				w.see(p, nh.Img)
			}
		}
	}
}

// see records that img was the head image while p was running (ghost).
func (w *world) see(p *Proc, img Image) {
	if p.saw(img) {
		return
	}
	if p.NSeen == maxSeen {
		w.bound(boundSeen)
		return
	}
	p.Seen[p.NSeen] = img
	p.NSeen++
}

func (p *Proc) saw(img Image) bool {
	for _, s := range p.Seen[:p.NSeen] {
		if s == img {
			return true
		}
	}
	return false
}

func (w *world) checkWrite(e int, nh Head, node int8, sess ID, cond ID) {
	en, g := &w.s.Ents[e], &w.s.Ghost[e]
	img := &nh.Img
	w.checkLedger(img)
	if en.HasHead && cond == 0 {
		w.fail("G6: a genesis applied by If-None-Match on a key with a head (revision %d)", en.Head.Rev)
		return
	}
	if old := &en.Head; en.HasHead && old.Deleted { // only a recreation replaces a tombstone (D25)
		fresh := Image{Seq: 1, Schema: w.c.Reqs[img.Ops[0]-1].Schema, Kept: 1}
		fresh.Ops[0] = img.Ops[0]
		switch {
		case nh.Deleted || nh.Inc == old.Inc || nh.Gen != old.Gen+1:
			w.fail("G6: tombstone (revision %d) replaced by something other than a new incarnation", old.Rev)
		case nh.Rev != old.Rev+1 || nh.Epoch != old.Epoch+1:
			w.fail("G7: recreation at revision %d epoch %d over tombstone revision %d epoch %d", nh.Rev, nh.Epoch, old.Rev, old.Epoch)
		case nh.Owner != node+1 || nh.Sess != sess:
			w.fail("G3: recreation owned by node %d, written by node %d", nh.Owner-1, node)
		case w.c.Reqs[img.Ops[0]-1].Kind != kCreate || *img != fresh || nh.ND != 0 || nh.Tail.Max != 0 || nh.Snap.Base != old.Rev:
			w.fail("G6: recreation is not the genesis of its create alone (%s): the deleted incarnation's data resurrected", imgString(img))
		}
		w.checkChain(e, &nh)
		return
	}
	if !en.HasHead {
		b := en.Birth
		switch {
		case nh.Rev != 1 || nh.Epoch != 1 || img.Seq != 1 || nh.ND != 0 || nh.Snap.Base != 0:
			w.fail("G7: malformed genesis (revision %d, epoch %d, sequence %d)", nh.Rev, nh.Epoch, img.Seq)
		case nh.Owner != node+1 || nh.Sess != sess:
			w.fail("G3: genesis owned by node %d, written by node %d", nh.Owner-1, node)
		case !b.Present || b.Inc == nh.Inc && img.Ops[0] != b.Req: // another incarnation supersedes (D27)
			w.fail("G6: genesis does not match birth.json (birth request r%d, genesis request r%d)", b.Req-1, img.Ops[0]-1)
		case g.EverHead && g.LostImg != (Image{}) && *img != g.LostImg:
			w.fail("G6: genesis published over a used key with a different image (%s, lost head had %s)", imgString(img), imgString(&g.LostImg))
		}
		w.checkChain(e, &nh)
		return
	}
	old := &en.Head
	oi := &old.Img
	ownerChanged := nh.Owner != old.Owner || nh.Sess != old.Sess
	switch {
	case nh.Rev != old.Rev+1:
		w.fail("G7: head revision %d replaced by revision %d", old.Rev, nh.Rev)
		return
	case nh.Inc != old.Inc:
		w.fail("G6: incarnation changed in place")
		return
	}
	for _, r := range headRefs(&nh) {
		if r.Base != old.Rev && !refs(old, r.Att) {
			w.fail("G8: head revision %d references object #%d produced on revision %d, not its predecessor %d (resurrection)", nh.Rev, r.Att, r.Base, old.Rev)
			return
		}
	}
	switch {
	case img.Seq == oi.Seq && *img != *oi:
		w.fail("G7: data changed without a sequence advance (%s -> %s)", imgString(oi), imgString(img))
	case img.Seq == oi.Seq && nh.Tail != old.Tail && !nh.Deleted: // a tombstone drops the tail (D25)
		w.fail("G7: inline tail changed without a sequence advance")
	case img.Seq < oi.Seq || img.Seq > oi.Seq+maxBatch:
		w.fail("G1: sequence %d replaced by %d", oi.Seq, img.Seq)
	case img.Seq > oi.Seq && (!extends(img, oi) || img.Floor < oi.Floor):
		w.fail("G1: head %s does not extend its predecessor %s", imgString(img), imgString(oi))
	case ownerChanged && nh.Epoch != old.Epoch+1, !ownerChanged && nh.Epoch != old.Epoch:
		w.fail("G7: owner change with epoch %d -> %d", old.Epoch, nh.Epoch)
	case ownerChanged && *img != *oi:
		w.fail("G7: ownership change altered data")
	case !ownerChanged && (old.Owner == 0 || old.Owner != node+1 || old.Sess != sess):
		w.fail("G3: head owned by node %d rewritten by node %d (another session or stale owner)", old.Owner-1, node)
	case ownerChanged && nh.Owner != 0 && (nh.Owner != node+1 || nh.Sess != sess):
		w.fail("G3: ownership handed to a session other than the writer's")
	case ownerChanged && nh.Owner == 0 && (old.Owner != node+1 || old.Sess != sess):
		w.fail("G3: release written by a non-owner")
	case nh.Deleted && nh.Owner != 0:
		w.fail("G3: tombstone with an owner")
	}
	if !nh.Deleted { // a tombstone names no state
		w.checkChain(e, &nh)
	}
}

// extends reports whether img's ledger starts with old's.
func extends(img, old *Image) bool {
	for i := int8(0); i < old.Seq; i++ {
		if img.Ops[i] != old.Ops[i] {
			return false
		}
	}
	return true
}

func headRefs(h *Head) []Ref {
	rs := []Ref{h.Snap}
	return append(rs, h.Deltas[:h.ND]...)
}

// checkLedger is G5: an operation ID has at most one committed effect.
func (w *world) checkLedger(img *Image) {
	for i := int8(0); i < img.Seq; i++ {
		for j := int8(0); j < i; j++ {
			if w.c.Reqs[img.Ops[i]-1].ID == w.c.Reqs[img.Ops[j]-1].ID {
				w.fail("G5: operation ID %d applied twice (sequences %d and %d)", w.c.Reqs[img.Ops[i]-1].ID, j+1, i+1)
				return
			}
		}
	}
}

// checkChain verifies that a head names a complete state: one snapshot from 1,
// contiguous deltas whose pre/post images chain, ending at the head's image.
func (w *world) checkChain(e int, h *Head) {
	en := &w.s.Ents[e]
	get := func(r Ref) *Obj {
		i := w.objIndex(e, r.Att)
		if i < 0 || ref(en.Objs[i]) != r || en.Objs[i].Inc != h.Inc {
			return nil
		}
		return &en.Objs[i]
	}
	s := get(h.Snap)
	if s == nil || !s.Snap || s.Min != 1 {
		w.fail("G6: head revision %d names no valid snapshot", h.Rev)
		return
	}
	post, max := s.Post, s.Max
	for _, r := range h.Deltas[:h.ND] {
		d := get(r)
		if d == nil || d.Snap || d.Min != max+1 || d.Pre != post {
			w.fail("G6: head revision %d names a broken delta chain", h.Rev)
			return
		}
		post, max = d.Post, d.Max
	}
	if t := h.Tail; t.Max != 0 { // D16: the inline tail is the last link
		if t.Min != max+1 || t.Pre != post {
			w.fail("G6: head revision %d names a tail that does not follow its refs", h.Rev)
			return
		}
		post, max = t.Post, t.Max
	}
	if max != h.Img.Seq || post != h.Img {
		w.fail("G6: head revision %d chain ends at %s, head says %s", h.Rev, imgString(&post), imgString(&h.Img))
	}
}

// checkState runs the per-state invariants after every transition.
func (w *world) checkState() {
	for e := 0; e < w.c.Ents; e++ {
		en := &w.s.Ents[e]
		if !en.HasHead {
			continue
		}
		if en.Head.Deleted {
			continue
		}
		if !w.present(e, &en.Head) {
			w.fail("G8: the current head (revision %d) references a deleted object", en.Head.Rev)
		}
		img := &en.Head.Img
		for q, seq := range w.s.Acked {
			// (a genesis after a lost tombstone starts at generation 1 again: Gone scopes it, D27)
			if seq != 0 && int(w.c.Reqs[q].Ent) == e && w.s.AckGen[q] == en.Head.Gen && w.s.Ghost[e].Gone&(1<<q) == 0 && (seq > img.Seq || img.Ops[seq-1] != int8(q+1)) {
				w.fail("G1: acknowledged r%d (sequence %d) is not in the current head %s", q, seq, imgString(img))
			}
		}
	}
}
