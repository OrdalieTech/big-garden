package kit

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"

	garden "github.com/OrdalieTech/big-garden"
)

// Wakes wakes entities at the times they asked for. It stores only (key,
// due) pairs, in shard entities kit/wakes/<i>; what is due, and what to do,
// lives in the woken entity itself, written in the same transaction as the
// business change that needs it. So a wake is idempotent by construction: the
// handler reads the entity's own due rows, acts on those due at now and
// returns the next time it wants (zero: none). Arm before the mutation that
// needs the wake (a crash in between costs a spurious wake, never a lost
// one). A wake fires at least once, at or after its time, while some Node
// runs Run.
type Wakes struct {
	r      *Router
	shards int
	op     string
}

const armOp, doneOp, wakeTable = "kit.wakes.arm", "kit.wakes.done", "kit_wakes"

// grace covers a business mutation that follows its Arm, plus the clock
// skew two Nodes may have (D22: 5 min each way): a wake may come once more
// up to grace after an arm, and the mutation must commit within it.
const grace = 15 * time.Minute

func ms(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

func stamp(ts ...int64) []byte {
	var b []byte
	for _, t := range ts {
		b = binary.BigEndian.AppendUint64(b, uint64(t))
	}
	return b
}

// NewWakes registers h as the wake handler of the entities woken through r
// (on every Node), over shards shard entities.
func NewWakes(r *Router, shards int, h func(tx *garden.Tx, now time.Time) (next time.Time, err error)) *Wakes {
	w := &Wakes{r: r, shards: shards, op: "kit.wakes.wake"}
	r.Handle(armOp, func(tx *garden.Tx, in []byte) ([]byte, error) { // in: due, armed at, key
		ctx := context.Background()
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+wakeTable+"(key TEXT PRIMARY KEY, due INTEGER NOT NULL, armed INTEGER NOT NULL) STRICT"); err != nil {
			return nil, err
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+wakeTable+" VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET due = min(due, excluded.due), armed = excluded.armed",
			string(in[16:]), ms(in), ms(in[8:]))
		return nil, err
	})
	r.Handle(doneOp, func(tx *garden.Tx, in []byte) ([]byte, error) { // in: next (0: none), woken at, key
		// A row armed within grace of the wake may belong to a mutation that
		// had not committed when the handler ran: it is kept, due again at
		// most at armed+grace. Older rows take the handler's answer.
		ctx, key := context.Background(), string(in[16:])
		if _, err := tx.Exec(ctx, `UPDATE `+wakeTable+` SET due = CASE WHEN armed > ?2 - ?4 THEN min(iif(?1 = 0, armed + ?4, ?1), armed + ?4) ELSE ?1 END WHERE key = ?3`,
			ms(in), ms(in[8:]), key, grace.Milliseconds()); err != nil {
			return nil, err
		}
		_, err := tx.Exec(ctx, "DELETE FROM "+wakeTable+" WHERE key = ? AND due = 0", key)
		return nil, err
	})
	r.Handle(w.op, func(tx *garden.Tx, in []byte) ([]byte, error) {
		next, err := h(tx, time.UnixMilli(ms(in)))
		if err != nil || next.IsZero() {
			return nil, err
		}
		return stamp(next.UnixMilli()), nil
	})
	return w
}

func (w *Wakes) shard(key string) string {
	f := fnv.New32a()
	f.Write([]byte(key))
	return fmt.Sprintf("kit/wakes/%d", f.Sum32()%uint32(w.shards))
}

func request(parts ...[]byte) (garden.Request, error) {
	id, err := garden.NewMutationID(time.Now())
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return garden.Request{ID: id, Hash: [32]byte(h.Sum(nil)), Schema: 1}, err
}

// Arm asks for key to be woken at due or earlier (an earlier arm wins).
func (w *Wakes) Arm(ctx context.Context, key string, due time.Time) error {
	in := append(stamp(due.UnixMilli(), time.Now().UnixMilli()), key...)
	req, err := request([]byte(armOp), in)
	if err != nil {
		return err
	}
	c := Call{Op: armOp, Key: w.shard(key), Req: req, Input: in}
	for try := range attempts {
		c.Create = false
		_, _, err = w.r.Mutate(ctx, c)
		var ge *garden.Error
		if !errors.As(err, &ge) || ge.Code != garden.CodeNotFound { // the shard's first arm creates it
			return err
		}
		c.Create = true
		// exists: another Node created the shard first; state_missing: its
		// create is still between birth and head. Either way, arm it again.
		if _, _, err = w.r.Mutate(ctx, c); !errors.As(err, &ge) || ge.Code != garden.CodeExists && ge.Code != garden.CodeStateMissing {
			return err
		}
		select {
		case <-time.After(rand.N(50 * time.Millisecond << try)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// Run wakes the due entities of the shards Preferred puts on this Node,
// every tick, until ctx ends. Run it on every Node.
func (w *Wakes) Run(ctx context.Context, tick time.Duration) error {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		for i := range w.shards {
			shard := fmt.Sprintf("kit/wakes/%d", i)
			if p, err := garden.Preferred(shard, w.r.m.Members()); err == nil && p == w.r.n.Owner().NodeID {
				w.fire(ctx, shard) // best effort: what fails is due again next tick
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// fire wakes the entities due in shard, then moves each row to its next
// time or removes it, unless it was armed again meanwhile.
func (w *Wakes) fire(ctx context.Context, shard string) {
	rt, err := w.r.n.Resolve(ctx, shard)
	if err != nil {
		return
	}
	now := time.Now().UnixMilli()
	type due struct {
		key string
		at  int64
	}
	var ds []due
	w.r.n.Read(ctx, rt.Ref, 1, func(v *garden.View) ([]byte, error) {
		return nil, v.Query(ctx, "SELECT key, due FROM "+wakeTable+" WHERE due <= ? ORDER BY due LIMIT 1000", []any{now}, func(r *sql.Rows) error {
			for r.Next() {
				var d due
				if err := r.Scan(&d.key, &d.at); err != nil {
					return err
				}
				ds = append(ds, d)
			}
			return r.Err()
		})
	})
	for _, d := range ds {
		at := stamp(now)
		req, err := request([]byte(w.op), []byte(d.key), at)
		if err != nil {
			return
		}
		res, _, err := w.r.Mutate(ctx, Call{Op: w.op, Key: d.key, Req: req, Input: at})
		var ge *garden.Error
		if err != nil && !(errors.As(err, &ge) && ge.Code == garden.CodeNotFound) { // a deleted entity wants no wake
			continue
		}
		var next int64 // 0: no next wake
		if len(res.Value) == 8 {
			next = ms(res.Value)
		}
		in := append(stamp(next, now), d.key...)
		if req, err = request([]byte(doneOp), in); err == nil {
			w.r.Mutate(ctx, Call{Op: doneOp, Key: shard, Req: req, Input: in})
		}
	}
}
