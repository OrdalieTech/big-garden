// Package model is an executable finite-state model of docs/PROTOCOL.md, written
// independently of the production packages (it imports none of them). It is the
// M1 oracle of docs/PLAN.md §14: nodes with sessions, one linearizable register
// per key with opaque tokens, immutable uploads, head CAS, independent apply and
// reply delivery for every storage request, hedged duplicates and resolve (D3,
// D12), takeover including D4 (mutations only), strong reads on any node (D24), release, crash/restart with cache loss, receipts
// with an expiry floor and bounded pruning, compaction at the exact head,
// collection by base revision, create including the D5 resume, and delete
// (a tombstone head) with recreation as a CAS on the tombstone (D25).
//
// The state is a plain value (fixed arrays, no pointers), so a transition is a
// copy plus an explicit step, and a canonical encoding deduplicates states.
// Abstractions, each a scaled-down protocol constant: the database image is a
// ledger of applied requests plus schema, floor and the set of retained receipts
// (it stands for the page image and its checksum); compaction fanout is 2
// instead of 8 and the delta cap is 4 instead of 32; clocks are small integers
// and ReceiptRetention is a case parameter; pruning deletes at most one receipt
// per transaction instead of 1000.
//
// Group commit (D18, PROTOCOL §6 Batches) is bounded to two members: a
// request with More names the queued Update an invocation may take behind it.
// The leader's process runs the member's admission against the local state
// that includes the leader's commit, and one candidate carries both (Req = op,
// More = the committed member); each member's outcome is reported separately.
//
// Invariants: every head write is checked as a transition (G1 lineage, G3
// authority, G5 one effect per ID, G6 genesis and chain, G7 monotonicity, G8
// no resurrection); every state is checked for G1 (acknowledged effects in the
// current head) and G8 (referenced objects exist); every client response is
// checked against the history (G2/G4: a state-dependent answer comes from an
// image that was the head during the call; acknowledgments are durable and
// unique; a definite failure of an operation with an ID never commits). G1 is
// scoped to the acknowledged incarnation: Delete ends its effects.
//
// Search: Exhaustive expands the complete reachable graph of a bounded case
// once per canonical state (64-bit fingerprints, omission bound reported);
// Shortest finds minimal counterexamples by iterative deepening; Random runs
// seeded traces with every reply split. Bounded cases deliver a reply at its
// request's apply point (see Case.Split) and drop abandoned requests that can no
// longer take effect (world.sweep); both are exact for the correct protocol.
package model

// ID is a fresh unique identifier: nonce, upload attempt, session, incarnation
// or candidate. The canonical encoding renames IDs by first appearance.
type ID uint16

const (
	maxNodes  = 3
	maxEnts   = 2
	maxMsgs   = 8
	maxObjs   = 16
	maxDeltas = 4 // model delta cap (production 32)
	fanout    = 2 // model compaction fanout (production 8)
	tailCap   = 2 // sequences an inline tail may span (stands for InlineBytes, D16)
	maxOps    = 8 // ledger capacity (Kept is a uint8 mask)
	maxReqs   = 20
	maxSeen   = 8
	maxFailed = 8
	maxDel    = 8
)

// Image is the logical database image named by a head: the ledger of applied
// requests (Ops[i] = request index+1 applied at sequence i+1), the schema, the
// receipt expiry floor and which receipts are still stored (bit i = receipt of
// Ops[i]). Protocol code reads receipts only through Kept; the full ledger is
// ghost state used by the invariants.
type Image struct {
	Seq, Schema, Floor int8
	Kept               uint8
	Ops                [maxOps]int8
}

// Ref names an immutable object from a head.
type Ref struct {
	Att             ID
	Base            int16
	Min, Max, Level int8
}

// Tail is the D16 inline delta: the last link of the chain, carried inside the
// head and never an object. Max == 0 means no tail.
type Tail struct {
	Min, Max  int8
	Pre, Post Image
}

// Head is the CAS-replaced entity head. Its ETag is its nonce: the store's token
// is content-derived, and the nonce makes every rewrite's content unique.
type Head struct {
	Inc, Nonce, Sess ID
	Cand             ID // ghost: the candidate that produced this head
	Rev, Epoch       int16
	Owner            int8 // node index + 1; 0 = released
	Gen              int8 // incarnation ordinal: 1 for the first, +1 per recreation (what a Ref's incarnation names)
	Deleted          bool // tombstone (Delete): owner 0, no state; only a recreation may replace it
	ND               int8
	Img              Image
	Snap             Ref
	Deltas           [maxDeltas]Ref
	Tail             Tail
}

// Obj is an immutable LTX object: content is fixed by Att; Present tracks
// deletion (a stale retry of the same upload may recreate it).
type Obj struct {
	Att, Inc               ID
	Base                   int16
	Min, Max, Level, Stamp int8
	Gen                    int8 // ghost: the incarnation ordinal a genesis snapshot belongs to
	Snap, Present          bool
	Pre, Post              Image
}

// Birth is the immutable birth record.
type Birth struct {
	Present  bool
	Req      int8 // creating request index + 1 (its ID and bound hash)
	Inc, Att ID
}

// Ent is the store's content for one entity key prefix.
type Ent struct {
	Birth   Birth
	HasHead bool
	Head    Head
	NObj    int8
	Objs    [maxObjs]Obj
}

// Message kinds.
const (
	mGetHead uint8 = iota + 1
	mGetBirth
	mGetObjs
	mList
	mPutBirth
	mPutObj
	mCAS
	mDel
)

// Store results.
const (
	rOK uint8 = iota + 1
	rNotFound
	rPreFailed
)

// LIST modes.
const (
	listFresh  uint8 = iota + 1 // create, birth absent: anything under the entity prefix
	listResume                  // create D5: objects outside birth.inc or with base > 0
	listEmpty                   // create D27: any object
	listCollect
)

// Msg is one storage request. Apply (the store's linearization point) and
// Deliver (the reply reaching its waiting process) are separate transitions.
// An orphan (Waited false) is a request whose client stopped waiting: it may
// still apply, and its reply is dropped.
type Msg struct {
	Kind            uint8 // 0 = free slot
	Ent, Node, Lane int8
	Waited, Applied bool
	Res, List       uint8
	Uncond          bool
	Cond            ID // If-Match ETag; 0 = If-None-Match: *
	Sess            ID // sender session (ghost: publication authority)
	Head            Head
	Obj             Obj
	Birth           Birth
}

// Candidate kinds are the request kinds below.
const (
	kCreate uint8 = iota + 1
	kUpdate
	kMigrate
	kRead
	kLookup
	kTakeover
	kRelease
	kCompact
	kCollect
	kDelete
)

// Cand is a publication candidate (PROTOCOL §2). It lives in node memory, not in
// the process, because it outlives the operation that built it.
type Cand struct {
	Kind       uint8
	Genesis    bool
	Req        int8 // request index + 1 whose receipt it carries; 0 = none
	NN         int8
	Lim        int8 // D12: NN may grow to Lim in the current resolve call
	More       int8 // D18: the batch member's request index + 1 committed with Req; 0 = none
	Att        ID   // object it introduces; 0 = none
	Nonces     [maxNonces]ID
	Base, Next Head
}

// Mem is a node's memory for one entity (D8): cached head, local image, the
// unresolved candidate. A crash loses it.
type Mem struct {
	HasH, HasImg, Unres bool
	H                   Head
	Img                 Image
	C                   Cand
}

// Proc is one running operation: an explicit program counter and locals. A node
// runs at most one operation per entity (the entity mutex) plus one collector.
type Proc struct {
	Kind, PC, Ret, Res, Code uint8
	Req                      int8
	W                        [2]int8 // waited message slots + 1
	Hedged, Redo, Applied    bool
	Acked, Resume, Flag      bool
	SawTomb                  bool // ghost: the head was a tombstone of the request's incarnation during the call
	Act                      ID   // nonce of the head being activated
	Put, Tk, Loops           int8
	Seq                      int8 // result sequence of a committed candidate (the leader's, D18)
	Mb                       int8 // D18 batch member request index + 1 (0 = none)
	MbCode                   uint8
	MbSeq                    int8
	Taken, Proof             bool  // member taken into the batch; committed by the leader's receipt (§2 rule 3)
	Hold                     Image // image the member's held verdict was computed from
	PendSeq                  int8
	Pend                     uint8
	CID, BAtt, Inc           ID
	ObsOwner                 int8
	ObsSess                  ID
	ObsEpoch                 int16
	Cur                      Head
	RBirth                   Birth
	O                        Obj
	NDel                     int8
	Del                      [maxDel]ID
	NSeen                    int8
	Seen                     [maxSeen]Image // ghost: images that were the head during the call
}

// Node is a process: its current session and its local clock.
type Node struct {
	Sess  ID
	Clock int8
}

// Ghost is per-entity history needed by the invariants.
type Ghost struct {
	EverHead, Lost bool
	MaxGen         int8 // highest incarnation ordinal published: a new incarnation's genesis takes the next (D27)
	LostImg        Image
	NFailed        int8
	Failed         [maxFailed]ID // candidates reported as definitely not committed
	Gone           uint32        // requests whose effect was in an incarnation since deleted (G1 is scoped to the live one)
}

// State is the whole model state.
type State struct {
	Nodes                           [maxNodes]Node
	Ents                            [maxEnts]Ent
	Ghost                           [maxEnts]Ghost
	Msgs                            [maxMsgs]Msg
	Procs                           [maxNodes][maxEnts][2]Proc
	Mem                             [maxNodes][maxEnts]Mem
	Used                            [maxReqs]int8 // invocations per request
	Acked                           [maxReqs]int8 // acknowledged result sequence
	AckGen                          [maxReqs]int8 // incarnation ordinal of that acknowledgment (0: in a deleted one)
	Restarts, Jumps, Losses, Hedges int8
	Fresh                           ID // generator; not part of the canonical state
}

// Req is one client request of a case's finite pool. Requests sharing an ID
// differ in Hash: same ID and same Hash is the same request.
type Req struct {
	Kind              uint8
	Ent               int8
	Nodes             uint8 // bitmask of nodes it may be sent to
	Tries             int8  // maximum invocations (retries reuse ID and hash)
	ID, Hash, TS      int8
	Schema, To, Input int8
	More              int8 // D18: request index + 1 that may be queued behind this Update (batch of 2)
	Gen               int8 // incarnation ordinal its Ref names (0 = 1); Create has none
}

// Variant selects the correct protocol or one deliberately broken control.
type Variant uint8

const (
	Correct Variant = iota
	UncondHead
	CheckThenPublish
	CachedRead
	PrematureAck
	FloorlessPrune
	AgeGC
	HedgeNoResolve
	D5NoIdentity
	RetargetCompaction
	TailWithoutObject
	BatchLeaderReceipt
	RideUnresolved
	FollowerStaleRead
	DeleteAsRelease
	ReceiptAcrossIncarnation
	RecreateKeepsData
	SupersedeUnchecked
	numVariants
)

var variantNames = [...]string{
	"correct", "unconditional-head-write", "owner-check-then-publish", "cached-strong-read",
	"premature-ack", "floorless-receipt-pruning", "age-only-gc", "hedge-without-resolve",
	"d5-resume-without-identity", "compaction-retargeted", "inline-tail-without-object",
	"batch-ack-on-leader-receipt", "read-rides-unresolved",
	"follower-stale-read", "delete-done-as-release", "receipt-across-incarnation", "recreation-keeps-data",
	"supersede-over-objects",
}

func (v Variant) String() string { return variantNames[v] }

// Negatives lists the broken variants, each of which must be detected.
func Negatives() []Variant {
	var vs []Variant
	for v := Variant(1); v < numVariants; v++ {
		vs = append(vs, v)
	}
	return vs
}

// ParseVariant maps a name back to its variant.
func ParseVariant(s string) (Variant, bool) {
	for i, n := range variantNames {
		if n == s {
			return Variant(i), true
		}
	}
	return 0, false
}
