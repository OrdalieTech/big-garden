package garden

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// Stats is a copied aggregate snapshot of a Node's counters and gauges.
// Relationships between fields are not atomic; use them for telemetry only.
type Stats struct {
	Reads, Mutations, Commits, Replays, Expired, Conflicts     uint64
	ReadRejections, MutationRejections, UnknownOutcomes        uint64
	OwnerChanges, Activations, ActivationFailures, Corruptions uint64
	CASAttempts, CASConflicts, AmbiguousPuts                   uint64
	Snapshots, Compactions, Collections, CollectedObjects      uint64
	SnapshotBytes, CompactionInputBytes, CompactionOutputBytes uint64
	CollectedBytes, UploadedBytes, DownloadedBytes             uint64
	ResidentEntities, QueuedRequests, InflightStorage          int64
	CacheBytes, StagingBytes                                   int64
	WarmHits, CatchUps, ActivationBytes, WarmEvictions         uint64 // D21: activations from a warm file as is or caught up; bytes GET by activations
	StaleHeads                                                 uint64 // heads older than one already seen: the store broke its contract (refused)
	WarmEntities, WarmBytes                                    int64  // the warm tier (also in CacheBytes)
	OldestUnresolved                                           time.Duration
	Batches                                                    [maxBatch]uint64 // [16]: committed publications by member count - 1
	Storage                                                    [4]struct {
		Attempts, Failures             uint64
		UploadedBytes, DownloadedBytes uint64
		Time                           time.Duration // in calls, retries and backoff included: Time/calls is the latency the caller pays
	} // fixed order: GET, PUT (including CAS), LIST, DELETE
}

// counters are the Node's cumulative Stats counters.
type counters struct {
	reads, readRejections, mutations, mutationRejections, commits, replays, expired, conflicts atomic.Uint64
	unknown, ownerChanges, activations, activationFailures, corruptions                        atomic.Uint64
	snapshots, compactions, collections, collected                                             atomic.Uint64
	snapshotBytes, compactionIn, compactionOut, collectedBytes                                 atomic.Uint64
	batches                                                                                    [maxBatch]atomic.Uint64
	warmHits, catchUps, activationBytes, warmEvictions, staleHeads                             atomic.Uint64
}

// observe counts the outcome of one public operation on key, and logs one
// that needs an operator (outcome_unknown, corrupt), at most once a second.
func (n *Node) observe(key string, read bool, res Result, err error) {
	c := &n.counters
	var e *Error
	switch {
	case err == nil && res.Version.Sequence == 0: // a callback panic is unwinding: count nothing
	case err == nil && read:
		c.reads.Add(1)
	case err == nil && res.Replayed:
		c.replays.Add(1)
	case err == nil:
		c.mutations.Add(1)
	case !errors.As(err, &e): // a callback error
	case read:
		c.readRejections.Add(1)
	default:
		c.mutationRejections.Add(1)
	}
	if e != nil {
		for code, ctr := range map[Code]*atomic.Uint64{CodeIDExpired: &c.expired, CodeIDConflict: &c.conflicts,
			CodeOutcomeUnknown: &c.unknown, CodeCorrupt: &c.corruptions} {
			if e.Code == code {
				ctr.Add(1)
			}
		}
	}
	if e == nil || (e.Code != CodeOutcomeUnknown && e.Code != CodeCorrupt && e.Code != CodeStateMissing) || n.cfg.Logger == nil {
		return
	}
	// corrupt and state_missing are rare and need an operator: always logged;
	// outcome_unknown comes in bursts during an outage: at most once a second.
	if last, now := n.logged.Load(), time.Now().UnixNano(); e.Code != CodeOutcomeUnknown || now-last >= int64(time.Second) && n.logged.CompareAndSwap(last, now) {
		sum := sha256.Sum256([]byte(key)) // the entity's directory name under entities/, never the key itself
		attrs := []any{"code", string(e.Code), "entity", hex.EncodeToString(sum[:]), "operation", hex.EncodeToString(e.Operation[:])}
		if e.Route != nil {
			attrs = append(attrs, "owner_session", hex.EncodeToString(e.Route.Owner.Session[:]), "epoch", e.Route.Epoch)
		}
		n.cfg.Logger.Warn("garden: "+e.Message, attrs...)
	}
}

// countCommit counts one committed publication by its kind.
func (n *Node) countCommit(c *candidate) {
	k := &n.counters
	if c.op != nil {
		k.commits.Add(1)
		k.batches[len(c.more)].Add(1)
	}
	switch {
	case c.object != nil && c.base != nil && c.next.Snapshot.Key == c.object.Key:
		k.snapshots.Add(1)
		k.snapshotBytes.Add(c.object.Bytes)
	case c.object != nil && c.op == nil:
		k.compactions.Add(1)
		k.compactionOut.Add(c.object.Bytes)
		for _, d := range c.base.Deltas {
			if !slices.ContainsFunc(c.next.Deltas, func(r head.Ref) bool { return r.Key == d.Key }) {
				k.compactionIn.Add(d.Bytes)
			}
		}
	case c.op == nil:
		k.ownerChanges.Add(1)
	}
}

// Stats returns a copied snapshot of the Node's counters and gauges.
func (n *Node) Stats() Stats {
	c := &n.counters
	s := Stats{Reads: c.reads.Load(), ReadRejections: c.readRejections.Load(), Mutations: c.mutations.Load(),
		MutationRejections: c.mutationRejections.Load(), Commits: c.commits.Load(), Replays: c.replays.Load(),
		Expired: c.expired.Load(), Conflicts: c.conflicts.Load(), UnknownOutcomes: c.unknown.Load(),
		OwnerChanges: c.ownerChanges.Load(), Activations: c.activations.Load(), ActivationFailures: c.activationFailures.Load(),
		Corruptions: c.corruptions.Load(), Snapshots: c.snapshots.Load(), Compactions: c.compactions.Load(),
		Collections: c.collections.Load(), CollectedObjects: c.collected.Load(), SnapshotBytes: c.snapshotBytes.Load(),
		CompactionInputBytes: c.compactionIn.Load(), CompactionOutputBytes: c.compactionOut.Load(),
		CollectedBytes: c.collectedBytes.Load(), StagingBytes: n.staging.Load(), WarmHits: c.warmHits.Load(),
		CatchUps: c.catchUps.Load(), ActivationBytes: c.activationBytes.Load(), WarmEvictions: c.warmEvictions.Load(),
		StaleHeads: c.staleHeads.Load()}
	for i := range s.Batches {
		s.Batches[i] = c.batches[i].Load()
	}
	n.mu.Lock()
	n.wmu.Lock()
	for _, w := range n.warm {
		s.WarmEntities, s.WarmBytes = s.WarmEntities+1, s.WarmBytes+w.bytes
	}
	n.wmu.Unlock()
	s.CacheBytes = n.reserved + s.WarmBytes
	for _, e := range n.entities {
		s.CacheBytes += e.bytes.Load()
		if t := e.since.Load(); t != 0 {
			s.OldestUnresolved = max(s.OldestUnresolved, time.Since(time.Unix(0, t)))
		}
		s.QueuedRequests += e.waiting.Load()
		if e.mu.TryLock() {
			if e.db != nil {
				s.ResidentEntities++
			}
			e.mu.Unlock()
		} else if e.bytes.Load() > 0 { // busy: resident only with a file
			s.ResidentEntities++
		}
	}
	n.mu.Unlock()
	s.InflightStorage = int64(len(n.st.(limited).sem))
	for i := range s.Storage {
		s.Storage[i].Time = time.Duration(n.st.(limited).busy[i].Load())
	}
	if s3, ok := n.st.(limited).Store.(*store.S3); ok {
		k := s3.Counts()
		s.CASAttempts, s.CASConflicts, s.AmbiguousPuts = k.CASAttempts, k.CASConflicts, k.AmbiguousPuts
		for i := range s.Storage {
			s.Storage[i].Attempts, s.Storage[i].Failures = k.Attempts[i], k.Failures[i]
			s.Storage[i].UploadedBytes, s.Storage[i].DownloadedBytes = k.Uploaded[i], k.Downloaded[i]
			s.UploadedBytes += k.Uploaded[i]
			s.DownloadedBytes += k.Downloaded[i]
		}
	}
	return s
}
