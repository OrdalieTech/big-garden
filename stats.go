package garden

import (
	"errors"
	"sync/atomic"
	"time"

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
	OldestUnresolved                                           time.Duration
	Storage                                                    [4]struct {
		Attempts, Failures             uint64
		UploadedBytes, DownloadedBytes uint64
	} // fixed order: GET, PUT (including CAS), LIST, DELETE
}

// counters are the Node's cumulative Stats counters.
type counters struct {
	reads, readRejections, mutations, mutationRejections, commits, replays, expired, conflicts atomic.Uint64
	unknown, ownerChanges, activations, activationFailures, corruptions                        atomic.Uint64
	snapshots, compactions, collections, collected                                             atomic.Uint64
}

// observe counts the outcome of one public operation.
func (n *Node) observe(read bool, res Result, err error) {
	c := &n.counters
	var e *Error
	switch {
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
}

// countCommit counts one committed publication by its kind.
func (n *Node) countCommit(c *candidate) {
	switch {
	case c.op != nil:
		n.counters.commits.Add(1)
		if c.base != nil && c.object != nil && c.next.Snapshot.Key == c.object.Key {
			n.counters.snapshots.Add(1)
		}
	case c.object != nil && c.next.Snapshot.Key == c.object.Key:
		n.counters.snapshots.Add(1)
	case c.object != nil:
		n.counters.compactions.Add(1)
	default:
		n.counters.ownerChanges.Add(1)
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
		Collections: c.collections.Load(), CollectedObjects: c.collected.Load()}
	n.mu.Lock()
	s.CacheBytes = n.reserved
	for _, e := range n.entities {
		s.CacheBytes += e.bytes.Load()
		s.QueuedRequests += e.waiting.Load()
		if e.mu.TryLock() { // a busy entity is resident by definition
			if e.db != nil {
				s.ResidentEntities++
			}
			e.mu.Unlock()
		} else {
			s.ResidentEntities++
		}
	}
	n.mu.Unlock()
	s.InflightStorage = int64(len(n.st.(limited).sem))
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
