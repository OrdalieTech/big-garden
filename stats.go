package garden

import "time"

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
