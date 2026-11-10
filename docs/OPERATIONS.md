# Operating the library

What a deployment must provide, what the library guarantees on which stores,
how to change versions and schemas, and what to do when something goes wrong.
The contract behind it is PLAN §3, §10 and §21; DECISIONS lists the amendments.

## Deployment

One consumer process needs an S3 client (`Config.S3`), a bucket and prefix
(the namespace), a `NodeID` and a writable `CacheDir`. There is no daemon.
Several processes may run on one namespace: route every operation on a key to
its owner (`Resolve`, or the `Route` of a `not_owner`/`route_changed` error),
take over only through `Takeover` with the observed `Route`, and use
`Preferred` for placement only. Report ready after `Open` returned: it
probed the store's conditional writes. The cache directory is disposable:
deleting it costs downloads, never data (D21: the warm tier is a hint).

Credentials: only this library's processes may write under the prefix.
Normal collection deletes only immutable objects under `entities/*/*/objects/`;
deleting heads, births or the namespace descriptor is an operator action.

## Operational envelope

- **No durable timers.** Nothing runs for an entity unless a caller calls
  (PLAN §11.1). Schedule `Collect` for abandoned uploads if you need them gone
  sooner than the owner's next maintenance (it collects after each one).
- **Callbacks are trusted code.** They run inside the library's transaction
  under its SQL authorizer; they must be deterministic in their effect for a
  given request hash, and must not block on another entity (PLAN §4.2).
- **One entity, one serialized writer.** No cross-entity atomicity. A hot
  entity's rate is bounded by one head CAS round trip per publication;
  batching (`BatchMax`, default 16) shares a publication among queued
  writers. Measured on Scaleway fr-par: 22 updates/s for one client, 93/s for
  8 and 170/s for 32 clients on one entity (docs/PERFORMANCE.md).
- **Deduplication horizon.** A `MutationID` is deduplicated for
  `ReceiptRetention` (default 1 h, at most 720 h); an older ID gets
  `id_expired`, never a second application.
- **Authority region.** Every strong operation needs the bucket's region:
  during an S3 outage strong reads and writes fail (`unavailable`,
  `outcome_unknown`), admissions stay bounded, and nothing is acknowledged
  from local disk.
- **Sizes.** `EntityBytes` up to 256 MiB per entity; 64 KiB heads; 32 deltas
  per chain; results up to 64 KiB.

## Provider qualification

The guarantees hold only on a store that passes `qual doctor` (atomic
If-None-Match and If-Match on PUT, read-after-write, exact attempt counts).

| Store | Status | Evidence |
|---|---|---|
| Scaleway Object Storage, fr-par | Qualified (2026-11-08) | `qual doctor --remote`: 1000 rounds × 32 contenders, one winner each; stalled conditional PUT neither blocks reads nor other writes and gets 412 if overtaken; core suite, kill and linearizability harness passed; latencies in docs/PERFORMANCE.md. 3.5 h soak from three hosts (2026-11-10, 449,101 writes, re-verified by `qual check`); the 24 h remote soak has not run |
| RustFS 1.0.4 (pinned digest) | Qualified, local tests | research.lock.json `rustfs_fixture`; under heavy contention it may reset a losing conditional PUT's connection instead of answering 412, resolved by reading the head |
| MinIO (pinned digest) | Qualified, local tests | research.lock.json `minio_fixture`; a GET blocks ~29 s behind a stalled conditional PUT: an availability gap, not a safety one |
| AWS S3, others | Not qualified | run `qual doctor` and the suites against them first |

Re-qualify after any change to SQLite (modernc), LTX, the AWS SDK's retry
behavior, the store or its proxy layer, or the publication code.

## Versions, schemas and rollback

- **Schema.** A caller-defined `uint32`. `Update`/`Read` with another schema
  get `schema_mismatch` with `Error.Schema`, the stored one: older code
  defers to newer Nodes, newer code runs `Migrate` (D20). Deploy schema
  changes only with their migration.
- **Library format.** Heads and descriptors carry `format`/`codec` fields and
  decode strictly: a binary that does not know a format refuses the entity
  (`corrupt`, the message names the unsupported format; at Open for the
  namespace descriptor) instead of rewriting it. A downgrade is safe for
  entities still at the old format and refused for the others.
- **Rollback of data.** There is no in-place rollback. `Export` writes a
  verified archive at a `Version`; restoring means `Import` of that archive
  into a fresh namespace (another prefix), then routing callers there, so old
  Nodes still running on the old namespace cannot write into the restored one
  (PLAN §10.3-10.4).

## Costs and behaviors to plan for

Measured on local RustFS with Scaleway-like delays (GET +9 ms, PUT +35 ms),
2026-11-10 review (`.work/paradigm-probe.md`):

- **Routing.** A request on the owner costs 42 ms and 1 PUT; a misrouted
  one 195 ms, 5.8 GET and 2.9 PUT (takeover, activation, and the old owner's
  `not_owner` on its next request). Keep requests for a key on one Node:
  route by `Resolve`/the error `Route`, and back off on `route_changed`.
- **NodeIDs.** One `NodeID` per live process. Two processes with one
  `NodeID` (a blue/green overlap) are safe, but every alternation is an
  implicit takeover (D4): a `not_owner` and two activations.
- **Restart.** After a restart the first read of an entity costs 2 GET and
  1 PUT (68 ms, takeover of the dead session's ownership), the first write
  148 ms; the warm tier (D21) avoids re-downloading the database.
- **Create.** 2 GET, 3 PUT and 1 LIST (birth, genesis snapshot, head;
  namespace check), p50 185 ms.
- **Retry horizon.** A `MutationID` older than `ReceiptRetention` gets
  `id_expired`, even if never used (`Create` too). A job retried later than
  that needs a fresh ID and its own idempotency record in the entity (a row
  keyed by the job's ID, checked by its callback).
- **Store outage.** An `Update` waits up to `CommitTimeout` (10 s), then
  returns `outcome_unknown`; the node keeps resolving and commits or abandons
  it once the store answers (`Lookup` tells). Size caller deadlines for it.
- **Re-entrancy.** A callback must not call the library on its own entity:
  the entity mutex is held, so the inner call waits until its context ends.

## Runbooks

- **Ambiguous mutation (`outcome_unknown`).** Retry with the same
  `MutationID` and request, or call `Lookup(ref, id)`: it returns the
  committed result, `absent_at_version` (not committed as of `Error.At`; a
  retry with the same ID is then safe), or `id_expired` (past the receipt
  floor: unknowable).
  Never retry with a new ID.
- **Prolonged unresolved publication.** `Stats.OldestUnresolved` grows: the
  store is failing or slow. The entity answers `outcome_unknown` to that ID
  and `unavailable` to others until it resolves; fix the store, nothing to do
  in the library.
- **Lost or full cache disk.** Restart the process with an empty `CacheDir`:
  state is in S3. `capacity` errors mean `CacheBytes` or `ResidentEntities`
  are too small for the working set; warm files are evicted first.
- **Takeover storms.** Entities moving back and forth mean callers route
  without `Resolve` or several Nodes share a `NodeID`. One `NodeID` per
  process; follow the `Route` in errors.
- **`state_missing`.** A head, birth or namespace descriptor is gone while
  other objects remain. The library never re-creates state over it. Restore
  from an export into a new namespace, or investigate the deletion.
- **`corrupt`.** An object or image failed a checksum. The entity stays
  unavailable; other entities are unaffected. Keep the objects for analysis,
  restore the entity from an export.
- **Incomplete creation.** Retry `Create` with the same ID: it finishes or
  replays.
- **`id_expired`.** The caller retried after `ReceiptRetention`; raise it if
  callers legitimately retry later.
- **Deleting an entity.** `Delete(ref)` on the owner (`not_owner`: follow
  the `Route`). It returns nil once the head is a tombstone and every object
  of the incarnation is deleted; on any error, call it again with the same
  `Ref` (anywhere a deleted entity's `Delete` or `Collect` only collects).
  Afterwards the entity directory holds only `birth.json` and `head.json`,
  which stay. Other nodes drop their local copy when they next touch the key;
  a node that never does keeps its warm file until `CacheBytes` evicts it, so
  for erasure delete the `CacheDir` of such nodes or `Evict` there too. With
  bucket versioning on, noncurrent versions must expire (as for `Snapshot`).
  A deleted key is recreated by `Create` or `Import`: a new incarnation with
  its own receipts; a retried `Create` after the `Delete` recreates it.
- **Storage growth.** `Collect` an entity to delete superseded objects; the
  owner already does after each compaction or snapshot.
- **Credential rotation.** Swap the `*s3.Client` credentials provider; the
  library holds no credentials of its own.

## Monitoring

`Stats` is cumulative per Node: request counts and outcomes, S3 attempts,
failures, bytes and time per class (`Storage[i]`), activations by kind (cold,
`WarmHits`, `CatchUps`, `ActivationBytes`), maintenance and collection, and
gauges (resident and warm entities, cache bytes, queued requests, oldest
unresolved). docs/COST.md turns them into money and time.

## Licenses

The library is pure Go (no cgo, no native code). Runtime dependencies of a
minimal consumer (`qual footprint`):

| Module | Version | License |
|---|---|---|
| github.com/aws/aws-sdk-go-v2 (and 9 submodules), github.com/aws/smithy-go | v1.47.8, v1.28.8 | Apache-2.0 |
| github.com/superfly/ltx | v0.5.3 | Apache-2.0 |
| modernc.org/sqlite, libc, mathutil, memory | v1.61.1, v1.79.0, v1.7.1, v1.12.1 | BSD-3-Clause (SQLite itself: public domain) |
| github.com/pierrec/lz4/v4 | v4.1.23 | BSD-3-Clause |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748 | BSD-3-Clause |
| golang.org/x/sys | v0.49.0 | BSD-3-Clause |
| github.com/dustin/go-humanize | v1.1.0 | MIT |

Distributions must carry these modules' license texts (`go mod vendor`
copies them, or take them from the module cache).
