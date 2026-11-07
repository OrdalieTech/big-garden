# Implementation plan: durable SQLite entities over S3

**Status:** proposed architecture and executable acceptance contract; not an implementation or a claim of production qualification.  
**Research date:** 7 November 2026.  
**Deliverable:** an ordinary, reusable Go module. Working package name: `state`. The repository owner chooses its final module import path at initialization.  
**Primary constraint:** every successfully acknowledged mutation must be recoverable using only the authoritative object-store namespace, after every compute node and every local cache has disappeared.

## 0. Read this before implementing

Build a library, not a server platform. The selected architecture is **one exclusively managed SQLite database per entity, a small immutable LTX object per published batch, and one conditionally replaced S3 head per entity**. The head contains both the ownership fence and the complete bounded description of the committed database image. Its successful compare-and-swap is the publication and mutation linearization point.

Use `modernc.org/sqlite` for SQLite and the small Go `github.com/superfly/ltx` codec for page-change files. Use the official AWS Go SDK's S3 client rather than implementing signing, credential handling, XML errors, or HTTP protocol details. Implement the narrow SQLite capture, publication, ownership, caching, recovery, and maintenance protocols described below. Do not embed CellD, Litestream, LiteFS, libSQL, a JavaScript runtime, a Rust library, or a sidecar.

Four deliberate boundaries keep this achievable and reviewable:

1. **S3 participates synchronously in every published mutation.** There is no peer-durable or local-durable acknowledgment mode. The design does not promise local-database write latency.
2. **Strong reads validate the head against S3.** The initial release does not advertise lease-free, network-free, linearizable cached reads.
3. **Only a committed history has a single writer.** An old process may temporarily execute speculative local SQL. Its stale head CAS cannot publish. Arbitrary Go callbacks are trusted code, not a sandbox for irreversible external effects.
4. **Durable wakeups are excluded from the initial core.** A stored deadline is durable data, not a promise that a sleeping entity will be discovered and invoked. There is no disguised queue, global scanner, or scheduler in this release.

The first release targets small transactional state: 64 MiB default maximum database size, 256 MiB qualified hard ceiling, 4 KiB pages, and thousands to hundreds of thousands of independently addressed entities. Large binary payloads are outside the module's contract. One hot entity is not horizontally writable.

All performance figures below are **candidate targets or explicitly hypothetical cost inputs**. No new library benchmarks, remote qualification runs, or measured performance comparisons have been executed while preparing this plan. Source inspection and arithmetic do not constitute those tests. M0–M9 must produce the actual evidence.

## 1. Source investigation and reuse decisions

### 1.1 What was inspected

The source references in §23 identify the actual files and documentation inspected. Versioned paths take precedence over default-branch documentation. For default-branch files, preserve the listed file blob hash in the implementation research lock and resolve the enclosing commit before using source as a baseline.

| System | Relevant observed implementation | What it does not establish for this module | Decision |
|---|---|---|---|
| CellD v0.6.1, commit `f2bf648663a610eefde71f3547ad61e9b896b1f0` | Conditional ownership records; ownership epochs; LTX WAL capture; epoch-separated object keys; output gating; distinct bucket and fleet durability paths; recovery of follower-held tails; durable alarm machinery. | Fleet acknowledgment can depend on follower disks before bucket replication. A runtime-wide output gate is not supplied by an ordinary Go callback API. Its Rust/V8 runtime is not reusable under the purity requirement. | Reuse the lessons and failure scenarios, not its runtime or its complete coordination topology. Study bucket mode separately from fleet mode. [S01–S04] |
| Litestream | Current source uses `modernc.org/sqlite` internally; WAL readers validate salts/checksums and collect only committed pages; checkpoint management, LTX staging, compaction, restore, and synchronous wait facilities exist. Its documented embedding API is unstable. | Replication completion alone is not a distributed ownership fence or a per-entity linearizable API. Importing the whole package brings a much broader dependency and operational surface. | Do not embed the full replicator. Reuse a narrowly attributed WAL-reader extraction if its license and tests pass M0; otherwise implement that small parser directly against SQLite's format. [S05–S09] |
| Go LTX | `superfly/ltx` v0.5.2 defines format version 3, a 100-byte header, sorted page frames, transaction ranges, pre/post database checksums, file checksum, decoder and compactor. Its module's compression dependency is LZ4. | It does not capture SQLite transactions, choose an owner, publish a durable commit, validate an S3 implementation, or make a compaction safe to publish. | Use the package directly, behind a small private adapter. Record the format version explicitly outside the LTX bytes. [S10–S12] |
| LiteFS | FUSE interception captures transaction pages; LTX files and rolling page checksums support replication and divergence detection. Architecture documentation uses Consul sessions for leader election and describes asynchronous replication. | Checksumming a divergent database is not prevention of lost acknowledged writes. FUSE, leases supplied by an external service, and replica-local durability violate this design's boundaries. | Study checksum and interception behavior; do not import or reproduce its filesystem or election service. [S13] |
| libSQL / Bottomless | Bottomless has a Rust WAL replication implementation with generations, frame batches, snapshots, upload-progress tracking and S3 clients. The inspected Go local/embedded binding has a `cgo` build constraint and links a native library. | An S3 backup path does not supply this module's ownership protocol. A pure-Go remote driver is not a pure-Go local SQLite engine. | No runtime dependency. Reuse design comparisons only. [S14–S16] |
| Turso Sync | The documented synchronization design pushes logical changes and pulls physical pages against a remote authoritative database. | It is not an S3-only authority or peerless single-writer coordination primitive. Its conflict/sync model is different from this contract. | Do not treat it as an interchangeable implementation of LTX replication. [S17] |
| modernc SQLite | A Go-translated SQLite implementation with `database/sql` support. The inspected v1.61.1 source exposes a connection authorizer through `sql.Conn.Raw`, without an application cgo binding. | Pure Go does not make SQL transactions durably published in S3; nor does it permit arbitrary concurrent manipulation of SQLite's files. | Selected engine, subject to the exact pin, build, authorizer, and WAL-capture gates in M0/M2. [S18–S20] |

Do not claim all of Litestream requires cgo: its inspected core uses modernc, although its repository and optional features have a much larger module graph. The rejection here is about scope, ownership semantics, and integration cost, not an invented inability to embed it.

Do not assume every system using the name LTX has byte-compatible output. In the inspected Go implementation, format versions 2 and 3 share the `LTX1` magic while their page framing differs. The enclosing head must therefore carry `codec: "ltx-v3"`, and an implementation upgrade cannot silently change that interpretation. [S10–S12]

### 1.2 Specific CellD lessons that become regression tests

CellD v0.6.1's output gate holds responses that could reveal not-yet-proven writes, including read-only outputs and errors. Its bucket path verifies ownership after proving bucket durability. Its fleet path has additional recovery/sealing mechanisms because some acknowledged data may still be held by other nodes. Those are materially different contracts. [S01, S02]

This library chooses an easier-to-audit publication protocol: ownership and the committed root share **one** CAS object. It does not infer a durable commit from an unconditional delta upload followed by an independent ownership check.

The later CellD v0.6.3 release explicitly addresses a missing ownership record beside existing stored state: initialization must fail rather than silently recreate an empty entity. That failure becomes a mandatory creation/recovery test here; v0.6.1 is not presumed to contain later fixes. [S04]

The inspected v0.6.1 epoch collector uses the actual restore chain, a monotone retired boundary, checks that its own newest epoch is visible, and guards pinned reads before retiring older prefixes. It specifically warns that a late snapshot outside the restore chain is not a valid replacement base. This is why collection needs a lineage proof, not simply an epoch number or age. This plan uses its own revision/no-resurrection proof rather than copying that collector's distinct epoch-chain protocol. [S31]

Epoch prefixes alone isolate writes; they do not explain which history a successor must retain. Never restore by selecting the highest object key, latest timestamp, largest transaction number, or newest-looking prefix. Recover only the chain explicitly named by the current head.

### 1.3 Dependencies and exact reuse boundaries

Production direct dependencies, initial budget **no more than five Go modules**:

- `modernc.org/sqlite`, candidate pin `v1.61.1`; its declared `modernc.org/libc` version must remain compatible. Use the published Go source as a dependency. Do not regenerate SQLite as part of consumers' builds.
- `github.com/superfly/ltx`, candidate pin `v0.5.2`; use encoder, decoder, checksum helpers, and compactor where qualified. Its LZ4 dependency is counted transitively.
- `github.com/aws/aws-sdk-go-v2` and `github.com/aws/aws-sdk-go-v2/service/s3`; caller supplies a configured S3 client. The core does not import every cloud credential-discovery service.
- `github.com/aws/smithy-go` only if needed for typed error inspection; count it as direct if imported.

Pin compatible AWS module versions together during M0. Store checksums in `go.sum`; no `latest`, floating Git branches, automatic runtime updates, or consumer-side code generators. The candidate modernc version declares Go 1.26.0; choose an available, security-reviewed patch toolchain at or above that version and record its exact distribution hash. This plan does not assert that an arbitrary future toolchain patch is available. [S18]

One optional test-only dependency is `github.com/anishathalye/porcupine`, for linearizability checking against an independently written model. Pin its version in M0. Use standard-library fuzzing and deterministic seeded generators rather than adding a property-test framework by default. [S28]

A WAL-reader extraction is at most approximately 350 non-test lines, including validation and adaptations. Preserve upstream license notices, exact source version/blob hash, and an extraction diff. It is maintained code and counts against the project budget; do not hide it as an unmeasured vendored subsystem. The parser must distinguish a legitimate uncommitted tail from corruption before an expected committed boundary.

### 1.4 LTX versus a purpose-built page format

Evaluate these in M2 using the same captured page stream:

| Candidate | Advantages | Costs / risks | Disposition |
|---|---|---|---|
| Full SQLite image per commit | Very small persistence logic; useful correctness oracle. | O(database size) upload per mutation, poor cold/hot write economics. | Baseline only, never the default scalable implementation. |
| Raw bounded WAL segment + metadata | Simple framing; SQLite's existing commit markers. | Repeated page images, WAL-generation/salt coupling, harder compaction and cross-generation replay. | Research baseline, not selected. |
| Own sorted page delta with stdlib encoding | Exact control, potentially fewer imported bytes. | New format, decoder, compactor, corruption tests, migration obligations. | Fallback only if measured LTX footprint or correctness gates fail. |
| Existing Go LTX | Existing validation, page checksums, compression and compaction; no runtime service. | Format pinning and adapter needed; does not solve capture or coordination. | Selected. |

The performance gate is not “LTX is famous.” At equal correctness, compare CPU, allocations, compressed bytes, decode throughput, binary contribution, and adapter size. A switch requires a decision record and equivalent adversarial tests; removing checksums to make a microbenchmark pass is prohibited.

## 2. Architecture decision record

| Decision | Chosen rule | Rejected expansion |
|---|---|---|
| Partition | One SQLite file per caller-selected entity key. | Automatic shared-database sharding, cross-entity SQL, distributed joins. |
| Authority | One regional S3-compatible namespace with qualified atomic conditional writes and strongly consistent object reads. | Local disk, peer logs, eventually consistent multi-region views as commit authority. |
| Coordination | Ownership plus published database root in the same entity head CAS. | Independent lease object followed by unguarded state publication. |
| Capture | Exclusive library-managed SQLite connection; controlled WAL boundaries; committed-page capture. | FUSE, custom SQLite VFS, generated SQLite fork, arbitrary external database writers. |
| Persistence | Immutable LTX snapshot/delta objects; bounded head descriptor. | One full snapshot per ordinary mutation, mutable delta files, LIST-based recovery. |
| Acknowledgment | Successful publication, or positive resolution proving the operation is in the current durable lineage. | Acknowledgment after local commit, queueing a PUT, follower memory or follower disk. |
| Reads | Strong head validation per read or per already-enqueued read group. | Time-based cached “strong” reads without a validated lease protocol. |
| Routing | Authoritative owner lookup plus deterministic preferred-placement helper; application transports requests. | Trying to serialize a Go closure, built-in RPC framework, service registry. |
| Failure detection | Advisory, caller-driven takeover after failed routing or planned movement. | Treating a timeout, clock, heartbeat, or consistent-hash placement as proof of exclusivity. |
| Scheduling | No durable timers or automatic sleeping-entity invocation in initial core. | A queue or global recurring scan presented as a trivial alarm API. |
| Compaction | Bounded adjacent LTX merges plus occasional full snapshot; exact-head publication. | General LSM engine, lazy page-serving VFS, retargeting stale outputs to a new head. |
| Recovery | Rebuild from an explicitly published root; discard unverified local residue. | Uploading a local WAL merely because its transaction number is higher. |
| Deployment | Library in the caller's process; one node is sufficient; callers provide compute lifecycle. | Required Kubernetes, resident sidecar, provisioning controller, consensus cluster. |
| Purity | Normal builds and tests use `CGO_ENABLED=0`. | Rust/C ABI, WASM runtime, native SQLite driver fallback. |

S3 is itself the shared coordination primitive. This is **not** a claim that distributed coordination has been eliminated or implemented without an underlying strongly consistent service. It is a claim that no *additional* persistent coordination system is required.

## 3. Guarantee, assumption, and threat model

### 3.1 Required object-store contract

For each key, require a linearizable register: a GET and a conditional replacement each take effect at a point within their invocation/response interval. In particular, the provider must supply:

- Atomic whole-object replacement; no successful partial object.
- `PUT If-None-Match: *` with exactly one successful contender for an absent key.
- `PUT If-Match: <opaque ETag>` that tests and replaces atomically against the current object version.
- Strong `GET` visibility after successful writes, including overwrites, across all client connections used by the deployment.
- Opaque version tokens suitable for conditional replacement; no accepted stale conditional write after an intervening head change.
- Durable successful writes under the provider's stated storage failure model.

`LIST` is used for maintenance and initialization diagnostics, **not commit discovery**. Pagination may race with changes; a missed garbage object leaks space until a later sweep, never selects an alternative history. Full ranged reads are optional for future optimization, not required by the initial codec path.

AWS documents conditional PUTs and strong read consistency; Scaleway documents both conditional writes and read-after-write consistency, including GET/LIST. These are necessary published capabilities, not a substitute for concurrent endpoint qualification. An “S3-compatible” label is insufficient. [S21–S24]

Disable caches, CDNs and asynchronously replicated secondary endpoints on the head path. Configure one authority region per namespace. Do not mix regional endpoints that can return different current heads.

### 3.2 Safety guarantees

**G1 — Durable success.** A successfully returned mutation remains in every later authoritative lineage for that entity until a later *application mutation* changes its values. A later snapshot may remove old physical logs but cannot remove the operation's committed effect improperly.

**G2 — Per-entity linearizability.** Successful mutations and strong reads admit a single sequential history respecting real-time ordering. Mutation linearization occurs at head CAS. A read linearizes at its qualifying head GET, provided the result is computed from that head's exact database image.

**G3 — Single authoritative publisher.** Competing owners cannot both replace the same head version. Takeover changes the same CAS token that stale commits require.

**G4 — No speculative output.** The library returns no successful result, data-dependent callback error, read result, or receipt that depends on a locally committed but unpublished successful mutation. A callback may report its own rolled-back attempt as a rejection; it must not describe that proposed state as committed.

**G5 — Retry identity.** Within the accepted operation-ID window, the same entity incarnation, operation ID, and input hash produce at most one committed effect and one stored result. Reusing the ID with another hash is rejected. Expired IDs are rejected, not treated as new operations.

**G6 — Explicit recovery.** Missing/corrupt required objects, unknown format versions, incompatible schemas, or a missing head for an existing entity fail closed. They never become an empty database.

**G7 — Monotone publication.** Every successful metadata rewrite advances the head revision and replaces its nonce. Ownership epochs only increase. Every normal mutation advances the logical data sequence. Compaction and takeover preserve the logical data state.

**G8 — Safe collection.** A collector never deletes a currently reachable object or an object that a valid future head transition can newly reference.

### 3.3 What “zero loss” excludes

No library can recover an acknowledged write after the only durable provider permanently loses or administratively deletes all of its copies. RPO=0 here covers compute failure, process termination, local disk loss, concurrent attempts, message loss, and storage responses that are delayed or lost while the store still honors its contract.

An independently retained backup protects against a different failure set. An asynchronous backup has a nonzero disaster-recovery RPO. Synchronous two-provider acknowledgment would change latency and availability and is not part of this release.

The model assumes correct executing Go/SQLite code and normal host memory/file I/O semantics. It does not promise to detect every valid-looking bit flip in a live SQLite page or arbitrary RAM corruption at the instant of a warm read; doing that would require additional protected storage/VFS machinery. Validate stored artifacts and reopened caches fully, quarantine detected I/O/checksum faults, and disclose this host trust boundary.

Checksums detect accidental corruption and wrong-object replay when compared with trusted head metadata. They do not defeat an administrator who can replace the entire namespace with a mutually consistent old copy. Detecting that rollback requires an external trusted witness or protected monotone record. Do not advertise that property under a sole rewritable S3 authority.

### 3.4 Liveness and unsupported guarantees

Progress requires a reachable store, a runnable candidate node, bounded resource availability, and eventually stable contention. During an object-store partition, strong operations fail or wait within their deadlines; they do not switch to an asynchronous durability mode.

An adversarially paused old process can resume indefinitely later. Clock expiry is not used for safety. Local speculative SQL on two machines is possible; two published authoritative branches are not.

No in-place key deletion/reuse, automatic secure erasure of all provider/backup history, cross-entity atomicity, automatic rollback of external actions, exactly-once callback execution, arbitrary SQL-file access, untrusted native-code isolation, active-active regional writes, historical point-in-time query service, or guaranteed service continuity during loss of the authority region is provided.

## 4. Public Go API and caller responsibilities

### 4.1 API shape

The following signatures are the normative interface sketch. Freeze names and data shapes at M1; do not introduce a framework around them. Type definitions may be split for readability, not turned into extension registries.

```go
package state

// Ref prevents a retry for an old/imported incarnation from reaching a new one.
type Ref struct {
    Key         string
    Incarnation [16]byte
}

type Owner struct {
    NodeID  string
    Session [16]byte
}

type Route struct {
    Ref      Ref
    Owner    Owner // zero Owner means unowned, not nonexistent
    Epoch    uint64
    Revision uint64
}

type MutationID [24]byte // 8-byte big-endian Unix ms + 16 cryptographic random bytes

type Request struct {
    ID     MutationID
    Hash   [32]byte // caller hashes canonical operation name, input, and schema contract
    Schema uint32
}

type Version struct {
    Incarnation [16]byte
    Sequence    uint64
}

type Result struct {
    Value    []byte // copied, bounded; never an open cursor or streaming body
    Version  Version
    Replayed bool
}

type Config struct {
    S3        *s3.Client // official SDK client, supplied by the caller
    Bucket    string
    Prefix    string
    NodeID    string
    CacheDir  string
    Limits    Limits
    Logger    *slog.Logger
}

func Open(context.Context, Config) (*Node, error)
func NewMutationID(time.Time) (MutationID, error)
func Preferred(key string, nodeIDs []string) (string, error)

func (*Node) Owner() Owner
func (*Node) Resolve(context.Context, string) (Route, error)
func (*Node) Create(context.Context, string, Request,
    func(*Tx) ([]byte, error)) (Ref, Result, error)
func (*Node) Takeover(context.Context, Route) (Route, error)
func (*Node) Release(context.Context, Ref) error
func (*Node) Update(context.Context, Ref, Request,
    func(*Tx) ([]byte, error)) (Result, error)
func (*Node) Read(context.Context, Ref, uint32,
    func(*View) ([]byte, error)) (Result, error)
func (*Node) Lookup(context.Context, Ref, MutationID) (Result, error)
func (*Node) Migrate(context.Context, Ref, Request, uint32,
    func(*Tx) error) (Result, error) // Request.Schema is from; uint32 argument is to
func (*Node) Evict(context.Context, Ref) error
func (*Node) Export(context.Context, Ref, io.Writer) (Version, error)
func (*Node) Import(context.Context, string, Request, io.Reader) (Ref, Result, error)
func (*Node) Stats() Stats
func (*Node) Close(context.Context) error

func (*View) Query(context.Context, string, []any,
    func(*sql.Rows) error) error
func (*Tx) Query(context.Context, string, []any,
    func(*sql.Rows) error) error
func (*Tx) Exec(context.Context, string, ...any) (sql.Result, error)
```

This is not an implementation. `Limits`, `Stats`, and the typed error definition are specified in the sections below. Do not add public `Begin`, `Commit`, `RawDB`, arbitrary transaction nesting, general hooks, or blob-storage APIs.

`Query` owns and closes the rows before returning, checks `Rows.Err`, and rejects use outside the callback scope. Mutation cursors such as `INSERT ... RETURNING` must be fully consumed or safely closed before local transaction completion. There is no exported prepared-statement handle with an unbounded lifetime.

### 4.2 Exactly what callbacks may do

Callbacks are trusted, synchronous, bounded Go code. They may issue permitted SQL through their scoped wrapper and compute a bounded result. They may not keep the wrapper/rows, start goroutines that use it, call another entity synchronously, modify the cache files, register side-effecting SQLite functions, send network requests, publish messages, or otherwise expose speculative results.

The library cannot sandbox arbitrary Go code or forcibly stop a callback that ignores context forever. SQL cancellation must interrupt through the driver's supported context path. A noncooperative callback may block that entity until the process is terminated; it may not cause another local callback to run against the same connection. Process-level isolation for hostile code belongs outside the module.

A callback can run again after an unacknowledged failed attempt. Only the committed effect/result is deduplicated. No promise of exactly-once function execution is made.

For external effects, callers may write their own intent row in the same transaction and act only after success, using an external idempotency key where available. The module does not turn that pattern into a scheduler or make the external action atomic with SQLite.

### 4.3 Routing is not callback transport

`Resolve` performs an authoritative head read and returns its recorded owner. `Preferred` implements deterministic rendezvous placement for **unowned/new** keys or advisory redistribution: hash the length-prefixed key and each distinct NodeID with SHA-256; choose the lexicographically largest digest, tie-breaking by NodeID. Reject empty/duplicate NodeIDs; input ordering cannot affect the result.

The application already has a transport and addresses its domain requests to a NodeID through its trusted routing map. The library does not dial addresses found in an untrusted blob or attempt to transmit a Go closure. A nonowner operation returns a typed `not_owner` error containing the authoritative Route. The caller forwards the original request and stable Request identity, not the callback.

For a single node, the owner is that node and no network transport or membership service is needed. A multi-node application's membership list can come from static configuration, ordinary DNS/service discovery, or its existing process supervisor. Membership only influences placement and failure detection; it is never a safety authority.

After a route fails, the caller selects a candidate deterministically from the currently believed available nodes and calls `Takeover` there with the observed Route. No background per-entity heartbeat writes or cluster-membership database are required. A generic counter service in the qualification harness exercises this routing pattern; it is test code, not a resident runtime component.

### 4.4 Errors and ambiguous outcomes

Use one typed error with a stable code, optional Route, operation ID, retry hint, and redacted cause. Initial codes: `not_found`, `not_owner`, `route_changed`, `incarnation_mismatch`, `schema_mismatch`, `id_conflict`, `id_expired`, `absent_at_version`, `outcome_unknown`, `unavailable`, `capacity`, `unsupported`, `corrupt`, `state_missing`, `closed`, `invalid_argument`.

`outcome_unknown` means the mutation may have committed. Reuse the exact Request ID/hash and inputs. Never generate a new ID to recover a transport timeout.

`Lookup` returns a committed stored result when present. `absent_at_version` means absent from a specific validated state at the read linearization point. It is **not** proof that an already-issued, unresolved CAS cannot commit later. `id_expired` does not prove that the historical mutation never happened; its result may have been deliberately retired.

After a known successful publication, local checkpoint/cleanup failure must not be reported as a definite failed mutation. Return the durable result, quarantine the cache, and force clean activation before further state access.

## 5. Storage layout and format contracts

### 5.1 Namespace and keys

A namespace has a random 128-bit namespace ID in an immutable descriptor. `Open` initializes only an empty prefix; if its descriptor is absent but data already exists, fail `state_missing`. On an initialization race, compare the winning descriptor and use it only if its format is supported. An IAM principal must not allow another library instance to initialize unrelated data inside this prefix.

```text
<prefix>/namespace.json
<prefix>/probe/<random-attempt>/...
<prefix>/entities/<first-2-hex>/<sha256-exact-key>/birth.json
<prefix>/entities/<first-2-hex>/<sha256-exact-key>/head.json
<prefix>/entities/<first-2-hex>/<sha256-exact-key>/objects/
    <incarnation>/<base-revision-hex>/<random-attempt>/<sha256>.ltx
```

Reject invalid UTF-8; hash exact valid UTF-8 key bytes without normalization. Keys are nonempty, at most 512 bytes, and are compared with the stored original key to detect misrouting/collision. Never use the raw key as a filesystem path. NodeID is a stable, caller-chosen routing label that must identify only one live routed instance. Overlapping reuse is a configuration error affecting liveness even though CAS still protects data. Session is a fresh cryptographic 128-bit identifier on each `Open`.

NodeID is nonempty valid UTF-8 of at most 128 bytes; it is a routing label, never a URL to dial. Schema versions are positive uint32 values.

No application object belongs in this namespace. Do not use object tags, mutable metadata, or a provider lifecycle rule as part of the correctness protocol.

The namespace descriptor is strict JSON `{ "format": 1, "namespace": "<32-hex ID>", "entity_format": 1 }`, bounded to 1 KiB. It contains no credentials or endpoints. The immutable birth descriptor is strict JSON with exactly `format` (1), `namespace` (32 hex), `key` (the exact key), `incarnation` (32 hex), `operation_id` (48 hex), `input_hash` (64 hex, the bound request hash from §8.3), `schema` (positive uint32), and `attempt` (32 random hex). It is bounded to 4 KiB. Apply the same duplicate-key, unknown-field, integer, UTF-8 and trailing-data rules as the head. Genesis uses base revision 0; later artifact base revisions are the predecessor head's actual revision.

### 5.2 Head object

The JSON schema is fixed at protocol format 1. It contains:

```json
{
  "format": 1,
  "namespace": "32 lower-case hex characters",
  "key": "exact caller key",
  "incarnation": "32 lower-case hex characters",
  "revision": 19,
  "nonce": "32 fresh lower-case hex characters",
  "owner": {"node": "node-b", "session": "32 lower-case hex characters"},
  "epoch": 4,
  "sequence": 31,
  "schema": 2,
  "codec": "ltx-v3",
  "page_size": 4096,
  "page_count": 128,
  "database_checksum": "16 lower-case hex characters",
  "snapshot": {
    "key": "namespace-relative immutable object key",
    "sha256": "64 lower-case hex characters",
    "bytes": 123456,
    "base_revision": 9,
    "min_sequence": 1,
    "max_sequence": 20,
    "pre_checksum": "0000000000000000",
    "post_checksum": "16 lower-case hex characters",
    "level": 0
  },
  "deltas": []
}
```

The example is a shape, not a valid concrete fixture: real head fields and references must satisfy every invariant below. `owner` may be null after Release. A delta has the same reference fields as the snapshot, with a nonzero valid pre-checksum and a contiguous sequence range. The snapshot's level is informational; delta levels guide compaction.

All integers are bounded nonnegative integers decoded into explicit Go integer types, never through float64. Revisions, epochs and sequences must not overflow the agreed signed-63-bit range. Reject unknown format versions, duplicate JSON keys, malformed hex, unrecognized enum values, extra trailing bytes, excessive nesting, unknown fields in format 1, oversized strings and oversized object bodies. A small strict-JSON validation helper may wrap `encoding/json`; do not write a new general JSON implementation.

Limits: head at most 64 KiB; no more than 32 delta references; recursion depth at most 16; reference paths must remain under this entity incarnation's object prefix. A head must name exactly one full snapshot and a contiguous, nonoverlapping delta sequence ending at its `sequence`. Checksum transitions must match, and page counts must be valid.

Metadata revisions advance for mutations, takeovers, releases and compactions. Data sequence advances only for successful logical mutations, creation and schema migrations. A fresh nonce is generated on every metadata rewrite, even if the data state is unchanged; never rewrite an identical head and assume its ETag still fences an old operation. ETags are opaque conditional tokens, not content-integrity hashes.

### 5.3 Immutable LTX objects

Use format 3 from the qualified codec. Freeze page size at 4096. Disallow `HeaderFlagNoChecksum`. Validate file checksum, pre/post database checksum, page ordering, page numbers, file size, all decoder bounds and the exact sequence range expected by the head.

The immutable object name includes both the **base metadata revision** on which publication depends and a fresh attempt ID. Also record SHA-256 and encoded length in the head. Two attempts never share a mutable key. A retry of the same object upload uses the exact same bytes and key; a precondition failure is accepted as already uploaded only after its stored bytes/hash are verified.

A snapshot uses LTX `MinTXID = 1`, `MaxTXID = current sequence`, `PreApplyChecksum = 0`, and every database page required by the format. A delta covers the successful operations in one publication batch. In this library, LTX positions are explicitly assigned logical sequence ranges; they are not assumed to be SQLite's native transaction IDs or Litestream's batching IDs.

External SHA-256 identifies bytes; LTX checksums verify page-image transitions. Neither checksum is an ownership fence. No online restore or compactor may guess that a missing delta is safe to skip.

### 5.4 Library-owned SQLite metadata

Reserve an internal table prefix such as `_state_`. At minimum store: schema version, logical sequence, monotonically nondecreasing receipt-expiration floor, and operation receipts `(operation_id PRIMARY KEY, input_hash, result, sequence)`.

Application SQL cannot read or write these tables, create objects under the reserved prefix, change journaling/storage settings, attach databases, issue transaction control, or load extensions. Use the inspected modernc connection authorizer to enforce the boundary; do not rely on matching the first word of a SQL string. Internal SQL runs under a private mode reachable only while the entity lock is held. Deny unknown sensitive operations by default. [S19]


## 6. SQLite execution and persistence protocol

### 6.1 Connection ownership and SQL boundaries

Each resident entity has one pinned physical `*sql.Conn`, one exclusive operation gate, and a bounded request queue. A lost/bad physical connection quarantines the entity; the driver must not silently reconnect a partially executed transaction into a different or newly created database. Do not expose its `*sql.DB`, connection, filename, WAL, transaction-control SQL, or driver `Raw` capability. Separate entities may execute concurrently. Multiple processes must never open the same cache path; include the process session in the cache root and use exclusive directory creation.

Set and verify, rather than merely request:

- `page_size=4096` before initialization; changing it later is unsupported.
- `journal_mode=WAL`, `wal_autocheckpoint=0`, and initially `synchronous=FULL`.
- Defensive mode and `trusted_schema=OFF`, where supported by the pinned engine.
- A small page cache, bounded busy timeout, database page-count limit, and the SQL authorizer policy.

Exclusive locking mode established before WAL activation may be used if the exact driver/platform qualification proves it works. It is not a requirement for the initial algorithm. Do not assume `journal_size_limit` is a hard limit on an actively growing WAL. Do not weaken synchronization settings before measuring their actual cost and recording a separate safety argument.

The connection authorizer has three internal states under the entity gate: library control, application mutation, and application read. Application read denies every write and schema mutation. Application mutation permits ordinary transactional DML and supported DDL but denies `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ATTACH`, `DETACH`, storage PRAGMAs, extension loading, reserved tables/objects, and filesystem-access functions. Transaction control and metadata access are permitted only for library-issued statements. Multi-statement strings receive authorization for every statement. No SQL-prefix classifier is an enforcement mechanism. Read-only SQL may use ordinary built-in nondeterministic functions, and mutation SQL may do so too: replication stores physical results, not replayed SQL. External-effect user functions remain prohibited.

Use a driver instance/connection hook that does not modify unrelated databases in the embedding application's process. Replacing an authorizer must invalidate or reauthorize prepared statements as documented by the driver. An authorizer callback must not reenter SQLite or mutate the connection. [S19]

Every query is scoped: finalize rows, inspect the terminal error, and verify that the connection is not left with an unfinished statement before committing, checkpointing, exporting, or eviction. Read-only mode is enforced by authorization and SQLite state, not by the callback's type name alone.

### 6.2 Capture without a VFS or asynchronous shadow replicator

The initial capture algorithm deliberately owns every checkpoint boundary:

1. Before a new publication batch, finish checkpointing the previous **published** state with `wal_checkpoint(TRUNCATE)`. Verify the returned status and empty WAL boundary. There are no independent readers holding snapshots and no external writers.
2. Keep the connection open. Disable automatic checkpoints on every connection, including any private diagnostic connection. Application SQL cannot change these settings.
3. Run each accepted mutation as its own SQLite transaction. Update the internal sequence and receipt in that same transaction. A successful mutation therefore has at least one persistent metadata write even when its application SQL is a no-op.
4. Commit SQLite locally, but do not return results or permit a read to observe the unpublished state.
5. Freeze local execution at the batch boundary. Read the WAL header and committed frames while no library writer or checkpoint can change them.
6. Encode the latest committed version of each changed page into one LTX object. The final committed database page count determines truncation; exclude pages beyond it. Assign the library's contiguous logical sequence range to the LTX header.
7. Publish as specified in §6.4. Only then can results leave and the next checkpoint/batch begin.

The WAL reader must validate the magic, supported format version, page size, salts, header checksum, cumulative frame checksums, alignment, and bounded frame lengths. A nonzero commit-size field terminates a committed transaction. Maintain a tentative page map for each transaction and merge it only on its commit marker. Repeated writes to a page keep its last committed image, not its last uncommitted frame. [S03, S07, S25]

SQLite can leave a partial or uncommitted tail after a rollback. Stopping there is legitimate only if every locally successful transaction expected for this batch has already been captured and its internal receipt/sequence is represented. A corrupt frame **before** an expected successful commit is a capture failure, not an empty successful delta. In this exclusively controlled path, an unexpected WAL reset, salt change or missing expected commit quarantines the cache. Do not silently snapshot a potentially incomplete state and call the problem repaired.

Compute the rolling page checksum using the qualified LTX helpers. At the start of a batch, the checkpointed main database contains the previous committed image. Remove the old checksum contribution for changed/deleted pages and add each final page contribution. Handle database growth, shrinkage and page-number changes explicitly. A full checksum scan is permitted during activation and snapshots; an ordinary small mutation must not scan the entire database.

The snapshot oracle used in tests must be independent of the WAL parser: obtain the engine's completed database image through a separately qualified backup/checkpoint path, then compare all page bytes and `integrity_check` results after replay. Do not compare two paths that share the same page-map bug.

SQLite's published WAL-reset issue and its fixed versions are a dependency gate. Record the embedded `sqlite_version()` and compile options, and require a version incorporating the documented fix, not just a Go module version number. The SQLite WAL documentation identifies 3.51.3 as a fixed release. [S26]

### 6.3 Creation: explicit, idempotent, and fail-closed

`Create` is the only ordinary path to a new key. `Update`, `Read`, `Resolve`, and takeover never implicitly initialize missing state.

1. Validate the namespace and key. An existing head returns the existing creation result only for a matching original creation identity/hash; otherwise report that the key already exists without changing it.
2. Conditionally create immutable `birth.json` with the key, namespace ID, new incarnation, creating operation ID/hash, and random creation-attempt nonce.
3. Only the invocation that won that conditional create may construct and publish genesis. It initializes SQLite, executes the creation callback, writes metadata and receipt, and encodes a complete snapshot at sequence 1.
4. Upload the immutable genesis object, then create `head.json` with `If-None-Match: *`, revision 1, epoch 1, and this process's owner/session. Return success only after this publication is confirmed or positively resolved.
5. If a birth PUT response is ambiguous, the same still-running invocation may read and compare the exact attempt nonce before proceeding. It cannot infer ownership from the key alone.

**Deliberate limitation:** a crash after birth creation but before a head exists leaves an incomplete creation requiring an operator decision or a different new key. A later invocation must not automatically complete genesis when it sees birth without head: that state is indistinguishable from deletion of a formerly used head. This is preferable to silently reviving an empty database. The diagnostic must identify `state_missing` and preserve all artifacts. There is no automatic destructive cleanup of birth markers.

Before declaring a key entirely new, inspect its entity prefix for incompatible existing artifacts. This catches obvious metadata loss; it cannot distinguish a truly new key from an administrator deleting *every* trace. That latter event is outside the sole-authority failure model.

A head and birth must agree on key, namespace and incarnation. The original creation identity is retained in birth independently of receipt expiration; an expired creation receipt never authorizes recreation. A retry may return `id_expired` for the old result, but cannot change the existing incarnation.

### 6.4 Unbatched mutation: the reference implementation

The following is the executable state machine, not optional guidance:

```text
VALIDATE -> LOCK -> LOAD_OWNED_PUBLISHED_ROOT -> CHECK_ID_AND_SCHEMA
 -> BEGIN_SQL -> CALLBACK -> INSERT_RECEIPT_AND_SEQUENCE -> COMMIT_SQL
 -> CAPTURE_LTX -> PUT_IMMUTABLE -> CAS_HEAD -> RELEASE_RESULT
```

Detailed rules:

1. Resolve/hydrate the current published image if necessary. The local root must belong to this session and incarnation. A cached owned head is sufficient as the expected token for a write: a later takeover makes the CAS fail. Do not spend an extra GET on every warm write solely to make the cache look current.
2. For a **new** operation, enforce the stored receipt floor and expected schema, run the transaction and capture. A cached deduplication hit or cached rejection that depends on authoritative state must first undergo the same strong-head validation as a read. A stale writer cannot return an old cached result as a newly validated outcome.
3. Write the operation's input hash, copied result, and sequence into its receipt in the same local transaction as application changes. Application callback failure rolls back its transaction. Before returning a callback error or any other state-dependent result without a successful CAS, validate the relevant published image through a fresh head GET. If that image changed, return a routing/retry failure instead of a stale conclusion. Caller-defined callback errors may pass through after rollback and this output barrier; library-originated errors use the typed error contract.
4. Encode to a bounded temporary file, compute its SHA-256 while writing, and close/validate it. Upload using immutable-create semantics. The staged artifact includes the exact base head revision in its key.
5. Construct a new complete head with the same incarnation/owner/epoch, revision plus one, fresh nonce, updated sequence/schema/checksum, and the new bounded manifest. Preserve all unchanged required references.
6. Execute exactly one conditional `PutObject(IfMatch=oldETag)` attempt for this candidate. Its success is the commit. **Do not require a second ownership check after a known successful CAS:** a takeover that follows it must preserve its published root.
7. Release the copied result only after success or positive ambiguity resolution. Update the published local position. Checkpoint failure afterward invalidates the cache, not the durable result.

A failed precondition means the local speculative image cannot simply be reused against a newly read ETag. Reload the authoritative image, resolve the operation ID, and discard the speculative WAL. The caller may retry the same request. The library must not automatically reexecute arbitrary callback code while disguising that fact as continuation of a completed transaction.

Only fully uploaded, verified immutable objects may be referenced. No head may publish a multipart upload in progress or a temporary local filename. Uploading a delta does not itself commit anything.

### 6.5 Group commit: bounded optimization, never weaker durability

First pass every safety test with `BatchMax=1`. Then qualify optional per-entity group commit, initially up to 16 requests collected for at most 2 ms. A request's entire queueing and collection time counts in latency measurements.

Collect only already-arrived mutations. Run their transactions sequentially in FIFO order. Each successful transaction gets a separate sequence and receipt; one LTX and head publication can cover their combined sequence range. They linearize in that order at the publication point. Do not pass a queued read, migration, release, or export barrier to fill a batch.

A failed callback rolls back only its own transaction. If it observed a preceding locally committed but unpublished transaction, hold its error until that preceding state is published; if the batch cannot be published, return the appropriate infrastructure/unknown outcome rather than leaking an authoritative-looking conclusion about discarded state. A callback's own rolled-back proposed values are not described as committed facts.

After local commitment, caller cancellation does not cancel the existence of the candidate. Use a bounded node-owned publication/resolution context rather than letting the shortest batch member silently abort durability for the others; canceled waiters receive uncertainty when appropriate. Freeze admission while a batch has an unresolved head CAS. No second speculative publication is built on an unresolved root. Memory, temporary files, result bytes and callback runtime remain bounded by the batch and entity limits. A batch cannot acknowledge only its “easy” members before S3 publication.

Do not expect a batch of eight on eight different idle entities. Report actual batch-size distributions for each workload; cross-entity batch objects are explicitly out of scope.

## 7. Ownership, reads, and their correctness argument

### 7.1 Takeover protocol

`Takeover(observedRoute)` performs a fresh head GET and validates format, key, incarnation and schema metadata before any change. It may continue only while the incumbent owner/session and ownership epoch still match the observed route; the metadata revision may have advanced through that owner's commits. It must always preserve the **freshly fetched** committed manifest, sequence and schema.

The candidate conditionally replaces that head with owner equal to its own session, epoch plus one, revision plus one, and a fresh nonce. If it loses a race to another owner, return `route_changed`, not a loop that steals the entity back. If only the same incumbent committed another update, a bounded retry against the new head is allowed.

After claim, hydrate exactly the preserved state. A failed hydration leaves the entity unavailable; it does not authorize an empty replacement. Another candidate may later take over and retry. A successful claim is not a promise that recovery has already finished; the implementation must define `Takeover` to return only once recovery succeeds or return an error with the now-current Route. Subsequent calls can discover that ownership moved even if the claiming invocation timed out.

A typical caller policy is a bounded routing attempt followed by takeover on another healthy candidate with jitter. A two-second routing timeout is a starting operational setting, not a safety assumption. Planned handoff uses the same mechanism. Clock jumps, false suspicion, and changing preferred placement may cause extra work but cannot authorize a stale publication.

A process restart always has a new Session. Reusing NodeID does not restore an old session's authority. Single-node startup can resolve and take over each existing entity lazily on first use; it does not need to enumerate or download the whole namespace.

### 7.2 Release, eviction and shutdown

`Release` first settles or explicitly returns uncertainty for in-flight work, then CASes owner to null, incrementing epoch/revision/nonce and preserving state. A lost response is resolved by reading the head. It must not undo a takeover or mutation that already won.

`Evict` closes only a clean, published local cache and releases memory. It does not have to rewrite ownership. The same live session may cold-load it again; a later session must take over. Idle residency is not a durability obligation.

`Close` stops admissions, drains bounded in-flight operations under the provided context, closes connections and transport resources the module itself owns, and returns uncertainty where appropriate. It does not scan all previously owned cold entities merely to rewrite their heads. Callers own the supplied SDK client and must not find it unexpectedly shut down. A forced process exit is handled by takeover, not by requiring graceful shutdown for correctness.

### 7.3 Strong-read protocol

Under the entity operation gate, wait for any earlier local speculative batch to settle. Initiate a fresh noncached head GET **after this read's invocation**. Verify identity/format and owner. Obtain the exact published image named by that head, using the local database only when its published position and checksum match. Execute a read-only transaction, buffer a bounded result, finalize it, and return the head's Version.

The read linearizes at that qualifying GET. A later concurrent takeover or write does not invalidate a coherent result for the earlier head. If GC removed an old manifest object while cold recovery was underway, reread the head and retry from a newer root; never combine pieces of two roots.

A group of reads already queued before a validation GET starts may share that validation and immutable image. A read arriving after the GET starts needs a later validation; it cannot inherit a cached barrier predating its invocation. There is no asynchronous refresh interval masquerading as strong consistency.

`Lookup` uses this same strong validation for receipt results. During S3 failure, return `unavailable` or a deadline error with its typed state; do not silently serve a cached strong read. An explicitly stale-read API could be added only by a later decision record, under a different name and contract.

### 7.4 Why one CAS head is enough under the store assumptions

Consider a current head H with ETag E, and two candidates: an old owner's mutation M and another node's takeover T.

- If M's CAS(E) wins, the stored head includes M's immutable data. T's stale CAS(E) fails. T must reread and preserve M when it retries.
- If T's CAS(E) wins, the owner/epoch and ETag change. M's CAS(E) fails, irrespective of when its local SQL or data upload completed.
- If a response is lost, there is still only one store-selected winner. Resolution determines whether the receipt belongs to the current lineage; it does not create a second branch.

Inductively, every published head names a complete state derived from its predecessor, or an equivalent compaction of that state. Every known successful mutation survives all later takeovers and compactions. Strong reads choose one such state after invocation. Fresh revision/nonce values and the prohibition on in-place rollback prevent ordinary ABA.

A separate “owner is still me” GET followed by an unconditional root PUT has a check-to-use gap. It is forbidden. Uploading under an epoch prefix without atomically selecting its root is also insufficient by itself.

This argument relies on S3's atomic conditional replacement and strong visibility; it is not a proof for stores that ignore conditions or for malicious administrators rewriting history. Formal/model tests in M1 must challenge this argument, not merely restate it.

## 8. Ambiguous outcomes, retry identity, and bounded failure handling

### 8.1 S3 adapter contract

Internally expose only bounded Get, immutable Put, conditional head Put, List and Delete operations, with typed distinctions between a definite precondition rejection and an unknown outcome. This is a private test seam, not a public provider-plugin registry.

For mutable head PUTs, override the supplied AWS client's retry behavior to **one physical attempt**. Otherwise an applied PUT whose response is lost can be retried internally and surfaced as a misleading 412. Explicit resolution owns retries. Preserve opaque ETags exactly as required by the SDK. Never strip quotes, reinterpret the ETag as MD5, or infer a version from Last-Modified. [S21]

Set per-attempt deadlines, bounded connection pools, response/header/body limits, and a finite retry budget. Retries use full jitter and stop on the caller's total deadline. 403 is not “not found”; 404 on a referenced object is not an empty database; 5xx and transport failures after issuing a write are ambiguous. A canceled HTTP request may still be applied by the server later.

### 8.2 Resolution rules

| Observation | Required behavior |
|---|---|
| Immutable PUT returned known success | It may be referenced after local checksum/length verification. |
| Immutable PUT timed out | Retry the exact bytes/key or GET and verify them; never publish a mere assumption that the upload landed. |
| Head CAS returned known success | Committed. Preserve and return the receipt even if local cleanup fails. |
| A first and only head CAS returned a definite precondition rejection | Reload current state; do not overwrite it or reuse the old speculative image. |
| Head CAS may have applied, then timeout/5xx | Mark entity publication unresolved. GET current head and resolve by candidate identity or durable receipt. |
| Current head is the exact candidate | Publication is proven; return its stored result. |
| Current head has advanced | Hydrate its authoritative image and check operation ID/hash/receipt; compaction may have removed the original physical delta but not a live receipt. |
| Current head still appears unchanged | This alone does not prove the pending request cannot apply later. Continue resolution or fence the old expected token. |
| Resolution cannot finish within deadline | Return `outcome_unknown`; require the same operation identity for retry. |

To close an unchanged ambiguous base safely, the current owner may perform a **no-op metadata CAS** preserving all data but advancing revision/nonce. If it wins, the older pending CAS cannot subsequently apply. If it loses, reread and resolve the winner. This fence must follow the same bounded single-attempt/resolution rules; uncertainty in the fence is not evidence of failure. Another node's takeover can provide the same token invalidation.

Never rebase an old staged artifact after such a fence. A new attempt must be built from the new authoritative state and use a fresh artifact key carrying that new base revision.

A late response may arrive after another operation was admitted or after the caller left. Track candidate identity, not a bare error channel, and never let a late completion install an older local cache. Late successful publication cannot be discarded as irrelevant simply because its caller canceled.

### 8.3 Receipt lifetime without replay after pruning

The first eight bytes of MutationID encode an unsigned Unix-millisecond timestamp; the remaining sixteen are cryptographically random. The timestamp is not a lease or a source of ownership authority. It makes a safe, bounded deduplication window possible.

Each entity has a durably stored `expiry_floor_ms`, monotonically nondecreasing. Before accepting an operation, reject an ID whose timestamp is at or below that floor. Reject a timestamp more than five minutes ahead of the local admissible clock. The default result-retention window is 24 hours, configurable within validated deployment limits.

A successful mutation or migration transaction may advance the floor to at least the prior floor and at most its chosen `now - retention`. Delete only receipts at or below the floor, in bounded chunks, and persist the floor change with the deletion. Initial-core pruning is piggybacked on those transactions in chunks of at most 1,000 rows; it is not a background SQL mutation with an unaccounted sequence. An idle entity can retain old receipts until its next mutation. Additional wall-clock admission checks may refuse old IDs sooner, but deletion is protected by the persisted floor. A receipt may remain physically stored after its ID expires; that does not reopen acceptance. Never delete a receipt while continuing to accept its ID as a new operation.

Bind the receipt to the API operation class as well as the caller's payload hash. The stored `input_hash` is SHA-256 of `"state-request-v1\x00" || kind-byte || from-schema-u32be || to-schema-u32be || Request.Hash`, where kind is 1=create, 2=update, 3=migrate, 4=import, and from/to both equal Request.Schema except for migrate. This fixed-width binding prevents an accidentally reused caller hash from replaying a different API operation. The caller still must hash its own complete logical input; the library cannot inspect a Go closure to verify that input.

For unexpired IDs: compare an existing receipt's bound hash before executing any application callback. A matching retry returns the original result/sequence without another effect; a different hash yields `id_conflict`. A completed request may be replayed even after an application schema migration, provided its stored hash matches; **new** requests must match the current schema. Unknown engine formats are always rejected before either path.

Clock skew can shorten or lengthen result availability, but cannot cause an expired ID to execute twice because the durable floor never retreats. Large clock jumps should raise diagnostics and may refuse service; they must not reset the floor. Backups preserve it.

Receipt storage counts against entity limits. A very hot entity with a long retention window can exhaust its state budget; reject admissions rather than silently discard live deduplication evidence. Record receipt bytes/rows in benchmarks and size models. Neither a 64 MiB entity limit nor a 24-hour deduplication promise permits unbounded request volume for free.

### 8.4 Bounded stalls

There is one unresolved publication at a time per entity, a bounded per-entity queue, a bounded number of globally active storage operations, and bounded temporary-file reservations. Slow S3 requests may exhaust those admissions, at which point return `capacity` or `unavailable` with a retry hint. Do not create an unlimited goroutine, retry, or staging-file backlog.

A deadline ends the caller's waiting, not the physical possibility of a remote write. State remains quarantined until the outstanding candidate is proven committed or excluded by a later authoritative head. Never “unlock and hope” after timeout.

## 9. Compaction, collection, and cache reconstruction

### 9.1 Bounded delta manifest, not a full snapshot on every write

A head retains one snapshot and at most 32 adjacent delta references. Ordinary batches produce level-0 deltas. Start with **fanout eight**: merge an oldest contiguous run of eight deltas of the same level into one delta at the next level. Repeat only as needed to stay within bounds. Fanout two would approach one compaction publication per batch and unnecessarily increase PUT counts; it is not the default.

Use the Go LTX compactor with `AllowNonContiguousTXIDs=false` and checksums enabled. Its inspected implementation validates page sizes and position ranges, merges ordered page streams, and copies the final checksum from the last input. The adapter must additionally validate every input object's SHA-256/length and each pre/post checksum transition; do not assume a position-range check is a complete chain check. Wrap readers for cancellation and size limits because the inspected compactor's inner page-buffer methods do not themselves use their context argument. [S12]

A compaction is an exact-head metadata operation:

1. Under the entity gate, select a run reachable from head H. No unresolved mutation may precede it.
2. Stream validated inputs into a fresh LTX object keyed by H's revision. Verify the output range, final page count and state checksum are equivalent to the replaced run.
3. Upload it completely. CAS H to a new revision/nonce with the exact run replaced, preserving data sequence, schema, owner and logical state.
4. A failed CAS abandons the output. It must not retarget the old object to a later head. An ambiguous CAS is resolved like any metadata publication.
5. Only a later safe collection pass may remove old inputs.

Keep this initially serialized per entity. A slow compaction may block that entity but cannot hold a global execution lock. Moving it outside the entity gate requires a separately reviewed revalidation protocol and measured benefit, not an extra generation framework by default.

Create a full snapshot when accumulated replay bytes or manifest depth makes recovery expensive. Initial policy: start snapshot work when decoded delta payload exceeds `live database bytes + 1 MiB`; require the published root to remain below `2 × live database bytes + 1 MiB` of decoded delta payload and the 32-reference cap. A mutation that would exceed a hard bound must incorporate a validated snapshot/merge into its publication or receive backpressure. It cannot publish an unbounded chain and promise to fix it later.

Snapshot capture happens at a frozen, complete state. It can merge the current snapshot, current deltas and a newly captured candidate delta; it must not read an inconsistent main SQLite file while its WAL still contains required pages. The output is a full LTX snapshot at the same final sequence and checksum. Snapshot publication advances metadata revision, not data sequence unless it also publishes a new mutation.

Measure both encoded and decoded bytes. Highly compressible test pages must not conceal an unbounded restore workload. Measure compaction PUTs and rewritten bytes separately; do not count only the foreground two-PUT path.

### 9.2 Safe garbage collection without a lease race

Add one small public maintenance entry point:

```go
func (*Node) Collect(context.Context, Ref) error
```

It collects only unreachable immutable artifacts for that entity, without running application code or opening SQLite. It may run on a nonowner because safety comes from monotonic publication, not a collector lease. Active entities can schedule bounded best-effort passes after compaction. A caller may sweep cold entities it knows about; an operator tool can enumerate entity heads and call this method. That enumeration's GET/LIST costs must be measured. There is no required resident collector service.

The proof depends on the object key's `base_revision` and a strict **no-resurrection rule**: a transition may preserve objects reachable from its immediate predecessor, or introduce newly produced objects under that predecessor's revision. It may never reintroduce an abandoned/unreachable old object merely because its contents look reusable.

Collector algorithm:

1. Read and validate current head H with metadata revision R. Record its reachable snapshot/delta object keys.
2. List immutable artifacts under this exact incarnation. Validate paths before deleting anything.
3. An artifact is eligible only when its encoded base revision is **less than R** and it is absent from H's reachable set. A grace period may reduce churn, but is not the safety fence.
4. Delete eligible artifacts in bounded pages/batches. Never delete namespace, birth, head, arbitrary caller objects, or data for an unrecognized format.

Why a takeover between step 1 and step 4 is safe: an artifact whose base revision is below R cannot be newly published by a still-valid older CAS. Newer transitions may carry forward H's reachable objects or create fresh objects at revision R or later; neither category is deleted. A compaction that was not yet published at H is also excluded when it was based on R, or rendered unpublishable if it was based on an older revision.

A stale PUT can recreate an already deleted orphan; a later pass reaps it. It cannot make that object reachable again. LIST omission delays collection, not correctness. Old readers/export attempts may lose an obsolete object while materializing; they must retry from a current head or fail explicitly. The initial core supplies no indefinitely pinned historical snapshots.

Do not replace this protocol with “check ownership, list old timestamps, delete.” Ownership can change between any check and deletion. Do not expire all objects older than a fixed date: a dormant entity's only snapshot may be old and still live.

Bucket versioning changes physical reclamation: deleting a current key may retain noncurrent versions. Include noncurrent object/head bytes in accounting and define an operator-controlled noncurrent-version policy. That policy is not permission to expire current live artifacts.

### 9.3 Cold activation and recovery

Recovery uses only the current head and its explicit manifest; it performs no LIST to infer a transaction tail.

1. Read/validate head and birth, and check incarnation, format, limits and expected schema. Ownership must be current for mutation admission.
2. Reserve disk and memory. Download the snapshot and deltas to temporary cache files, with at most four concurrent downloads per activation and a global cap.
3. Verify exact lengths and SHA-256. Decode into a new, unopened SQLite file: snapshot first, then contiguous deltas. Validate checksums and truncate to each committed page count. Do not apply pages through a live SQLite connection or leave an old `-wal`/`-shm` attached.
4. Verify the final checksum, page count and internal metadata against the head. Open with the pinned driver and run the qualified consistency check. Release only a complete image through an atomic cache-file installation.
5. Record the published root token with the cache. Any subsequent write still requires its conditional head publication; a current-owner validation alone never replaces that CAS.

Before opening an existing reconstructed/cache database, verify its complete expected image and use SQLite open mode `rw`, never an option that creates a missing file. Only explicit genesis creation may use `rwc`. Cache directories are private (0700), files private (0600), and symlink/path escapes are rejected.

A default startup discards cache directories from older process sessions. Optimizing warm restarts is deferred unless it proves exact-image equivalence without trusting residual WAL. “SQLite recovered this local database successfully” does not mean its newest local transaction was published remotely.

If a required object is missing, first reread the head. If the manifest changed, a collection race may justify restarting recovery at the new root. If the same root still references the missing/corrupt object, report `corrupt`/`state_missing` and stop. Bounded retries distinguish transient unavailability from permanent incomplete state; they never downgrade into an empty entity.

Validate a closed cache file again before reopening; a metadata stamp is not proof that its bytes remain intact. Detected cache corruption is recoverable by discarding the cache. Authority corruption is not repaired by uploading a guessed local history.

## 10. Schema evolution, exports, disaster recovery, and rollback

### 10.1 Schema versions are explicit caller contracts

`Request.Schema` identifies the application schema expected by a new mutation; `Read` takes the expected schema explicitly. The module does not infer a safe application version from an executable's name. A mismatch rejects execution before application SQL.

`Migrate(ref, request, toVersion, fn)` requires the current schema to equal `request.Schema` and `toVersion > request.Schema`. It runs transactional DDL/data changes, schema metadata, receipt and sequence in one local SQLite transaction, then publishes through the same LTX/head path. No separate schema flag may become visible before the data it describes.

A matching completed migration ID is replayable. A failed or uncertain migration is resolved by its receipt, not rerun with a new ID. An old application binary requesting an earlier schema cannot write the upgraded database. Down-migrations and executable rollback across incompatible schemas are unsupported by default.

Engine protocol/codec upgrades are separate from application schema upgrades. Unknown head or LTX versions fail before ownership acquisition. The release procedure must first deploy readers that understand both formats, then enable new writers, or perform a documented maintenance-window migration. The initial implementation accepts only its qualified format; it must not pretend to be forward compatible by ignoring unknown fields.

### 10.2 Export format and semantics

`Export` produces one coherent entity image and its Version, not a namespace-wide atomic snapshot. Start from a strongly validated root. Hold the entity gate while producing a complete local snapshot, or retry cleanly if obsolete remote inputs are collected. Never emit a mixture of roots.

Use a standard-library `archive/tar` stream containing **exactly** `manifest.json` and `snapshot.ltx`, in that order. No symlinks, directories, absolute paths, duplicate entries or extra trailing records beyond valid tar termination are accepted. Manifest format 1 records namespace/key/incarnation for provenance, exported sequence/schema, codec, page size/count, LTX checksum, SHA-256 and encoded length. It is bounded to 64 KiB. Snapshot bytes are bounded by the import limit.

A partial write, absent entry, mismatched digest, or incomplete tar termination is not a backup. The exporter returns an error and the caller must not publish that artifact as complete. Copy the completed export into independently retained storage before considering it a backup.

For many entities, publish a backup catalog **last**, after every named export has been verified and durably copied. Each entry carries its own Version. The catalog is not a claim that every entity was frozen at one common instant. A raw copy of a live namespace without pinning/quiescence can race GC and is not a valid backup procedure.

### 10.3 Import and restoration

`Import` creates a **new destination key/incarnation** through the creation protocol. Validate the entire archive and SQLite image before publication. The request's hash must bind the archive digest and requested schema. Existing destination keys are never overwritten.

Preserve application rows, schema, receipt expiration floor and historical receipts. Rebase engine identity/provenance to the new incarnation, and record the import operation as a new logical sequence after the exported sequence. Recompute a complete snapshot and checksums after any metadata change. The new Ref prevents old in-flight requests carrying the prior incarnation from targeting it accidentally. Import is an explicit clone/restore branch, not transparent in-place time travel: application rows that themselves contain full old Refs need a caller-controlled remapping/migration. A namespace restore tool emits an old-to-new Ref map but must not rewrite arbitrary application rows speculatively. Persisting logical keys and resolving current Refs at an application boundary avoids some of that coupling. Normal cache-loss recovery from the current S3 namespace preserves incarnations and needs no such remapping.

An old backup intentionally excludes changes acknowledged after its captured Version. Report that recovery point explicitly. It is not a violation of compute-failure RPO=0 to restore an older backup deliberately, but presenting it as the latest history would be false.

To recover a complete namespace, restore into a fresh prefix/namespace and redirect callers only after validation. Revoke old writer credentials or keep the old namespace physically isolated. Never point an existing head backward to an older sequence, reuse an old ETag/nonce, or resurrect old object paths as a rollback technique. A late writer to the old prefix must be incapable of publishing into the restored namespace.

### 10.4 Administrative hazards

Missing head, mixed-incarnation data, unknown protocol, changed namespace descriptor, or unexplained checksum divergence stops service for that entity. Diagnostics should preserve artifacts and identify the violated invariant, without logging SQL values or credentials.

A malicious or mistaken administrator with unrestricted write access can still replace all metadata and data consistently. Preventing or proving that historical rollback needs protection outside this library's stated authority model. IAM restrictions, independent exports and optional object retention are operational safeguards, not an invented in-library solution.

## 11. Scope boundaries, limits, and observability

### 11.1 Why durable timers are not in the initial core

A correct durable timer requires more than a timestamp in an entity's SQLite file. After all nodes lose their caches, some durable index must reveal which dormant entities are due. Updating that index and entity state is a multi-object protocol with orphan handling, ambiguous writes, claims, retries, completion and collection. A timer callback also cannot promise exactly-once external effects.

A full namespace scan is a possible discovery strategy, but its cost is proportional to the number of entities per scan and it supplies no tight wake latency without continuous infrastructure. It is not silently included in this minimalist module.

The core therefore persists caller-owned deadline fields like any other data but **does not discover or invoke sleeping entities**. Local eviction timers and best-effort maintenance scheduling are not durable application alarms. An application needing guaranteed wakeups must explicitly provide that capability or wait for a separately specified extension. The library alone is consequently not a drop-in replacement for a runtime's durable alarms.

A future optional extension must prove crash-safe discoverability before it enters the code budget. It cannot piggyback on an uncommitted S3 index write or claim that a local `time.Timer` survives eviction.

### 11.2 Limits contract

`Limits` is a plain struct, not a functional-options framework. Zero values select documented defaults. Reject negative, overflowing or mutually inconsistent settings. Keep protocol-format hard limits nonconfigurable.

| Field/capability | Initial default | Qualified upper bound or rule |
|---|---:|---|
| EntityBytes | 64 MiB | 256 MiB; larger requires separate qualification |
| ResultBytes | 16 KiB | 64 KiB; receipts count against entity size |
| SQL value/bound-argument bytes | 1 MiB | Not a document-storage API; internal control statements have separate fixed bounds |
| HeadBytes / DeltaReferences | 64 KiB / 32 | Fixed format-1 limits |
| ResidentEntities | 256 | Resource gate, not a namespace entity-count limit |
| CacheBytes | 8 GiB | Includes databases, WALs, downloads and staged objects |
| InflightStorage | 64 | Shared across reads, writes, recovery and maintenance |
| PendingPerEntity | 64 | Reject overflow; never an unbounded mailbox |
| Recoveries | 8 | At most four parallel object downloads each |
| BatchMax / BatchDelay | 1 / 0 | Opt-in qualified settings: 16 / at most 2 ms |
| StorageAttemptTimeout | 2 seconds for control operations | Bounded larger-object upload/read timeouts based on size; total request deadline still applies |
| CommitTimeout | 10 seconds | Resolution can outlive caller wait; no new speculative publication until settled |
| IdleTimeout | 60 seconds | Eviction of clean caches; not an ownership lease |
| ReceiptRetention | 24 hours | Minimum 1 hour; changes may not move the durable floor backward |
| GC objects per invocation | 1,000 | Resumable bounded passes; no unlimited list buffering |

A single large SQL statement may temporarily expand a WAL before control returns. Page-count limits and argument limits are not hard filesystem quotas. Reserve staging space, limit concurrent expensive operations, monitor file growth, cancel through the driver, and fail safely on ENOSPC. A hard per-process RSS/disk ceiling ultimately uses an OS/cgroup/filesystem quota, supplied by the host; it does not justify a custom VFS or hidden storage daemon.

Do not accept a configuration whose maximum database plus required recovery/staging headroom cannot fit in its cache budget. Eviction may wait for pinned entities; if all capacity is pinned, reject new activation rather than evict an in-use database. No cold entity needs a persistent goroutine or an in-memory entry after eviction.

### 11.3 Minimal Stats and logging

Expose a snapshot of aggregate counters/gauges through `Stats()`, using typed fields and standard-library atomics/locks. Required counters: admitted/rejected reads and mutations, committed/replayed/expired/conflicting IDs, unknown outcomes, owner changes, activation attempts, corruptions, S3 physical attempts by operation/result, bytes uploaded/downloaded, snapshot/compaction/GC counts and bytes. Required gauges: resident entities, queued requests, inflight storage, cache/staging bytes, oldest unresolved publication age.

Do not import a metrics collector merely to expose counters. Callers can export Stats to their own system. No per-entity high-cardinality metric registry. The qualification harness records latency distributions and process/cgroup CPU, RSS, file descriptors, disk and network separately.

Structured logs use `slog` and stable error codes, hashed entity IDs, owner session/epoch, and operation IDs where appropriate. Do not log SQL values, result payloads, credentials, presigned URLs or raw provider error bodies. Rate-limit repeated diagnostics. Use TLS verification through the SDK; untrusted proxy URLs and accidental public cache directories are configuration errors.

### 11.4 Exact configuration, error and Stats shapes

Freeze these data shapes with the API at M1. Fixed protocol limits from §11.2 are not additional tuning fields.

```go
type Limits struct {
    EntityBytes, CacheBytes int64
    ResultBytes, ResidentEntities, InflightStorage int
    PendingPerEntity, Recoveries, BatchMax int
    BatchDelay, StorageAttemptTimeout, CommitTimeout time.Duration
    IdleTimeout, ReceiptRetention time.Duration
}

type Code string // sixteen named constants with the exact values in §4.4

type Error struct {
    Code       Code
    Operation  MutationID
    Route      *Route        // nil if unavailable/not relevant
    At         Version       // validated observation point, if available
    RetryAfter time.Duration // hint, never permission to change operation identity
    Message    string        // bounded, sanitized explanation
    cause      error         // private; context cause or sanitized lower-layer error
}
func (*Error) Error() string
func (*Error) Unwrap() error

type Stats struct {
    Reads, Mutations, Commits, Replays, Expired, Conflicts uint64
    ReadRejections, MutationRejections, UnknownOutcomes uint64
    OwnerChanges, Activations, ActivationFailures, Corruptions uint64
    CASAttempts, CASConflicts, AmbiguousPuts uint64
    Snapshots, Compactions, Collections, CollectedObjects uint64
    SnapshotBytes, CompactionInputBytes, CompactionOutputBytes uint64
    CollectedBytes, UploadedBytes, DownloadedBytes uint64
    ResidentEntities, QueuedRequests, InflightStorage int64
    CacheBytes, StagingBytes int64
    OldestUnresolved time.Duration
    Storage [4]struct {
        Attempts, Failures uint64
        UploadedBytes, DownloadedBytes uint64
    } // fixed order: GET, PUT (including CAS), LIST, DELETE
}
```

Stats is a copied aggregate snapshot; it has no live maps/pointers that callers may mutate. Non-atomic relationships between counters are acceptable for telemetry, never for correctness decisions. The private qualifier additionally records operation-specific latency and exact storage traces.

An all-zero request hash or malformed/zero MutationID is rejected. Result bytes are copied before they escape the callback. Public methods validate contexts and limits rather than panicking on ordinary bad input. `Error` supports `errors.As`; its cause preserves appropriate context cancellation/deadline identity without dumping a provider response body. Callback errors are not silently relabeled as successful outcomes.

A zero Limits value selects safe defaults. BatchDelay remains zero when BatchMax is one; a nonzero BatchMax greater than one defaults its zero delay to the qualified 2 ms cap. The API does not accept a negative timeout to mean infinity. Application-owned memory retained by arbitrary Go callback closures is outside the library's allocator control; admission bounds how many such requests the library retains, and benchmarks use explicitly bounded callbacks.

## 12. Minimalism, purity, and artifact budgets

### 12.1 Production source allocation

The following is a budget, not permission to create empty files. Combine or split only for coherent responsibilities. Count all runtime-reachable project Go, including attributed extractions and code hidden under `internal`, examples used as runtime helpers, or generated project code. Tests and an independent qualification executable are counted separately, but they cannot contain implementation required by consumers.

| File | Responsibility | Budget: nonblank, noncomment Go lines |
|---|---|---:|
| api.go | Public types and scoped facade | 160 |
| node.go | Lifecycle and admission | 300 |
| errors.go | Stable errors and outcome classification | 100 |
| routing.go | Preferred placement and owner transitions | 180 |
| entity.go | Entity state/gate and activation orchestration | 360 |
| read.go | Strong reads and result barriers | 180 |
| write.go | Mutation/batch publication | 450 |
| receipt.go | Deduplication and expiration floor | 230 |
| migration.go | Explicit schema transition | 180 |
| sqlite.go | Engine setup/checkpoints and metadata | 330 |
| sqlscope.go | Authorizer and scoped SQL access | 240 |
| wal.go | Narrow attributed WAL capture/parser | 350 |
| delta.go | LTX encoding/validation adapter | 300 |
| restore.go | Bounded image reconstruction | 260 |
| head.go | Strict head codec and invariants | 280 |
| create.go | Birth/genesis protocol | 170 |
| store.go | Private storage seam and types | 120 |
| s3.go | SDK adapter and exact-attempt behavior | 240 |
| compact.go | Fanout merges and snapshot publication | 280 |
| gc.go | Revision-safe collection | 220 |
| cache.go | Bounded cache/pin/eviction policy | 220 |
| backup.go | Strict export/import | 280 |
| stats.go | Aggregate observability | 140 |
| limits.go | Configuration validation | 100 |
| doc.go | Package contract/examples metadata | 60 |
| **Total** | **25 production files** | **5,730** |

The 270-line remainder is contingency, not an invitation to omit validation. At each milestone, print actual and projected totals. If correctness needs more than 6,000 lines, first remove a nonessential capability or duplication. If that is insufficient, submit an evidence-based budget amendment showing the invariant, tests and measured benefit. Do not compress unrelated logic onto one line, hide implementation in tests, or remove checks to satisfy a counter.

File sizes above approximately 500 executable lines and functions above approximately 20 cyclomatic branches trigger review, not automated code contortions. An immutable protocol switch can justify complexity; a generalized repository/strategy framework cannot justify itself merely by being reusable.

### 12.2 API and dependency budgets

Target at most 18 exported types, 30 exported functions/methods, and the 16 listed error codes. Exported fields are documented configuration/data contracts, not a loophole for a second API. A public API snapshot generated from `go/types` must detect additions and signature changes in CI.

Runtime direct modules: at most five, as listed in §1.3. Target at most 30 modules in the transitive **runtime import closure**, reported separately from the module graph including tests/tools. The full module graph, licenses and checksums remain visible. Every addition must identify the invariant or measured capability it supplies. No framework, RPC stack, metrics server, cloud multi-provider facade, general scheduler or dependency-injection container is allowed by default.

Ordinary consumer builds and tests must work with `CGO_ENABLED=0`, no compiler toolchain outside Go, no native shared objects, no downloaded runtime artifacts, no embedded WASM engine, and no generated service. Translated Go in the SQLite dependency is permitted; a C ABI is not. Do not count third-party SQLite source as project-owned LOC, but do count its dependency, binary and memory impact.

### 12.3 Measurements that CI must generate

Implement a standard-library AST/import-graph checker in the qualification tools. It must report:

- Production source files, executable physical lines, AST statement count and function complexity. Test-only exclusions are verified against consumer imports.
- Exported types/functions/methods/constants and an API snapshot diff.
- `go list -deps -json` runtime package/module closure, `CgoFiles`, non-Go/native artifacts, licenses, and module checksums.
- Minimal consumer binary sizes on Linux amd64 and arm64: stripped target at most 35 MiB, review gate above 45 MiB; report unstripped size too. No executable compression to hide size.
- Empty-process RSS target at most 64 MiB; active fixture targets in §18. Report Go heap **and** RSS, plus cgroup memory including filesystem cache, because they are not interchangeable. Performance/RSS gates use ordinary binaries, not race-instrumented binaries.
- Reproducible builds from two clean directories with the same pinned Go toolchain, `GOTOOLCHAIN=local`, verified module sums, `-trimpath -buildvcs=false` and fixed build flags. SHA-256 of equivalent binaries must match. Record architecture/OS and flags.
- Offline consumer build from a prefetched verified module cache, vulnerability scan with a pinned Go tool, and license/attribution output. No live code-generation service is permitted in this process.

**Race-detector exception is only tooling:** Go's race detector requires cgo and a C compiler for its instrumentation runtime. Therefore run the normal/purity suite with `CGO_ENABLED=0`, and a separate `CGO_ENABLED=1 go test -race` lane. The latter does not permit a cgo SQLite implementation or cgo production dependency; inspect the import closure in both lanes and require the same project source path. Do not write an impossible `CGO_ENABLED=0 go test -race` acceptance command. [S27]

## 13. Autonomous-agent workstreams and integration discipline

Assign responsibilities by invariant rather than by a list of features to maximize concurrently.

| Workstream | Owns | Must not change unilaterally | Independent verifier |
|---|---|---|---|
| A — protocol and integration | Head/birth schemas, state-machine model, error/ack semantics, API freeze, integration decisions | Safety assertions or performance gates to accommodate a failing implementation | E; another agent reviews the model counterexamples |
| B — SQLite and LTX | Driver setup, scoped SQL, WAL capture, LTX adapter, replay oracle, schema transactions | Ownership/publication semantics; codec flags disabling validation | A and E using independently generated SQLite images |
| C — S3 publication and ownership | SDK adapter, exact attempts, write/read publication, takeover and ambiguity resolution | Receipt semantics, manifest invariants, GC no-resurrection rule | A and E with adversarial store/proxy |
| D — maintenance and bounded resources | Cache, compaction, GC, export/import, resource accounting | Published history, reintroduction of retired objects, import incarnation rules | B for page correctness; E for crash/interleaving tests |
| E — adversarial qualification | Reference model, external history oracle, subprocess harness, remote endpoint probes, stress/cost/footprint reports | Production implementation solely to make a test pass | A reviews harness soundness; B/C/D fix implementation |

M0 and M1 precede speculative runtime development. After the head/API model is frozen, B and C can proceed in parallel against explicit fixtures. D begins against immutable validated manifests, not an evolving internal representation. E starts with model and negative controls before any production path passes.

Every integration checkpoint includes: exact commit hashes, artifact format version, expected source budget, tests actually run, failures, pending/unexecuted checks, and the decision to proceed. “Tests written” and “tests passed” are separate fields.

An implementation agent cannot remove an assertion, broaden an allowed outcome, increase a timeout/limit, or skip remote qualification to turn red into green without an explicit protocol/target amendment reviewed by another agent. The qualification agent must retain counterexamples as permanent fixtures. Agents may fix a wrong test, but must show why it was wrong against the contract, independently of the code under test.

Do not carry two production persistence engines, two coordination strategies or an abstraction for a hypothetical future backend. Full-snapshot persistence remains a test/benchmark baseline. Optional batching may be left off; safety cannot.

## 14. Milestones, deliverables, and stop/go decisions

### M0 — Reproduce the research and qualify the dependency surface

**Deliver:** `research.lock.json` with source repository/commit/blob hashes, inspected paths, licenses, exact module/toolchain pins, embedded SQLite version/compile flags, and a short reusable-code inventory. Include a minimal consumer that opens SQLite, authorizes SQL, commits WAL transactions, and encodes/decodes LTX using `CGO_ENABLED=0`. Confirm modernc's authorizer API exists in the selected pin and does not require global hooks affecting other consumers.

**Verify:** E rebuilds in clean Linux amd64 and arm64 environments; checks module sums, licenses, native-artifact/import scans, source size and binary footprint; demonstrates `RETURNING`, context cancellation and authorizer transaction control. Recheck actual bundled SQLite WAL-reset fix. Verify the pinned LTX compactor, not only default-branch documentation.

**Pass:** no native runtime/build dependency; SQL boundary enforceable; LTX strict checks work; exact pins reproducible. Dependency budgets and initial binary review gate pass or have an evidence-based amendment.

**Failure action:** stop before building a distributed protocol on unverified engine behavior. Select another qualified pure-Go SQLite pin if necessary. Do not substitute libSQL/cgo, shell out to sqlite3, or introduce a runtime sidecar. If the authorizer/capture boundary cannot be made correct, redesign that boundary and update the contract.

### M1 — Prove the publication model and freeze contracts

**Deliver:** executable finite-state model of immutable upload, head CAS, ownership transfer, delayed/lost replies, receipts, expiration, compaction and collection. Include the short safety proof, machine-readable head/birth/export schemas, errors and public API snapshot. Define all failpoints and the history record format before writing the happy path.

**Verify:** E explores all schedules within bounded configurations of two/three nodes, two entities and up to three concurrent mutations, then at least one million seeded randomized schedules. Use an independent history checker. Inject intentionally broken variants: unconditional head write, separate-owner check followed by publication, cached strong reads, premature acknowledgment, floorless receipt pruning, and age-only GC. Each negative control must produce a detected counterexample.

For reproducibility, initialize the finite models with these bounded cases: (P) two nodes/one entity/two mutations/one takeover/four messages in flight, schedule depth 32; (O) three nodes/one entity/three mutations/two takeovers/eight messages, depth 64; (G) two nodes/one entity with an initial two-delta root, one compaction, one mutation and one collector, depth 64; (I) two IDs, one floor advancement and a delayed retry. Input values and schema versions are drawn from a finite set of three values. Enumerate enabled transitions with canonical-state deduplication, independent apply/reply delivery and crash transitions. Record explored/pruned states and bounds. Hitting a state/time budget is `inconclusive`, never “exhaustive passed.” The million random traces supplement these cases with larger schedules; they do not replace them.

**Pass:** zero invariant violations in the correct model, all broken variants detected, no reliance on clock accuracy for safety, and a reviewed argument covering schedules larger than the finite test bounds.

**Failure action:** redesign the protocol, not its checker. No production publication code is accepted until the model and contracts converge. Model exploration is evidence, not a mathematical proof of every implementation behavior.

### M2 — Single-process SQLite capture and replay

**Deliver:** exclusively managed connection wrapper, authorizer, bounded WAL reader, LTX encode/replay, independent snapshot oracle, fuzz corpora and SQL differential fixtures. Include initialization, growth/shrink, BLOB limits, indexes, triggers, transactional DDL, rollback, failed constraints, row-returning writes and checkpoint boundaries. Unsupported extensions/features must be explicitly refused or excluded from compatibility claims.

**Verify:** B's output is replayed by E into fresh files and compared against SQLite's own completed image and integrity checks. At least 100,000 randomized transactions across multiple sizes, plus bounded parser fuzzing and SQLite error injection. Reuse attributable upstream golden fixtures where licenses permit, but include independent fixtures and malformed input tests.

**Pass:** byte/page equivalence and logical results match; no lost committed frames, no uncommitted tail applied; ordinary mutations capture changed pages rather than scan the database; capture memory scales with bounded changed pages, not namespace size.

**Failure action:** keep a full-snapshot reference path to isolate the bug. Do not promote it silently as the final efficient implementation. Simplify checkpoint policy before inventing a VFS. Change codec only with measured, reviewed evidence.

### M3 — Storage adapter and real conditional-write behavior

**Deliver:** minimal S3 adapter, explicit retry/timeout classification, instrumented local store/proxy, startup diagnostic and remote endpoint probe command. The startup diagnostic refuses service when its required conditions cannot be established; an inconclusive probe is not a successful qualification.

**Verify:** E executes conditional create/update races, stale ETag tests, overwrite visibility, truncated/corrupt bodies, 409/412/403/5xx, response loss after application and delayed application after client cancellation. Verify physical SDK attempt counts. Repeat against a real remote regional endpoint, under an approved test prefix.

**Pass:** one winner per contested token; every failure classified correctly; broken-condition stores rejected; no hidden CAS retries; all required remote tests actually executed.

**Failure action:** mark the endpoint unsupported or blocked. Do not add Redis/Consul to cover a broken object-store contract or call a fake-store pass portable production qualification.

### M4 — Reference unbatched durable entity

**Deliver:** Create, Update, Read, Lookup and basic recovery with `BatchMax=1`; external acknowledgment oracle; process-kill failpoints at every commit boundary; safe missing-head behavior; bounded receipt expiration.

**Verify:** E kills the process before/after every SQL, upload and CAS transition, then restarts with an empty cache. Compare every observed success with recovered state/receipt, allowing properly classified unknown operations to have committed. Test repeated IDs with both matching and different inputs.

**Pass:** zero lost acknowledged mutations, zero duplicated committed IDs, no speculative data exposure, no empty reinitialization of existing state, and no permanent dependency on local files.

**Failure action:** stop performance work. A corruption or lost-ack failure invalidates the milestone even if throughput is excellent. Reduce concurrency/features until the invariant is understood.

### M5 — Multi-node ownership, routing and split-brain resistance

**Deliver:** Resolve/Preferred/Takeover/Release, three independently hosted node processes, caller-side generic test transport, owner pause/termination/handoff scenarios, ambiguous CAS fencing.

**Verify:** E runs simultaneous candidates, false failure detection, resumed stale owners, pending old requests, membership reorder and full-node replacement with fresh sessions. Validate data history independently of which node reports ownership.

**Pass:** a single published lineage, preservation of every earlier acknowledged mutation, strong-read linearizability, successful recovery after stable connectivity returns, and no safety dependence on clock synchronization.

**Failure action:** inspect the shared head boundary before adding leases/heartbeats. Do not mask split-brain by assuming the old node stops executing after a timeout.

### M6 — Long-lived resource and storage correctness

**Deliver:** bounded cache, fanout-eight compaction, safe collection, migrations, strict export/import, namespace recovery recipe and resource metrics. Integrate all metadata publications with ambiguity resolution.

**Verify:** D's compaction/replay is checked by B; E pauses compactor/collector/exporter across takeovers and corrupts each retained object type. Run a dataset larger than the entire local cache. Restore exports on machines with no original data directories and with old writers still attempting to access their old namespace.

**Pass:** every current root remains reconstructible; collection cannot remove future-publishable objects; local disk/RSS remain within the measured envelope; schema/format rollback fails closed; backup recovery points are exact and honestly labeled.

**Failure action:** simplify to serialized per-entity maintenance. If collection proof fails, disable deletion while repairing it and report the temporary storage growth; do not release an indefinitely leaking design as complete.

### M7 — Optimize only proven bottlenecks

**Deliver:** bounded per-entity batching and read coalescing, or an explicit decision to retain the reference behavior. Measure each change against M4/M6 using identical seeds, hardware, payloads and durability. Re-run all safety models and fault tests with batching both enabled and disabled.

**Verify:** E checks errors/reads following speculative earlier members, cancellation mid-batch, partial callback failures, result ordering and unknown outcome resolution. B confirms final page equivalence.

**Pass:** safety unchanged; measured benefit in the target workload exceeds noise; source/dependency budgets remain defensible. Publish actual batch distributions, not configured maxima.

**Failure action:** remove the optimization. A simple correct slower path is preferred to a fast unproven path; performance shortfalls remain visible for M8.

### M8 — Production-like qualification and cost evidence

**Deliver:** complete local and real-remote qualification matrix, sustained stress/soak histories, p50/p95/p99 and error-rate reports, CPU/RSS/disk/S3 accounting, linearizability and acknowledgment checks, repeatable baseline comparisons, and cost outputs at increasing scales.

**Verify:** E reruns from a clean checkout without author guidance, with at least three remote hosts and independently retained client history. A reviews raw histories and any excluded runs. All candidate gates in §18 receive pass/fail/blocked statuses.

**Pass:** every safety gate passes; required endpoint tests and soak are executed; resource and performance gates pass on the stated environment or have an explicit justified target amendment. Provider outages are reported, not filtered out without explanation.

**Failure action:** classify the bottleneck: implementation, provider latency/quota, entity hotspot, footprint, or impossible target. Simplify/redesign the responsible mechanism. Do not weaken durability to pass a latency gate or equate an unexecuted remote run with success.

### M9 — Minimize and release the library

**Deliver:** final source/API/dependency inventory; reproducible minimal consumer; package docs; no application-specific concepts; operational limits and provider qualification matrix; migration/rollback policy; license notices; release evidence manifest with artifact hashes.

**Verify:** another agent builds and uses the library in two unrelated small generic examples, such as a counter and a key/value registry, without changing the library or introducing a service sidecar. E independently verifies all mandatory artifacts and that no correctness-critical logic lives only in tests.

**Pass:** the release acceptance checklist in §22 is complete. No “production-ready” label without remote safety/recovery evidence and the declared limits.

**Failure action:** do not release. Remove speculative abstractions and unused features; preserve all checks and evidence. A budget amendment must name what correctness or measured capability justifies the excess.

## 15. Verification architecture and independent oracles

### 15.1 Three different storage test layers

**Deterministic reference store:** a small test-only linearizable map with explicitly scheduled apply and reply events, opaque ETags, controllable visibility/failures and a persistent operation journal. It enables exhaustive interleavings and negative controls. This is not evidence that a real S3 endpoint has those properties.

**Instrumented local S3-compatible store:** run a pinned actual server with a Go fault proxy and test credentials. Resolve the server image digest in M0; do not use an unreviewed `latest`. The inspected CellD documentation identifies a conditional-create regression in one historical MinIO release and a subsequent corrected release, so the probe must run regardless of the chosen brand/version. These are test fixtures, not required runtime services. [S01]

**Real remote S3 endpoint:** initially a dedicated Scaleway regional Multi-AZ test bucket/prefix with three compute hosts in the selected region. Verify the actual endpoint, signing configuration, SDK version and permissions. A second independently implemented S3 service is required before advertising broad provider portability rather than “qualified on the listed configurations.” No external coordination database is added.

Remote credentials, endpoint, bucket and an explicit permission to write/delete the disposable test prefix are supplied by environment/configuration. If absent, required remote commands exit nonzero with `blocked_missing_remote_configuration`; they never pass by skipping. Do not use a production data prefix for qualification.

The proxy must preserve signed request semantics while delaying or dropping transport events. Tests that mutate payload bytes after signing must distinguish a provider's signature rejection from the library's response-body corruption detection. For late-apply tests, keep the server-side attempt alive after client cancellation. A mock that simply cancels every delayed operation with its client cannot exercise the dangerous window.

### 15.2 Histories and safety checks

A separate driver records invocation, stable request ID/hash, response, response classification, entity/incarnation, returned sequence/result, node/session and monotonic timestamps. It persists that log independently of nodes that will be destroyed. An acknowledged result is one **observed by the driver**, not a server log line saying a response was attempted.

The generic workload writes an application ledger entry keyed by operation ID in the same transaction as a counter/map mutation. After all nodes are terminated and caches deleted, a fresh deployment reconstructs state from S3. Every driver-observed success must appear exactly once with the correct result in the test ledger. Library receipt retrieval is required only within its declared retention window; the test ledger/oracle must not mistake intentionally expired result retention for loss of the application effect. An unknown operation may be present or absent, but cannot appear twice or conflict with its receipt. Do not require the recovered counter to equal only known successes when unknown operations legitimately committed.

Use a per-entity sequential model for reads, updates, duplicates, failed preconditions and migrations. Porcupine is a suitable test-only Go checker; partition large histories by entity and check bounded windows with retained boundary state. Its timeout/inconclusive result is not a pass. For very large histories, combine complete specialized ledger checks with fully checked sampled/adversarial windows; disclose coverage. [S28]

The store trace independently checks CAS winners and reachability. Do not use production `head.go`, `gc.go` or receipt implementation as the sole oracle for the same invariant. An independent exported-image checker validates SQLite state and manifest integrity without trusting the original node's in-memory assertions.

### 15.3 Test classes

- Unit tests for strict decoders, paths, overflow, schemas, errors, authorizer modes, checksum adapters, placement determinism and queue/resource accounting.
- Property tests generating SQL changes, transaction failures, grow/shrink patterns, repeated IDs, schema transitions, compaction groupings and collect/takeover schedules.
- Fuzz targets for head/birth/export parsing, WAL headers/frames/commit boundaries, LTX adapter/replay and malformed SQL scope use. Each parser has size/time limits; decompression bombs must fail within them.
- Concurrency and race tests for simultaneous same-entity calls, independent entities, Close/Evict/Read races, context cancellation, storage callbacks and late replies.
- Real subprocess tests using SIGKILL and SIGSTOP/SIGCONT, not only cancellation of goroutines. Destroy data directories between selected runs.
- Long-running stress with open-loop offered load, overload, cold churn, uneven key popularity, takeover and maintenance. Test overload recovery as well as steady success.

Run at least 30 minutes per fuzz target in a release qualification campaign, with persistent seed corpora and all discovered failures promoted to deterministic regressions. PR CI may use shorter bounded runs but cannot replace the release campaign.

## 16. Mandatory fault matrix

Each row is a release test family with reproducible seeds and an external acknowledgment/history check. Run the bounded deterministic version locally and the supported transport/process variant remotely. A safety failure is fatal regardless of how rare it is.

| Fault/window | Required observation |
|---|---|
| Kill before/inside SQL transaction, after local COMMIT, during LTX capture | No success may be observed without a published root; a fresh node ignores speculative local residue. |
| Kill before PUT, mid-body, after object application but before response | No partial artifact becomes reachable; ambiguity is retried/resolved on the same key. |
| Kill before head CAS, after CAS application, after response but before caller acknowledgment | Recovered state includes every observed success; an unobserved operation may legitimately be committed once. |
| Kill immediately after observed success and delete every node/cache disk | All acknowledged effects/results are recoverable solely from S3. |
| Store applies a request after its client deadline or after client disconnect | A timeout never implies nonapplication; later fence/takeover prevents stale publication. |
| CAS succeeds, reply is lost, SDK would otherwise retry and receive 412 | One physical head attempt is visible; resolution finds the successful receipt, never double-applies. |
| Two/32 simultaneous create or takeover candidates | One conditional winner per head version; no silently reinitialized key or competing authoritative branch. |
| SIGSTOP owner, take over elsewhere, commit new mutations, then SIGCONT old owner | Old SQL/upload may finish, but its CAS fails; strong reads from it cannot expose stale/unpublished state as current. |
| Old commit wins immediately before takeover | Takeover rereads and retains that commit; it cannot restore an earlier snapshot cut. |
| Same operation ID reaches several nodes with same/different hashes | At most one committed effect; exact replay or `id_conflict`, including after restart. |
| Receipt pruning races delayed retries; clocks jump ±1 hour | Durable expiration floor never retreats; no expired operation is interpreted as new. Availability differences are reported. |
| Read begins after a completed write on another owner | It sees that write or a later state, never a cached earlier head. |
| Read/error follows an unpublished earlier batch member | Output waits for publication or becomes an infrastructure failure; no speculative authoritative result escapes. |
| Context canceled mid-batch; one callback errors; another succeeds | Successful receipts and batch ordering remain coherent; canceled clients receive an accurate uncertainty classification. |
| Compactor paused before/after upload or CAS across takeover | Old inputs remain usable until replacement publication; stale output cannot overwrite or be retargeted. |
| Collector pauses after reading head, ownership changes, new objects publish | Revision-based deletion cannot remove the successor's current/future-valid objects. |
| Stale writer uploads after its orphan was deleted | Only an unreachable orphan reappears; subsequent collection can remove it. |
| Restore/export races collection of an older root | Retry from a current root or fail explicitly; never mix images. |
| Header/length/digest/checksum corrupted; LTX duplicated/reordered; a referenced object missing | Bounded detection and fail-closed recovery; no skipped range or empty fallback. |
| Compressed object expands beyond configured limits | Abort without excessive allocation/disk consumption. |
| WAL has partial header/frame, wrong salt/checksum, valid uncommitted tail, duplicate pages, shrink/regrow | Only complete committed changes publish; expected missing commits are errors. |
| SQL caller attempts COMMIT, ATTACH, storage PRAGMA, reserved-table write, escaping cursor | Authorizer/scope boundary rejects the operation; no bypass through comments/multiple statements/triggers. |
| ENOSPC/EIO during WAL, staging, checkpoint, replay, rename or export | Unpublished work fails safely; already published work remains successful and recoverable after cache replacement. |
| Head missing beside birth/data; namespace descriptor missing beside existing data | Service refuses initialization/activation. No automatic empty state. |
| Wrong incarnation/namespace/key, unknown codec/schema, downgraded binary | Reject before mutation/claim; older code cannot silently write upgraded state. |
| Import old backup while old nodes still run | New namespace/incarnation isolates stale requests; recovery point is the recorded backup Version, not “latest.” |
| Store ignores conditions or serves provably stale overwritten heads | Qualification/startup rejects the provider; the module does not claim correctness on it. |
| S3 unavailable for 60 seconds; all queues reach limits | Bounded memory/disk/goroutines and explicit backpressure; no local-durable success. Recovery resumes after availability returns. |
| Repeated failovers and node replacement under load | No unbounded owner ping-pong retry loop; after stable routing, the system converges and backlog drains. |

For publication/crash windows, run at least 100 deterministic repetitions per failpoint locally and 20 remote repetitions for the remotely reproducible subset. For contention, execute at least 1,000 contested-token rounds with 32 contenders in the storage probe, then independently test entity-level histories. Do not confuse a single passing conditional write with sustained concurrent correctness.

## 17. Workloads and measurement environments

### 17.1 Environment lock

Record exact Go version, dependency sums, git commit, OS/kernel/container image digest, architecture, CPU model/allocation, RAM, local filesystem/disk, cache quota, `GOMAXPROCS`, GC settings, open-file limit, S3 endpoint/region/class, encryption settings, SDK retry configuration and run timestamps. Provider prices and quotas are dated inputs, including per-key as well as aggregate request limits. A provider cannot be qualified for a hot-entity rate above its own per-key write ceiling.

Reference application nodes: Linux, 4 allocated vCPUs, 8 GiB RAM, fixed 20 GiB or larger disposable local disk, `GOMAXPROCS=4`, library cache budget 8 GiB, library memory target at most 1 GiB. Run amd64 and arm64 compatibility/purity; perform the full performance matrix on at least one architecture and label it. Hosts must not share a CPU allocation while pretending to be independent capacity.

Local fixture: actual S3-compatible server on a separately budgeted host/process, plus controlled one-way delay injection. Run an unmodified low-latency profile and a profile with 25 ms per storage request. Report the store's resources too. It is permissible for a fault-injection machine to use non-Go tooling; this is not a consumer dependency.

Remote fixture: three distinct same-region application hosts and a separate load/history driver. Baseline endpoint PUT/GET latency and error rate before each campaign, including 4 KiB, 32 KiB, 1 MiB and 64 MiB objects and conditional PUTs. The workload must fit known request/bandwidth quotas. A regional network trip is measured, not assumed away.

### 17.2 Required profiles

| Profile | Dataset and operations | Purpose |
|---|---|---|
| Q0 — component baselines | Bare modernc local transaction; full-snapshot/CAS reference; direct SDK primitives | Separate SQLite, codec, storage and publication overhead. |
| Q1 — mixed state | 10,000 entities, initially 64 KiB each; 80% reads/20% mutations; 1 KiB logical updates; 256 concurrent clients | Common many-entity workload, bounded active cache, strong-read cost. |
| Q2 — write-heavy | 10,000 entities; 80% mutations/20% reads; 1 KiB and 16 KiB incompressible updates; uniform and Zipf popularity | Batching, amplification, contention and compaction. |
| Q3 — cold churn | 100,000 entities, 128 KiB each; working set larger than 8 GiB local cache; at least 1 million operations | Lazy loading, eviction and namespace-scale memory independence. |
| Q4 — hotspot fairness | One hot entity plus 999 independent entities; 64 concurrent hot callers plus independent traffic | Single-entity ceiling and isolation of unrelated entities. |
| Q5 — image/recovery | 64 KiB, 1 MiB, 64 MiB and 256 MiB images; 0/8/32 deltas; cold process and empty cache | Full restore, snapshot, compaction and size-boundary behavior. |
| Q6 — soak | Three nodes, at least 24 hours and 10 million logical operations; 70% reads/30% writes; bounded steady-state data | Leaks, long-run maintenance, receipt aging, repeated recoveries. |
| Q7 — failure waves | Q1/Q2 load with node kills, stale-owner pauses, 60-second storage outage and restore storm | Safety and recovery of admission/latency under stress. |

Report both initial creation cost and steady-state operations. Receipts are real production receipts in every profile, not disabled for benchmarks. Limit fixtures so they do not trivially exceed the declared maximum entity size; also run deliberate capacity-exhaustion cases and verify refusal.

Use fixed seeds and both compressible and incompressible data. Record offered load, achieved throughput, successes, duplicates, unknown outcomes, timeouts, rejections and errors. Do not silently drop overload requests. A failed deadline is not a fast completed operation.

For steady profiles: five-minute warmup, at least 30 measured minutes, three seeds/runs. Exercise both a paced open-loop generator and a bounded-concurrency generator; otherwise coordinated omission can hide overload. Report full distributions including queueing, not just SQL execution time. Separate healthy, injected-fault and recovery intervals while retaining the complete timeline.

### 17.3 Baselines that are fair

Implement the full-snapshot reference behind the **same** CAS/ack protocol in test tooling. Compare its cost and latency with LTX; it is a legitimate correctness/performance baseline, unlike asynchronous replication acknowledged early.

A CellD v0.6.1 comparison is optional unless the exact release and equivalent workload can be reproduced. Use bucket durability, record every relevant setting, match read/durability semantics, and report mismatches rather than normalizing them away. Fleet acknowledgment is a different storage guarantee. Do not reuse an old laptop result as a baseline for these machines.

Litestream asynchronous backup, its explicit synchronization path, and bare local SQLite can be reported as **different-contract** component baselines. They cannot be labeled faster substitutes for fenced S3-authoritative commits without implementing the missing boundary.

## 18. Candidate numerical acceptance gates

These are initial targets to test, **not measured results**. Correctness gates are absolute. Performance/footprint changes require a reviewed amendment with a reproducible reason, not silent movement after the first run.

### 18.1 Absolute safety/release gates

Zero lost driver-observed acknowledged mutations. Zero duplicated effects for an unexpired operation identity. Zero receipt/input-hash conflicts accepted. Zero invalid current-root chains. Zero stale-writer publications past a successful takeover fence. Zero strong-read linearizability violations in checked histories. Zero races in the required race campaign. Zero successful initialization from ambiguous missing existing state.

Every scheduled fault must either complete with a valid outcome or fail explicitly within its documented resource/deadline bounds. An unknown result is acceptable only in a genuinely ambiguous window and must be resolvable after stable connectivity returns, unless its result has expired under the documented contract.

### 18.2 Latency and throughput targets

| Metric | Local low-latency S3 fixture | Healthy real regional S3 target |
|---|---:|---:|
| Warm strong-read p50 / p95 / p99 | 5 / 15 / 30 ms | 80 / 180 / 400 ms |
| Warm B=1 mutation p50 / p95 / p99 | 15 / 40 / 100 ms | 180 / 450 / 1,000 ms |
| Cold activation, 1 MiB with 8 deltas, p95 / p99 | 0.5 / 1.5 s | 2 / 5 s |
| Cold activation, 64 MiB, p95 / p99 | 3 / 8 s | 15 / 30 s |
| Small-entity takeover-to-usable state, p95 / p99 | 2 / 5 s | 10 / 20 s |
| Q1 mixed throughput, three nodes | At least 5,000 logical operations/s | At least 2,000 logical operations/s |
| Q2 B=1 mutations, three nodes, 1 KiB logical updates | At least 1,500 successful mutations/s | At least 1,000 successful mutations/s |
| Three-node scale versus one, independent entities | At least 2.2× before store saturation | At least 2.2× before store/quota saturation |

For the healthy-remote latency gates, the concurrent direct-provider baseline should have small-object GET p95 at most 100 ms/p99 at most 250 ms, and PUT p95 at most 150 ms/p99 at most 500 ms. If it does not, mark the dependent absolute gate `provider_blocked` and still report achieved results and relative library overhead. That is not a qualification pass. Safety tests remain required under bad provider latency.

Under healthy offered load below measured saturation, unexpected error/timeout rate should be below 0.1%; report all unknown outcomes separately. No finite p99 target is promised under a deliberately stalled/unavailable store. In those tests, the gates are bounded admissions, correct uncertainty, and backlog recovery within two minutes after restored health for the small-entity fixture.

A single entity's sustainable rate is bounded by its serialized publication path. With one batch per two storage round trips, roughly `B / (PUT-object latency + CAS latency + CPU/checkpoint)` is a useful upper-bound estimate, not a guarantee. Independent entity scaling cannot remove this hotspot bound.

### 18.3 Resource and amplification gates

- Default small-state Q1/Q2: peak library consumer RSS target at most 512 MiB and review ceiling 1 GiB per node with 256 resident entities. Empty consumer target at most 64 MiB. Full-size Q5 cases report the explicit bounded replay/staging envelope separately.
- After all entities become idle and caches are evicted, Go heap returns near its steady baseline and RSS is within baseline plus 64 MiB after a controlled GC/scavenging observation period. Report allocator retention separately; do not falsify RSS as heap.
- Q6 steady-state memory growth in the final six hours: at most 5% after accounting for intentionally retained live data/cache. Goroutines and file descriptors must not grow with total historical entity count; default fixture target at most 1,024 goroutines and 2,500 descriptors.
- Cache files plus WAL/staging/downloads stay within the configured budget and declared transient reservation margin. Record peak overshoot. ENOSPC may reduce availability but cannot corrupt authoritative state.
- Fault-free warm B=1 normal publication: exactly one immutable PUT and one head CAS, excluding separately recorded maintenance. A strong read requires one head GET; an explicitly coalesced queued read group may share it. No ordinary read/write uses LIST to discover state.
- For the fixed Q1/Q2 schema and steady maintenance, target at most 2.5 successful PUTs per B=1 mutation, including amortized compaction/snapshot publication. Physical failed/retried attempts are reported separately and in billing totals. Batching denominators use observed batch size.
- Target average encoded fresh delta at most 32 KiB for the fixed 1 KiB update fixture; this is not a bound for arbitrary SQL. Report dirty-page bytes, receipt bytes and compression ratio independently.
- Target total maintenance-plus-foreground upload bytes at most 4× freshly changed page bytes on the fixed fixture. If large-state levels/entropy make that unattainable, report the measured asymptote and redesign/adjust with evidence rather than removing integrity checks.
- Capture CPU for an identical fixed-page update in a 64 MiB database should be no more than 3× that in a 64 KiB database, excluding explicitly measured full snapshots. This detects accidental full-database work on every commit.
- Required cold-activation reads are bounded by one manifest head plus one snapshot plus at most 32 deltas, with birth/ownership validation and any GC-race retries counted separately. Initial discovery, schema checks and integrity scans are included in elapsed latency.

## 19. Cost model and required financial outputs

### 19.1 Measure operations before pricing them

The qualification runner must output physical GET/HEAD/PUT/LIST/DELETE attempts, transferred bytes, encoded/decoded storage sizes, current and noncurrent object bytes, compaction rewrites, abandoned uploads, receipt growth, export/backup bytes and CPU seconds. SDK-internal attempts cannot be invisible to the accounting.

Define, for a workload interval:

```text
N = logical operations; W = new mutations; R = strong reads/receipt lookups
B = observed mean committed mutations per publication batch
U = W / B
P = 2*U + compaction PUTs + snapshot PUTs + ownership PUTs
    + creation PUTs + ambiguity-fence PUTs + failed/retried physical PUTs
G = read-validation GETs + activation GETs + compaction misses
    + resolution GETs + metadata probes + failed/retried physical reads
S = time-weighted live + retained delta + orphan + version-history + backup bytes
X = billable egress bytes under the selected region/provider arrangement
```

Creation is not free: birth, genesis snapshot and head require separate conditional publications, in addition to namespace/probe overhead. Dormant entity count determines storage; operation count alone does not. An independent backup is a separate retained copy, not a second charge for a provider's internal Multi-AZ replication.

The price function is:

```text
monthly storage = average GB-month * storage price
request charges = billing_round(P)*PUT rate + billing_round(G)*GET rate
                  + LIST/DELETE charges where applicable
transfer = billable X * egress price
compute = sum(node-hours * hourly node price)
total = storage + request charges + transfer + compute
        + local cache disks + network/IP/load-balancer charges + observability
        + independently priced backup transfer/storage + explicit contingency
```

Use decimal GB for provider billing and label MiB/GiB for binary resource limits. Apply each provider's rounding and free allowances explicitly; publish a no-free-credit comparison too. VAT/currency conversion, retention horizon and whether a monthly figure is initial or steady-state must be labeled.

Capacity planning uses the maximum of **measured** CPU, RSS, concurrency/latency, network throughput, per-key contention and provider-quota requirements, then adds failure headroom. It must not infer node capacity solely from CPU seconds while ignoring a saturated storage request pool.

### 19.2 Dated price anchors and provider independence

As a dated example, Scaleway's public storage page lists Multi-AZ object storage at €0.000022/GB-hour, or €0.01606/GB-month at 730 hours, requests included, and external egress at €0.01/GB under its published conditions. Its compute page lists a 4-vCPU/8-GB compute option at €0.1064/hour, excluding some ancillary resources. Recheck the exact region/class and current quote in qualification; these are price inputs, not performance evidence. The storage page also describes GB-hour rounding: verify the billing aggregation level from the provider's meter/contract before pricing millions of tiny short-lived objects. Until resolved, report both aggregate-rounding and per-object-minimum sensitivity bounds, rather than assuming a favorable interpretation. [S29, S30]

Also calculate a provider-neutral paid-request sensitivity with **hypothetical**, not quoted, prices of €5/million PUTs, €0.40/million GETs, storage €0.03/GB-month, and egress €0.09/GB. Include warm/cold cache and no-batching scenarios. A favorable provider with included requests is an optimization, not an architectural safety assumption.

### 19.3 Illustrative arithmetic per million operations

The following only checks the cost-model arithmetic. It is not a benchmark or a capacity recommendation.

Assume 80% reads/20% new writes, warm steady state, amortized PUT count `2.3*U`, maintenance GETs `1.2*U`, and additional cold-read cost `0.03*N`. The 2.3 coefficient roughly budgets fanout-eight compaction publication; measured snapshots/ambiguity must replace it. Then:

| Per 1 million logical operations | B=1 | B=8, only if actually achieved |
|---|---:|---:|
| Publication batches U | 200,000 | 25,000 |
| Modeled PUTs | 460,000 | 57,500 |
| Modeled GETs | 1,070,000 | 860,000 |
| Request charge at hypothetical paid-request rates | €2.728 | €0.6315 |

Storage and compute are not included in this small table. Strong reads are not free just because state is cached. A read-heavy workload can remain GET-dominated even after batching removes most write requests.

For scaling outputs, use N = 1M, 10M, 100M and 1B operations/month; vary write share 1%/20%/80%, achieved B=1/2/8, cold activation 0%/1%/20%, 64 KiB/1 MiB/64 MiB entity images, and 1,000/100,000/1,000,000 retained entities independently of N. Include a hot-key profile rather than assuming uniform partitioning. Produce CSV/JSON and a reproducible cost report; no spreadsheet service is required.

An additional CPU-only illustration may assume 5 ms/op, 4 vCPUs/node, 60% CPU target, peak factor 10, and one spare node, minimum two. These are **unverified assumptions**. For 1M/10M/100M/1B monthly operations, the CPU formula alone yields 2/2/2/10 nodes over a 30-day traffic month. It is merely a lower-bound illustration until the measured memory, S3 concurrency, quota and network constraints are applied.

Require the final report to show cost **per million new durable mutations**, **per million strong reads**, and **per million logical mixed operations** separately. Include empty/low-traffic deployment floor and takeover/restore-storm costs. Do not repeat a low state-storage number as the cost of an entire application with unrelated data and services.

## 20. Executable verification procedures to deliver

The commands below define the qualification interface the agents must implement. They are **not claims that these tools or results already exist**. One Go executable under `internal/qual/cmd/qual` is sufficient; it is not installed with the production library. Checked-in profiles contain the exact parameters from §§16–18. Missing required fixtures/configuration, inconclusive checks, and safety violations exit nonzero.

### 20.1 Reproducible local build and purity

```sh
set -eu
export GOTOOLCHAIN=local
export CGO_ENABLED=0
mkdir -p artifacts

go mod verify
go vet ./...
go test -count=1 ./...

go run ./internal/qual/cmd/qual footprint \
  --max-production-files=25 --max-production-loc=6000 \
  --max-direct-modules=5 --max-runtime-modules=30 \
  --max-exported-types=18 --max-exported-callables=30 \
  --out=artifacts/footprint.json

GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
  -ldflags='-s -w -buildid=' -o artifacts/consumer-amd64 ./internal/qual/consumer
GOOS=linux GOARCH=arm64 go build -trimpath -buildvcs=false \
  -ldflags='-s -w -buildid=' -o artifacts/consumer-arm64 ./internal/qual/consumer

go run ./internal/qual/cmd/qual reproducible \
  --clean-builds=2 --offline-second-build --out=artifacts/reproducible.json
```

The footprint command must fail on cgo/native/runtime artifacts in the consumer import closure, even when an accidental alternate build tag makes the local test binary pass. Build/test on the real target architectures as well as cross-compiling. Dependency vulnerabilities and licenses are captured by separately pinned tools, whose versions appear in the report.

### 20.2 Model, unit, fuzz and race lanes

```sh
CGO_ENABLED=0 go run ./internal/qual/cmd/qual model \
  --nodes=3 --entities=2 --concurrent-operations=3 \
  --exhaustive-bounded --random-traces=1000000 --seed=20261107 \
  --negative-controls=all --out=artifacts/model

CGO_ENABLED=0 go test -count=100 -run 'Test(Publication|Takeover|Receipts|GC)' ./...
CGO_ENABLED=0 go test -run '^$' -fuzz=FuzzHeadDecode -fuzztime=30m .
CGO_ENABLED=0 go test -run '^$' -fuzz=FuzzWAL -fuzztime=30m .
CGO_ENABLED=0 go test -run '^$' -fuzz=FuzzLTX -fuzztime=30m .
CGO_ENABLED=0 go test -run '^$' -fuzz=FuzzExport -fuzztime=30m .

# Go race instrumentation requires this separate tooling environment.
CGO_ENABLED=1 GORACE='halt_on_error=1' go test -race -count=10 ./...
```

Root fuzz target names are part of the harness contract; tests may call private internal helpers. Do not install test-only fault switches as public production options. Any build-tagged failpoints must preserve production control flow and receive independent review for realism.

### 20.3 Local S3 and subprocess faults

```sh
CGO_ENABLED=0 go run ./internal/qual/cmd/qual local-store \
  --lock=research.lock.json --profile=profiles/local.json --out=artifacts/local-store

CGO_ENABLED=0 go run ./internal/qual/cmd/qual doctor \
  --config=artifacts/local-store/endpoint.json \
  --cas-rounds=1000 --contenders=32 --out=artifacts/local-doctor

CGO_ENABLED=0 go run ./internal/qual/cmd/qual faults \
  --config=artifacts/local-store/endpoint.json --nodes=3 \
  --matrix=profiles/faults.json --repetitions=100 --empty-cache-restarts \
  --out=artifacts/local-faults

CGO_ENABLED=0 go run ./internal/qual/cmd/qual stress \
  --config=artifacts/local-store/endpoint.json --profiles=Q0,Q1,Q2,Q3,Q4,Q5,Q7 \
  --runs=3 --warmup=5m --measure=30m --out=artifacts/local-stress
```

`local-store` starts a pinned test fixture only when its required local container/process runner is available and emits its actual endpoint/config; it must not silently substitute the in-memory model. The runner cleans up only its verified random test prefix and resources.

### 20.4 Mandatory remote qualification

```sh
# Credentials use the ordinary SDK credential configuration in the harness.
# These explicit values must designate an approved disposable test namespace.
: "${QUAL_S3_ENDPOINT:?required}"
: "${QUAL_S3_REGION:?required}"
: "${QUAL_S3_BUCKET:?required}"
: "${QUAL_S3_PREFIX:?required}"
: "${QUAL_NODES_CONFIG:?three remote node addresses/configurations required}"
export QUAL_REMOTE_REQUIRED=1

CGO_ENABLED=0 go run ./internal/qual/cmd/qual doctor \
  --remote --require-remote --cas-rounds=1000 --contenders=32 \
  --out=artifacts/remote-doctor

CGO_ENABLED=0 go run ./internal/qual/cmd/qual faults \
  --remote --require-remote --nodes-config="$QUAL_NODES_CONFIG" \
  --matrix=profiles/remote-faults.json --repetitions=20 \
  --empty-cache-restarts --out=artifacts/remote-faults

CGO_ENABLED=0 go run ./internal/qual/cmd/qual stress \
  --remote --require-remote --nodes-config="$QUAL_NODES_CONFIG" \
  --profiles=Q0,Q1,Q2,Q3,Q4,Q5,Q7 --runs=3 --warmup=5m --measure=30m \
  --out=artifacts/remote-stress

CGO_ENABLED=0 go run ./internal/qual/cmd/qual stress \
  --remote --require-remote --nodes-config="$QUAL_NODES_CONFIG" \
  --profiles=Q6 --measure=24h --min-operations=10000000 \
  --out=artifacts/remote-soak
```

`QUAL_NODES_CONFIG` names actual remote node processes and their environment hashes; the runner refuses to report a three-host test when all endpoints are one process. Provisioning is supplied by the operator, not implemented as a library control plane. A budget preflight computes expected storage/requests/VM hours and requires an explicitly supplied maximum before a costly remote campaign; reaching that maximum yields a blocked/incomplete report, not a success.

### 20.5 Independent verification and report assembly

```sh
CGO_ENABLED=0 go run ./internal/qual/cmd/qual check \
  --histories=artifacts --require-ack-recovery --require-linearizability \
  --require-negative-controls --out=artifacts/checks.json

CGO_ENABLED=0 go run ./internal/qual/cmd/qual cost \
  --metrics=artifacts/remote-stress --prices=profiles/prices.json \
  --volumes=1000000,10000000,100000000,1000000000 \
  --sensitivity=profiles/cost-sensitivity.json --out=artifacts/costs

CGO_ENABLED=0 go run ./internal/qual/cmd/qual report \
  --artifacts=artifacts --require-remote --require-soak \
  --require-milestones=M0,M1,M2,M3,M4,M5,M6,M7,M8,M9 \
  --out=artifacts/qualification.md
```

The reporter verifies artifact hashes, environment consistency, required tests, nonempty samples and explicit pass/fail/blocked status. It refuses missing histories, silently skipped tests, changed seeds without annotation, or a safety failure hidden inside a successful aggregate command. Preserve raw JSON/CSV, compact histograms and minimal reproductions; do not require a hosted dashboard to understand the evidence.

## 21. Deployment and operations contract

A single consumer process needs a configured S3 client, a namespace, a NodeID and a disposable writable cache directory. No second daemon is required. On a new key it calls Create; on an existing key after process replacement it resolves/takes over lazily before updating. The caller owns request authentication, its network server and transport, node membership information, workload-specific authorization, and compute lifecycle.

A multi-node deployment runs the same application/library version on several machines, routes by the authoritative Owner, and uses Preferred only for placement/candidate selection. Scale adds capacity for independent entities. It does not magically increase one entity's serialized write rate, establish a global transaction order, or provide automatic capacity provisioning.

Expose readiness only after store capability checks and process resources are valid. Entity-specific corruption or recovery failures must be visible without marking unrelated entities irrecoverable. Do not declare readiness solely because a TCP listener is open. During an authority-store outage, advertise degraded/unavailable strong operations and retain bounded admissions.

Operational runbooks must cover: ambiguous mutation lookup, prolonged unresolved publication, recovering a lost cache, false takeover storms, incompatible schema/version, missing head/birth, incomplete creation, full local disk, receipt-capacity exhaustion, runaway object versions, backup import into a new namespace, and credential rotation. The caller can restart disposable nodes; it cannot safely “fix” a problem by deleting a head or resetting an epoch.

Credential policy must prevent unrelated services from writing this prefix. Where the provider supports it, enforce conditional writes for protected head keys and separate privileged deletion/restore credentials. Normal garbage collection needs deletion only under immutable-artifact paths. Do not make broad bucket-admin access a library requirement. An object-store account compromise is not solved by checksums.

Default guarantees apply only to qualified endpoint/region/configuration combinations. Changes to SQLite, LTX, SDK retry behavior, object-store implementation, encryption/routing layer, or mutation publication code trigger the relevant qualification subset. Schema changes must not be deployed independently of their recorded compatibility policy.

## 22. Final acceptance and evidence manifest

The implementing team produces one release evidence manifest containing:

1. Exact source/dependency/toolchain/environment locks and licenses; release artifact hashes; current API and storage-format documents.
2. M0–M9 deliverable/check/decision records, including every unexecuted or blocked item. M7 may explicitly choose no batching, with its measured reason.
3. Model exploration, negative-control detections and permanent counterexample fixtures; independent history/acknowledgment verification outputs.
4. Complete local and real-remote fault matrices, including all-node/cache loss, stale-owner resume, delayed CAS application, GC/compaction races and fresh-namespace restore.
5. Performance distributions and throughput by workload/size/architecture/durability, with offered-load timelines, errors, overload, recovery and repeat-run variance.
6. Source/API/module/native-artifact/binary/reproducibility reports; CPU/RSS/heap/goroutine/descriptors/disk/cache/staging measurements.
7. Physical S3 attempts/bytes, current/noncurrent/orphan/backup storage, compaction amplification and the parameterized price/capacity calculation.
8. A provider qualification matrix and a clearly stated operational envelope, including absent durable timers, callback trust, no cross-entity atomicity, deduplication horizon, single-entity throughput limit and authority-region availability boundary.

**Release is permitted only when all safety gates pass, required remote tests ran, and every failed performance/size gate has either been fixed or received an explicit evidence-based decision.** “Works in the simulator,” “passed unit tests,” and “the coding agent says the implementation is complete” are not substitutes.

The result should be a small storage library whose guarantees fit on one page and whose difficult behavior is visible in a reproducible test suite. Any additional component must defend its existence through an invariant, observable capability, or measured improvement—not through a speculative future application.

## 23. Primary sources and research provenance

The URLs below identify source material inspected for this plan. They are evidence for the descriptions in §1 and the stated external protocol/tooling facts, not claims that the proposed library has passed them. Default-branch content can change; listed blob IDs identify the inspected file contents. M0 records enclosing commits, dependency checksums and complete local source snapshots for reproducibility.

**[S01] CellD v0.6.1 guarantees and storage assumptions.**
https://github.com/denoland/celld/blob/v0.6.1/docs/guarantees.md  
Tag tree commit: `f2bf648663a610eefde71f3547ad61e9b896b1f0`. Read for conditional writes, bucket/fleet acknowledgment differences, output gating, epoch restore, GC and provider limitations.

**[S02] CellD v0.6.1 output-gate implementation.**
https://github.com/denoland/celld/blob/v0.6.1/crates/logic/output_gate.rs  
Inspected file blob: `22b4766971b1b63875443bb30e688142fb13982a`. Read for observed-unproven-state handling, read/error/stream output barriers and ownership checks.

**[S03] CellD v0.6.1 WAL parser.**
https://github.com/denoland/celld/blob/v0.6.1/crates/ltx/src/wal.rs  
Inspected file blob: `57a60873200a324b32883e9da429103b433e2798`. The source attributes its parser to Litestream v0.5.11 and distinguishes valid frames from committed transactions.

**[S04] CellD v0.6.3 release: missing ownership-record regression.**
https://github.com/denoland/celld/releases/tag/v0.6.3  
Release published 21 October 2026; target commit `8702f6d186c77698dbfb30cf29e3b1922a88f008`. Used to distinguish later fixes from the v0.6.1 baseline, not to silently upgrade that baseline.

**[S05] Litestream replication architecture.**
https://litestream.io/how-it-works/  
Read for asynchronous replication, WAL/checkpoint interactions, LTX levels and snapshot recovery.

**[S06] Litestream database implementation.**
https://github.com/benbjohnson/litestream/blob/main/db.go  
Inspected blob: `63922228f6c2f0196cd890d353938cb5d38648ff`. Read for modernc use, checkpoint strategy, bounded sync work and retained replication state.

**[S07] Litestream WAL reader and committed-page map.**
https://github.com/benbjohnson/litestream/blob/main/wal_reader.go  
Inspected blob: `e323c7d1b205d2c8025238c78f8f4a30d20d203e`. Read header/frame checksum paths and commit-aware page-map construction. Attribution/license must accompany any extraction.

**[S08] Litestream embedding and synchronization interfaces.**
https://litestream.io/guides/go-library/  
https://litestream.io/reference/sync/  
Used to assess direct embedding and explicit synchronization without assuming distributed ownership guarantees.

**[S09] Litestream dependency graph.**
https://github.com/benbjohnson/litestream/blob/main/go.mod  
Inspected blob: `6445ee4c7434897d95162c0d90e41316c7cd80c8`. Its repository includes many provider/tooling dependencies; this is not a claim that every optional dependency executes in the core replication path.

**[S10] Go LTX v0.5.2 format and validation.**
https://github.com/superfly/ltx/blob/v0.5.2/ltx.go  
Inspected blob: `57d9aa41863a6126b3142da084b66631424aada7`. Read header, format version, snapshot rules, position ranges and checksum flags.

**[S11] Go LTX module dependency.**
https://github.com/superfly/ltx/blob/main/go.mod  
Inspected blob: `f78d963480bd526d319492d3b9956933af32a0f8`. The inspected module declares `github.com/pierrec/lz4/v4`; M0 confirms the exact selected tag's dependency graph.

**[S12] Go LTX encoder and pinned compactor.**
https://github.com/superfly/ltx/blob/main/encoder.go  
Encoder inspected blob: `dc652d382fca0974896dd01f2c5326a663370108`.  
https://github.com/superfly/ltx/blob/v0.5.2/compactor.go  
Compactor inspected blob: `2bdbda76845a89263c0797faa49a9f30cec8259c`. Main-branch encoder additions are not presumed to exist in v0.5.2. The compactor's permissive noncontiguous option must remain disabled.

**[S13] LiteFS architecture and guarantees.**
https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md  
Inspected blob: `a8cc10f75e5a480b9c111fe70eb6cb0469d4d4e5`. Read FUSE capture, Consul leadership, asynchronous-replication loss window and page checksum design.

**[S14] libSQL Bottomless architecture.**
https://github.com/tursodatabase/libsql/blob/main/bottomless/README.md  
Read for native WAL replication, S3 storage and restoration behavior.

**[S15] libSQL Bottomless replicator implementation.**
https://github.com/tursodatabase/libsql/blob/main/bottomless/src/replicator.rs  
Inspected blob: `6f318fd125ba49090b1741dfaa14039c94bf5892`. Read frame/generation/snapshot and upload-progress structures. A complete Bottomless correctness audit is not claimed.

**[S16] Go libSQL local/embedded binding.**
https://github.com/tursodatabase/go-libsql/blob/main/libsql.go  
Inspected blob: `7ec436d02639d794785ba310faf2266d779259a1`. Its build constraint and native library linkage rule it out for this module's local pure-Go engine.

**[S17] Turso Sync design.**
https://turso.tech/blog/introducing-databases-anywhere-with-turso-sync  
Read to distinguish logical/physical synchronization with a remote database from sole-authority object-store persistence.

**[S18] modernc SQLite candidate dependency pin.**
https://github.com/modernc-org/sqlite/blob/v1.61.1/go.mod  
Inspected blob: `8570e77e9af13763051f97fc55e8f600b7c3ac8f`. Toolchain/libc dependencies must be reproduced, not independently guessed.

**[S19] modernc SQLite authorizer implementation and contract.**
https://github.com/modernc-org/sqlite/blob/v1.61.1/authorizer.go  
Inspected blob: `7401a38419990630dd1f646ba00f655c012212a0`. Read connection registration, action codes, statement invalidation and callback restrictions.

**[S20] modernc SQLite package documentation.**
https://pkg.go.dev/modernc.org/sqlite  
https://modernc.org/sqlite/  
Use the documentation for the exact chosen version during M0; cached documentation for an older version is not evidence that a new API exists.

**[S21] Amazon S3 conditional writes and PUT contract.**
https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html  
https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html  
Read `If-None-Match`, `If-Match`, preconditions, concurrency/error behavior and versioning qualifications.

**[S22] Amazon S3 consistency model.**
https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel  
Supports the required regional object visibility model; does not create multi-object transactions.

**[S23] Scaleway conditional-write documentation.**
https://www.scaleway.com/en/docs/object-storage/api-cli/using-conditional-writes/  
Required capability documentation, still subject to real endpoint tests.

**[S24] Scaleway object-storage consistency documentation.**
https://www.scaleway.com/en/docs/object-storage/faq/  
Read with the actual endpoint/region configuration; no qualification is inferred for arbitrary S3 services.

**[S25] SQLite database and WAL file formats.**
https://sqlite.org/fileformat2.html  
Canonical authority for WAL headers, frames, commit markers, checksums and database file interpretation.

**[S26] SQLite WAL behavior and WAL-reset fix.**
https://sqlite.org/wal.html  
Read checkpointing, shared-memory/exclusive-mode considerations and the documented WAL-reset issue/fixed versions.

**[S27] Go race detector.**
https://go.dev/doc/articles/race_detector  
Includes instrumentation requirements; the ordinary pure-Go build and race-testing lane are distinct.

**[S28] Porcupine Go history checker.**
https://pkg.go.dev/github.com/anishathalye/porcupine  
Test-only candidate for model-based linearizability checking; pin its revision in the tooling lock.

**[S29] Scaleway object-storage price input.**
https://www.scaleway.com/en/pricing/storage/  
Consulted 7 November 2026. Recheck region, tier, unit, rounding, transfer conditions and tax treatment before cost qualification.

**[S30] Scaleway compute price input.**
https://www.scaleway.com/en/pricing/virtual-instances/  
Consulted 7 November 2026. The example CPU price is not an assertion of measured capacity or a complete infrastructure quote.


**[S31] CellD v0.6.1 epoch collection implementation.**
https://github.com/denoland/celld/blob/v0.6.1/crates/logic/epoch_gc.rs  
Inspected blob: `2435f2104c02103ef018d9ddaaeff1f546d464a6`. Read actual restore-chain selection, pinned-read guard, recorded retirement boundary, grace margin and ownership-read ordering.
