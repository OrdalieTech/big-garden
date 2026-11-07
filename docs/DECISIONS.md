# Decisions that amend docs/PLAN.md

PLAN.md is the contract. Each entry below changes it on purpose; anything not listed here follows the plan exactly. New amendments go at the end, with the invariant or measurement that justifies them.

## D1. Local SQLite runs with `synchronous=OFF`

Every cache directory is discarded when a process starts (PLAN §9.3), and nothing in a running process trusts local files beyond what the OS page cache already guarantees. An fsync would only protect against an OS crash, and the files are deleted after one. Durability comes from S3 alone, so the WAL and checkpoint fsyncs are dropped from every mutation. Staging files are not fsynced either.

## D2. Capture reads pages through SQLite, not through a hand-written WAL parser

After the `wal_checkpoint(TRUNCATE)` that ends each publication, the WAL holds only frames written since. Capture takes the page numbers from the WAL frame headers. That set is a superset of the pages the batch changed, since every page write in WAL mode lands in a frame. Capture then reads each page's committed content via `SELECT data FROM sqlite_dbpage WHERE pgno=?` on the library's own connection, and the final size via `PRAGMA page_count`. A page from a rolled-back frame comes back unchanged, which is harmless. Pages above `page_count` are dropped. This replaces PLAN §6.2's salt, checksum and commit-marker parser (budgeted at 350 lines) with SQLite's own pager. The authorizer allows `sqlite_dbpage` only in library mode.

To discard speculative local state, close the connection, delete `-wal`/`-shm` and reopen. The main file always equals the last published image, because a checkpoint runs only after publication.

## D3. One resolution function and hedged writes instead of "one physical attempt"

PLAN §8.1 forbids SDK retries on the head CAS so that a retried attempt cannot produce a misleading 412. That rule still holds: the SDK makes exactly one attempt per call. The library may send an explicit hedge, an identical second request with the same bytes and the same `If-Match`/`If-None-Match`, once the first has been outstanding longer than a hedge delay. This addresses the measured Scaleway PUT stalls (about 1 in 300 PUTs stall around 30 s).

Any CAS outcome other than a known success, including 412, timeout, 5xx and cancellation, goes through a single `resolve(candidate)` step:

1. GET the head.
2. If the head's nonce equals the candidate's, the write is committed.
3. If the head's revision is greater than the candidate's base, the candidate is dead (its If-Match token can never match again), and the receipt is checked in the new image.
4. If the head is still at the candidate's base, the outcome stays unknown. In that case, either fence with a no-op CAS or return `outcome_unknown`.

Immutable PUTs can be hedged and retried freely, because the key and bytes are fixed.

## D4. A Node takes over automatically from its own previous sessions

When a head's owner has this Node's NodeID but a different Session, it belongs to an earlier process of the same node. Update, Read and Lookup then take over implicitly; the CAS still fences that process. Owners with other NodeIDs still require an explicit `Takeover`. This removes boilerplate from single-node deployments.

## D5. A Create interrupted between birth and head can be resumed

If `birth.json` exists without `head.json`, and the retry carries the same operation ID and bound input hash as the birth record, the retry may build and publish genesis. The first attempt was never acknowledged, so this is the retry contract of `outcome_unknown`. Any other request still gets `state_missing`. Before resuming, the entity prefix must contain no artifact with a base revision above 0. Otherwise the head was deleted after use, and the library fails closed.

## D6. Receipt retention defaults to 1 hour

The minimum stays at 1 hour. With a 24 hour default, a hot entity at 10 writes/s accumulates about 864k receipts a day and exhausts the 64 MiB cap.

## D7. Strong reads revalidate the head with a conditional GET

`If-None-Match: <cached ETag>` answering 304 means the cached head is still current at that instant, which is the read's linearization point. A 304 has no body, so this saves bytes without weakening any guarantee.

## D8. Structure: straight-line operations, no implicit state machines

Every public operation is one function that runs top to bottom under the entity's mutex. Durable state lives in the head and the SQLite image. The in-memory entity struct holds only the cached head, its ETag, the open connection, and an `unresolved *candidate` pointer that blocks admission while it is set. There is no status enum with scattered transitions. Failure handling does not attempt in-place repair: close the cache, rehydrate from the current head, or return a typed error.

## D9. Package layout

- `/` (package `garden`): the public API, node, entity operations, maintenance.
- `internal/head`: strict codecs and invariants for namespace, birth and head JSON.
- `internal/engine`: the SQLite connection, authorizer scope, capture, the LTX adapter and restore.
- `internal/store`: the private storage interface and the S3 adapter.
- `internal/storetest`: an in-memory store with a scheduled-fault store. It is imported only by tests and qualification code.
- `internal/qual/...`: the model checker, the `qual` command and the minimal consumer.

The layout lets parallel workstreams compile and test independently. The production LOC budget in PLAN §12.1 still covers everything outside `internal/storetest` and `internal/qual`.

## D10. Speculative local state is discarded with the whole cache file

M0 (`TestCloseCheckpointsWAL`) shows that closing the last SQLite connection checkpoints committed WAL frames into the main file even with `wal_autocheckpoint=0`, and modernc v1.61.1 exposes no `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`. The second paragraph of D2 ("close, delete `-wal`/`-shm`, reopen") would therefore install unpublished pages into the main file. Discarding speculative state (any publication that is not proven committed, quarantine, a bad connection) closes the connection and deletes the database, `-wal` and `-shm` files; the next access rehydrates from the current head. `Evict` also deletes the files: a closed cache is never reopened (PLAN §9.3 would require revalidating it anyway). This is D8's close-and-rehydrate.

## D11. Create runs the callback before writing birth.json

PLAN §6.3 writes `birth.json` first and runs the callback after. A callback that fails deterministically would then leave a birth without a head, which D5 lets only the identical request resume: the key would answer `state_missing` forever. Create therefore builds the genesis image locally first (speculative, never published), writes `birth.json` only after the callback succeeded, and publishes only if it won that conditional create. "Only the birth winner publishes genesis" still holds.

## D12. The D3 fence re-proposes the candidate with a fresh nonce

When `resolve` finds the head still at the candidate's base, it sends the same proposed head again with a new nonce and the same `If-Match` (or `If-None-Match: *` for genesis). All proposals of one candidate share one condition, so at most one applies; resolution accepts any of their nonces. A winning re-proposal both fences the earlier ambiguous request and commits the intended effect, so an ambiguous mutation ends committed instead of rolled back. Proposals are capped at three per candidate.

## D13. LTX is pinned at v0.5.3

PLAN §1.3 names v0.5.2 as the candidate pin. v0.5.3 rejects bytes after the trailer, validates the page index against the decoded pages while streaming it instead of reading the whole tail into memory, and aborts the encoder on partial writes. The only format-relevant change excludes the SQLite lock page from checksums, and that page (262145 at 4 KiB) lies beyond the 256 MiB ceiling. Its `encoder.go` is the main-branch blob the plan inspected [S12]. Details in `research.lock.json`.

## D14. A deleted head is outside the fail-closed guarantee while a create of that key is in flight

The M1 model (finding F1, `.work/model-findings.md`, fixture `internal/qual/model/testdata/findings`) shows that a genesis CAS can apply after an administrator deletes `head.json`: `If-None-Match: *` holds again once the key is gone, and a hedge, a late-applied request, a D12 re-proposal or a D5 resume can still carry that condition. Genesis then republishes over acknowledged data. This is inherent to per-key S3 conditions and already existed in PLAN §6.3 (a single-attempt genesis CAS can apply late), so narrowing D5 or D12 would only make it rarer. G6's missing-head clause therefore holds for a head that disappears while no create of that key is in flight, and PLAN §3.3 already excludes head deletion from the store contract. The library never deletes `head.json`, `birth.json` or `namespace.json`, and the operational guide requires a bucket policy that denies `DeleteObject` on those keys to every credential, operators included. The model keeps F1 as a documented fixture, and its G6 check runs without the administrative deletion (case C0).

## D15. No read lease and no stale local read in v1

Every strong read revalidates the head with the D7 conditional GET, about 10 ms to fr-par from a warm owner, because that request is what keeps a read from returning data that a takeover has superseded. A lease would remove the request but make safety depend on bounded clock drift and pauses, and a "maybe stale" local read would add API surface that PLAN §2 explicitly does not provide. Both can be added later without changing the storage format.

## D16. Small writes publish in one CAS: an inline tail in the head

A write that uploads an object and then CASes the head makes two sequential S3 round trips, and each is exposed to the rare multi-second PUT stalls measured on Scaleway. The head therefore carries `tail`: at most one LTX delta, inline (base64), covering the sequences after the last delta ref up to `head.sequence`. Each mutation compacts `tail + new delta` into one LTX with the existing compactor (later page wins). If the result is at most `InlineBytes` (31 KiB raw, so the largest valid head stays under 64 KiB) and the head already references an object with `base_revision >= 1`, the mutation is a single CAS carrying the new tail. Otherwise it PUTs that compacted LTX as an ordinary delta object and CASes the head with the ref appended and the tail empty, which is today's path and doubles as level-0 compaction.

Rules, enforced by `head.Validate`:
- A tail is allowed only when some ref has `base_revision >= 1`. The first mutation of an incarnation therefore always uploads an object, which keeps D5's resume check complete: a used-then-deleted head always leaves a base >= 1 object behind.
- The tail is the last link of the chain (contiguous range ending at `head.sequence`, sha256, pre/post checksums), and the replay bound counts its decoded pages.
- A successor with an unchanged sequence carries the tail byte-identical.
- Snapshots empty the tail. Run compaction of delta refs carries it unchanged.

Resolve, GC and the 32-ref cap are unchanged: the tail is never an object and is always replaced together with the head, so a single-CAS mutation resolves through the receipt in the head. Capture must emit every page a delta grew the database by, because compaction is exact only then (asserted in the engine).
