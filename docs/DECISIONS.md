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
