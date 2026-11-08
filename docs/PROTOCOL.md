# Protocol, format 1

The exact steps every operation implements. PLAN.md is the contract and DECISIONS.md amends it; this file turns both into straight-line procedures. Each operation is one function, run top to bottom under the entity mutex (D8). If this file and the code disagree, fix one of them in the same commit.

## 0. Conventions

- `self` = `(Config.NodeID, session)`; `session` is a fresh `head.NewID()` per `Open`.
- `GET`, `PUT_IMM`, `CAS`, `LIST`, `DEL` are the `internal/store` methods; outcomes are `store.Outcome`. Keys below are namespace-relative; the S3 adapter prefixes `<Config.Prefix>/`.
- Reads and `PUT_IMM` retry `Unknown` with full jitter until the operation deadline (the S3 adapter does this inside `Get` and `PutImmutable`). `CAS` makes one SDK attempt; an identical hedge (same bytes, same condition) is sent once the first has been outstanding for `StorageAttemptTimeout/2` (D3). Anything but `OK` from `CAS` goes to `resolve` (§2).
- "Strict decode" = `head.Decode` / `DecodeBirth` / `DecodeNamespace`, then identity checks: `namespace` equals the Node's, `key` equals the requested key, `incarnation` equals `Ref.Incarnation` (else `incarnation_mismatch`).
- Head missing: if `birth.json` exists, `state_missing`; else `not_found`.
- "Discard" (D10) = close the connection, delete the entity's db, `-wal`, `-shm` files, drop the cached head. The next access rehydrates (§4).
- "Barrier" = §3 steps 2–3 (fresh conditional head GET). Required before returning any state-dependent answer that is not a successful CAS (replays, `id_conflict`, `schema_mismatch`, `id_expired` by floor, callback errors). If the head changed, redo the operation from its activation step once, then return `route_changed`.
- "Lock" = take the entity mutex within the caller's ctx (PLAN §8.4: a deadline ends the caller's waiting). A deadline while queued returns `outcome_unknown` to a request with an operation ID (the unresolved candidate may be its own; Release and Evict too) and `unavailable` to reads and Takeover. "Settle `unresolved`" runs `resolve` under the caller's ctx capped by `CommitTimeout`; cut short, the candidate simply stays unresolved.
- Entity memory (D8): cached head `H`, its ETag `E`, the connection, `unresolved *candidate`. Nothing else, except the decoded page frames of each of `H`'s snapshot, deltas and tail, derived from `H` and reset with it (the replay pages of §6.7 and §9; set by activation and by each committed candidate), and the time of the last admission (cache policy only).

## 1. Objects and formats

```text
namespace.json                                    immutable  head.Namespace  ≤ 1 KiB
probe/<attempt-32hex>/...                         doctor and Open probe only
entities/<h[0:2]>/<h>/birth.json                  immutable  head.Birth      ≤ 4 KiB
entities/<h[0:2]>/<h>/head.json                   CAS        head.Head       ≤ 64 KiB
entities/<h[0:2]>/<h>/objects/<inc-32hex>/<base-revision-16hex>/<attempt-32hex>/<sha256-64hex>.ltx
```

- `h` = lower-hex SHA-256 of the exact key bytes (`head.EntityDir`). Object keys come only from `head.ObjectKey` and are checked by `head.ParseObjectKey`. `base-revision` is the revision of the head the object was produced against (0 for genesis).
- JSON: canonical encoding only (`internal/head`); decode = encode round trip.
- Tail (D16): `head.tail` is null or one LTX delta stored inline (`data`, standard padded base64, `bytes` ≤ `head.InlineBytes` = 31 KiB raw, `sha256`, sequence range, pre/post checksums). The chain is snapshot, deltas, then the tail, ending at `head.sequence` and `database_checksum`. A tail is allowed only when some ref has `base_revision ≥ 1`, so a used head always leaves an object for the D5 check (§5 step 3). It is never an object: GET, LIST, Collect and the 32-ref cap ignore it. `Tail.Ref()` gives it as a key-less `head.Ref` (sha256, bytes, range, checksums), which `engine.Compact` and `engine.Restore` accept with `bytes.NewReader(tail.data)`; they never read `key`, `base_revision` or `level`.
- LTX: format 3 (`ltx-v3`), page size 4096, `MinTXID..MaxTXID` = the logical sequence range, never SQLite or Litestream IDs. Snapshot: `MinTXID=1`, `PreApplyChecksum=0`, every page `1..Commit`. Delta (and tail): pre-checksum = predecessor's post-checksum. Reject `HeaderFlagNoChecksum`. Before decoding bytes not yet matched to a head's `sha256`/`bytes` (Import), bound every frame's declared size to `lz4.CompressBlockBound(4096)`: ltx v0.5.3 allocates the declared size first (M0).
- Compaction (`engine.Compact`, later page wins, pages beyond the last commit dropped) equals applying its inputs in order only if every input contains every page it grew the database by. Capture guarantees it (§6.6), so it holds for the tail and for run compaction alike.
- Replay pages = decoded pages of the deltas plus the tail (a 31 KiB tail can decode to MiBs of compressible pages); both §6.7 and the §9 snapshot trigger use this sum.
- Checksums: `database_checksum` = `ltx.ChecksumReader` over the whole image (flag bit 63 set). Ordinary mutations update it incrementally: XOR out `ChecksumPage(p, old)` for every changed or truncated page, XOR in `ChecksumPage(p, new)` for every final page.
- Request binding (PLAN §8.3): `bound = SHA-256("state-request-v1\x00" || kind || u32be(from) || u32be(to) || Request.Hash)`, kind 1 create, 2 update, 3 migrate, 4 import; `from = to = Request.Schema` except migrate.

Reserved SQLite tables (part of format 1; the authorizer denies application access, including triggers or indexes whose table is reserved):

```sql
CREATE TABLE _state_meta(format INTEGER NOT NULL, namespace BLOB NOT NULL, key TEXT NOT NULL,
  incarnation BLOB NOT NULL, schema INTEGER NOT NULL, sequence INTEGER NOT NULL,
  expiry_floor_ms INTEGER NOT NULL) STRICT;                       -- exactly one row
CREATE TABLE _state_receipts(operation_id BLOB PRIMARY KEY, input_hash BLOB NOT NULL,
  result BLOB NOT NULL, sequence INTEGER NOT NULL) STRICT, WITHOUT ROWID;
```

Head field transitions (`=` unchanged; refs `=` includes the tail byte-identical; every row also sets `revision+1` and a fresh `nonce`; `head.Successor` enforces it, `head.CheckGenesis` for the first row):

| Operation | owner | epoch | sequence | schema | refs |
|---|---|---|---|---|---|
| create / import | self | 1 | 1 / exported+1 | Request.Schema | snapshot at base 0, no deltas, no tail (revision 1) |
| update, batch of B | = self | = | +B | = | tail = compact(tail, delta) inline; or that LTX appended as one delta at base `revision`, tail null; or snapshot-on-cap (§6.7–6.9) |
| migrate | = self | = | +1 | `to` | as update |
| takeover | self | +1 | = | = | = |
| release | null | +1 | = | = | = |
| compaction | = self | = | = | = | run of 8 deltas replaced by one delta (level+1) at base `revision`; tail = |
| snapshot | = self | = | = | = | new snapshot at base `revision`, no deltas, tail null |

## 2. Publication: `publish` and `resolve` (D3, D12)

A candidate is `{base *Head (nil for genesis), baseETag, next Head, nonces []ID, object *Ref, op *OpID, bound}`. Its identity is the set `nonces`. `object` is nil for a single-CAS (inline tail) mutation, takeover, release and fence.

`publish(c)`:
1. Check `next.Validate()` and `Successor(base, next)` (or `CheckGenesis(next)`). A failure is a bug: discard, return `corrupt`.
2. `e.unresolved = c`; `c.nonces = [next.Nonce]`.
3. `CAS(head key, encode(next), baseETag)` (`""` = `If-None-Match: *` for genesis), under a node-owned context bounded by `CommitTimeout`; the caller's cancellation does not cancel it.
4. `OK`: `e.H, e.E = next, etag`; `e.unresolved = nil`; committed.
5. A definite failure (the store adapter's `Failed`: the request was answered and never applied, e.g. 403 or 400) of every request of the proposal, hedge included: no proposal of `c` can ever apply, so `c` is definitely not committed: discard (D10); `unresolved = nil`; return `unavailable`. A candidate is definitely not committed only if every proposal it ever sent got such an answer; `PreconditionFailed` (412) is not one (it does not prove an earlier proposal did not apply), nor is a timeout, 5xx, cancellation after sending or a lost reply.
6. Otherwise: `resolve(c)`. A candidate in `resolve` thus has at least one proposal that may still apply late, and a later definite failure of a re-proposal does not change that: it stays unresolved under rule 5 below until a read decides it.

`resolve(c)`, looped until `CommitTimeout`:
1. `GET head` (no `If-None-Match`). `Unknown`: back off, loop. `NotFound` with `base != nil`: `state_missing`, keep `unresolved`. `NotFound` with `base == nil`: go to 4.
2. Strict decode `cur`. If `cur.nonce ∈ c.nonces`: committed; `e.H, e.E = cur, etag`; `unresolved = nil`.
3. If `base == nil` or `cur.revision > base.revision`, every proposal is dead (their condition can never hold again). Then:
   - `c.object` is referenced by `cur`: committed (only our transition could introduce it). The tail is not an object and never proves anything here. A maintenance candidate (object, no op) whose object is not referenced is not committed.
   - else `c.op != nil` (always the case for an inline-tail mutation): hydrate `cur` (§4) and look up the receipt: same `bound` → committed (return the stored result); other `bound` → `id_conflict`; absent with `ts(op) ≤ meta.expiry_floor_ms` of `cur`'s image → a newer owner may have pruned the receipt (or a snapshot or compaction dropped our object), so the outcome is unknown for good: discard; `unresolved = nil` (every proposal is dead); return `outcome_unknown` (a retry then gets `id_expired`); absent otherwise → not committed.
   - else takeover: committed iff `cur.owner == self`; release: done iff `cur.owner != self`.
   - Not committed: discard; `unresolved = nil`; return `not_owner` with `cur`'s Route if `cur.owner != self`, else `route_changed`.
4. `cur` is still the base (same nonce, or still absent): if this call of `resolve` has sent fewer than two re-proposals (so a publication sends at most three proposals, and each later `resolve` at most two more), set `next.nonce = NewID()`, append it, `CAS(encode(next), baseETag)`; `OK` → committed. Back off, loop.
5. Deadline: return `outcome_unknown`; `e.unresolved` stays set. Any other failure while `e.unresolved` stays set (a head that fails strict decode, hydration for the receipt check failing with `unavailable`, `capacity` or `corrupt`) is also returned as `outcome_unknown`, never as a code that means "not committed"; only rule 1's `state_missing` keeps its own code. Every later operation on the entity runs `resolve(e.unresolved)` first; while it stays unresolved, the same operation ID gets `outcome_unknown`, others `unavailable`.

After committed: if the local image is the candidate's, `PRAGMA wal_checkpoint(TRUNCATE)` and verify an empty WAL; on any failure discard (the result stays a success). If `cur` is not our candidate head and differs in sequence or checksum from the local image, discard.

## 3. Strong read: `Read(ref, schema, fn)` (PLAN §7.3, D7)

1. Lock. Settle `unresolved` (§2 rule 5).
2. `GET head` with `If-None-Match: e.E` if cached. This GET, issued after the call began, is the linearization point.
3. `NotModified`: `e.H` is current. `OK`: strict decode; if its sequence or checksum differs from the local image, discard. Not owned: if `owner.node == NodeID` take over implicitly (D4, §6); else `not_owner`.
4. Activate if needed (§4) with exactly that head.
5. `meta.schema != schema`: `schema_mismatch`.
6. Library `BEGIN`; switch authorizer to read mode (re-register: expires prepared statements); `fn(view)`; library `COMMIT`. Result over `ResultBytes`: `capacity`. A callback error passes through unchanged (it describes validated state).
7. Return `Result{Value, Version{inc, H.sequence}}`.

`Lookup(ref, id)`: steps 1–4, then read `_state_receipts` for `id`. Found: `Result{result, Version{inc, receipt.sequence}, Replayed: true}`. Absent: `ts(id) ≤ meta.expiry_floor_ms` → `id_expired`, else `absent_at_version` with `At = Version{inc, H.sequence}`.

`Resolve(key)`: `GET head` (fresh), strict decode, return `Route{Ref{key, inc}, owner or zero, epoch, revision}`.

## 4. Activation (cold restore of exactly head `H`, PLAN §9.3)

1. Take one of `Limits.Recoveries` activation slots, waiting within the caller's ctx (else `capacity`). Reserve cache bytes (`4×EntityBytes + 1 MiB`) under the cache policy (§7); else `capacity`.
2. GET snapshot and deltas (≤ 4 concurrent), `max = ref.bytes`. The tail needs no GET: its bytes are in `H`. Check length and SHA-256. `NotFound` or mismatch: GET head again; if it changed, restart with the new head (collection race); else `state_missing` (missing) or `corrupt` (mismatch).
3. Into a new private file (`O_EXCL`, 0600) in the session cache dir: apply the snapshot, then each delta in order, then the tail as `Tail.Ref()` over `tail.data`. Per object: `MinTXID/MaxTXID` = ref range, `PreApplyChecksum` = running checksum, decoder close succeeds, truncate to `Commit` pages, recomputed checksum = trailer post-checksum = `ref.post_checksum`.
4. Check `page_count = H.page_count` and checksum = `H.database_checksum`.
5. Install by rename; open with mode `rw` (never `rwc`); set and verify `page_size=4096`, `journal_mode=WAL`, `wal_autocheckpoint=0`, `synchronous=OFF` (D1), `trusted_schema=OFF`; install the authorizer.
6. `_state_meta`: format 1, namespace/key/incarnation = head, `sequence = H.sequence`, `schema = H.schema`; `PRAGMA integrity_check` = `ok`. Else discard, `corrupt`.
7. `e.H, e.E = H, etag`.

## 5. Create(key, req, fn) (PLAN §6.3, D5, D11)

1. Validate key and request (nonzero ID and hash, schema > 0, `ts(ID) ≤ now+5 min`, `ts(ID) > now − ReceiptRetention`). `bound` with kind 1.
2. `GET head`. Exists: strict decode, `GET birth`. Birth has `operation_id = ID` and `input_hash = bound`: barrier, read the receipt (§3 Lookup) and return it `Replayed`. Same ID, other hash: `id_conflict`. Else `invalid_argument` ("key exists") with the Route.
3. `GET birth`.
   - Exists with the same ID and bound (D5 resume): `LIST entities/<h>/objects/`; any key that does not parse under `ObjectPrefix(key, birth.incarnation)` or has base revision > 0 → `state_missing`. (Complete with D16: a head with data beyond genesis always references an object at base ≥ 1, since a tail requires one.) `inc = birth.incarnation`.
   - Exists otherwise: `id_conflict` if same ID, else `state_missing`.
   - Absent: `LIST entities/<h>/` limit 1; anything → `state_missing`. `inc = NewID()`.
4. Build genesis locally (D11): create the db file (`rwc`, the only such open), pragmas as §4.5, reserved tables, meta `(1, ns, key, inc, req.Schema, 1, 0)`. Library `BEGIN IMMEDIATE`; mutation mode; `fn(tx)`; callback error → rollback, discard, return it. Library: insert receipt `(ID, bound, result, 1)`; `COMMIT`; checkpoint TRUNCATE.
5. If birth was absent: `PUT_IMM birth {1, ns, key, inc, ID, bound, schema, attempt=NewID()}`. `PreconditionFailed`: `GET birth`; our attempt → continue; same ID and bound → continue with its incarnation only if it equals `inc`, else discard and restart at 3; otherwise discard, `state_missing`. From the birth `PUT_IMM` on (from step 4 for a D5 resume), `birth.json` may exist and only this request can resume it: any later `unavailable`, `capacity` or `corrupt` failure of the create (this `PUT_IMM`, the snapshot `PUT_IMM`, a definitely failed genesis CAS) is returned as `outcome_unknown` with `Operation = ID`.
6. Encode the snapshot LTX (1..1); `PUT_IMM ObjectKey(key, inc, 0, NewID(), sha)`; `PreconditionFailed` → GET, compare bytes.
7. `next` = genesis head (revision 1, epoch 1, owner self, sequence 1, schema, page_count, checksum, snapshot ref level 0, no deltas, no tail). `publish` with `base = nil`.
8. Return `Ref{key, inc}`, `Result{result, Version{inc, 1}}`. A `resolve` that finds another genesis carrying our receipt returns its stored result.

## 6. Mutations: Update(ref, req, fn), B = 1 (PLAN §6.4)

1. Validate request; `bound` with kind 2. Lock. Settle `unresolved`.
2. Owned root: cached `H` owned by self is enough (no GET; a takeover makes the CAS fail). Otherwise `GET head`, strict decode; owned by an earlier session of this NodeID → implicit takeover (D4, §7); owned by another node or unowned → `not_owner` with Route. Activate if needed (§4).
3. Admission, in order (after the settle of step 1, so a same-ID retry of an unresolved candidate is resolved first): `ts(ID) > now+5 min` → `invalid_argument`; `ts(ID) ≤ now − ReceiptRetention` → `id_expired`; then, each after a barrier: `ts(ID) ≤ meta.expiry_floor_ms` → `id_expired`; receipt with same bound → return it `Replayed`; other bound → `id_conflict`; `meta.schema ≠ req.Schema` → `schema_mismatch`.
4. Library `BEGIN IMMEDIATE`; mutation mode; `result, err = fn(tx)`; every `Query` finalized. Error → library `ROLLBACK`, barrier, return it unchanged. Result over `ResultBytes` → rollback, `capacity`.
5. Library: `floor' = max(floor, now_ms − retention_ms)`; delete at most 1000 receipts with `operation_id < u64be(floor'+1)`; insert receipt `(ID, bound, result, seq+1)`; update meta `sequence = seq+1`, `expiry_floor_ms = floor'`; `page_count × 4096 > EntityBytes` → rollback, `capacity`; `COMMIT`. From here until resolution, nothing derived from this state leaves the entity.
6. Capture (D2): page numbers from WAL frame headers; `PRAGMA page_count`; for each named page ≤ page_count, `SELECT data FROM sqlite_dbpage WHERE pgno=?`; update the checksum incrementally from the old pages in the main file (still the published image). Assert that every page in `(H.page_count, page_count]` was captured (compaction exactness, §1); else discard, `corrupt`.
7. Cap: if `len(H.deltas) = 32`, or replay pages (deltas plus tail, known from encoding or activation) plus this delta's pages exceed `2 × page_count + 256`, publish a snapshot instead: the main file overlaid with the captured pages, `1..seq+1`, no deltas, tail null; go to 9.
8. Encode the delta while hashing (in memory: `PUT_IMM` takes the whole body, and the delta is bounded by `EntityBytes`). `t` = the delta if `H.tail` is null, else `engine.Compact([H.tail.Ref(), delta])` into another (range `H.tail.min..seq+1`, pre = `H.tail.pre_checksum`).
   - Inline: if `t.bytes ≤ InlineBytes` and some ref of `H` has `base_revision ≥ 1`, `next.tail = t` with its bytes; no PUT, `c.object = nil`. The first mutation after genesis therefore always takes the upload path.
   - Upload: otherwise `PUT_IMM ObjectKey(key, inc, H.revision, NewID(), sha)` (retry same bytes; `PreconditionFailed` → GET, compare bytes); append it at level 0, `next.tail = null`, `c.object` = that ref. Failure before any CAS → discard, `unavailable` (definitely not committed).
9. `next = H` with revision+1, fresh nonce, `sequence = seq+1`, page_count, checksum, and the refs and tail from 7 or 8; `publish`. An inline mutation is the single CAS of this step.
10. Committed → return `Result{result, Version{inc, seq+1}}`. Then, best effort under the gate: compaction or snapshot (§9).

`Migrate(ref, req, to, fn)`: as Update with kind 3 (`from = req.Schema`, `to`); `to > req.Schema` else `invalid_argument`; admission requires `meta.schema = req.Schema`; the transaction also sets `meta.schema = to`; `next.schema = to`; result value is empty.

Batches (B > 1, M7 only): steps 4–5 per request in FIFO order with consecutive sequences, one capture, one delta `seq+1..seq+B` (inline or uploaded as in step 8), one `publish`. A callback error that observed an earlier unpublished member is held until that publication resolves; if it does not commit, the error becomes `outcome_unknown` or `unavailable`.

## 7. Ownership

`Takeover(observed)`:
1. Lock. Settle `unresolved`.
2. `GET head` fresh; strict decode against `observed.Ref` (`incarnation_mismatch`).
3. `owner = self`: activate if needed, return the Route.
4. `owner ≠ observed.owner` or `epoch ≠ observed.epoch` → `route_changed` with the current Route.
5. `next = cur` with owner self, epoch+1 (data refs, tail, sequence, schema preserved); `publish` with `base = cur`.
6. Not committed and the current head still has the observed owner and epoch (the incumbent committed more data): retry from 2, at most 3 times. Else `route_changed`.
7. Activate exactly `next` (§4). Failure → return it with the current Route; ownership stays claimed, the entity stays unavailable until a later activation succeeds. If activation restarted on a newer head (collection race) that this session does not own, discard and return `route_changed` with that head's Route.

Implicit takeover (D4) is steps 5–7 from inside Update/Read/Lookup when `owner.node = NodeID` and `owner.session ≠ session`.

`Release(ref)`: lock; settle `unresolved` (still unknown → `outcome_unknown`); use `H` if owned, else `GET head`; `owner ≠ self` → return nil. `next = H` with owner null, epoch+1 (tail preserved); `publish`; then discard. Dead with `cur.owner ≠ self` → nil.

`Evict(ref)`: lock; settle `unresolved` (still unknown → `outcome_unknown`); discard (D10); forget the entity. Ownership is unchanged: this session may activate it again.

Entity lifetime (PLAN §11.2): every operation (and each maintenance step) holds a reference to the entity struct, counted under the node mutex. When an operation ends with `unresolved = nil` and a cached head this session does not own, it drops that head (and any local image). When the last reference goes, or the sweeper finds it unreferenced, an entity that is neither resident nor unresolved leaves the node's map; the next operation on the key starts from a new struct. Since a struct leaves only while no operation holds it, one mutex still serializes every operation on the key.

Cache policy: before an activation or a genesis build, if `ResidentEntities` other entities are resident (a busy entity counts only if it has a cache file), discard the least recently admitted one that is idle and settled (never a busy one; wait up to 1 s, and never past the caller's deadline, then `capacity`). Then reserve `4×EntityBytes + 1 MiB` (§4 step 1) against `CacheBytes`, which also counts the other reserves in progress and the database and WAL files of every resident entity (a busy entity with its size when last seen idle or settled): while it does not fit, discard the least recently admitted idle, settled entity; if none is left (all pinned), wait up to 1 s (within the caller's deadline), then `capacity`. The reserve is released once the file is built; the file then counts as resident bytes. A sweeper discards settled caches idle for `IdleTimeout`. Both are D10 discards: ownership is unchanged.

`Close(ctx)`: stop admissions (`closed`); wait for maintenance in flight, lock each entity and settle its `unresolved`, all within ctx (and at most `CommitTimeout` in total), and discard; an entity still busy at the deadline counts as unresolved; delete the session cache dir; return `outcome_unknown` if any candidate stayed unresolved. Never closes the caller's S3 client.

## 8. Open(cfg)

1. Validate `cfg` (`limits.go`).
2. `GET namespace.json`. Found: strict decode. Absent: `LIST entities/` limit 1; anything → `state_missing`; else `PUT_IMM` a new descriptor; `PreconditionFailed`/`Unknown` → GET again and use the stored one if it decodes.
3. Probe under `probe/<NewID()>/`: `PUT_IMM` x twice (second must be `PreconditionFailed`); `CAS` y create (ETag `e1`); `CAS` y with `e1` succeeds; `CAS` y with the now stale `e1` must be `PreconditionFailed`; `GET` returns the second body; `DEL` both. Any deviation → `unsupported` (`Unknown` → `unavailable`).
4. `session = NewID()`. Create `CacheDir/<session-hex>/` with `O_EXCL`, 0700, and hold an exclusive `flock` on a lock file in it. Remove sibling session dirs whose lock can be acquired (older processes); never follow symlinks.

## 9. Maintenance (owner, under the gate, `unresolved = nil`)

Compaction (fanout 8, PLAN §9.1):
1. Pick the oldest index `i` such that `deltas[i..i+8)` all have the same level `L`. None → stop. The tail is never part of a run.
2. GET each input, check length and SHA-256, check the chain (`pre = previous post`, contiguous ranges): ltx's compactor checks neither checksum chains nor SHA-256 (M0).
3. `ltx.Compactor` with `AllowNonContiguousTXIDs=false`, readers bounded by `ref.bytes` and checked for cancellation. Verify the output: range `deltas[i].min..deltas[i+7].max`, pre/post equal the run's ends, `Commit` equal to the last input's.
4. `PUT_IMM` at base `H.revision`; `next = H` with the run replaced by one ref at level `L+1`, tail carried byte-identical; `publish`. Not committed → abandon the object (collection reaps it); never retarget it to another head.

Snapshot: when replay pages (deltas plus tail) exceed `page_count + 256`: checkpointed main file = published image; encode `1..H.sequence`, verify its checksum equals `H.database_checksum`; `PUT_IMM` at base `H.revision`; `next = H` with the new snapshot, no deltas, tail null; `publish`.

## 10. Collect(ref) (PLAN §9.2), any node, no SQLite

1. `GET head` fresh, strict decode; `R = revision`; `live` = snapshot and delta keys (the tail is not an object).
2. `LIST ObjectPrefix(key, inc)` in pages, at most 1000 objects per call.
3. Eligible iff `ParseObjectKey` succeeds, `base < R` and key ∉ `live`. Unparseable keys and other incarnations are never deleted, nor are namespace, birth or head.
4. `DEL` each eligible object; `Unknown` is left for a later pass.

## 11. Export and Import (PLAN §10.2–10.3)

`Export(ref, w)`:
1. Lock; §3 steps 1–4 (validated head `H`, local image).
2. Encode a snapshot LTX `1..H.sequence` of the checkpointed image in memory; its post-checksum must equal `H.database_checksum`.
3. `manifest.json`, canonical JSON ≤ 64 KiB: `format` 1, `namespace`, `key`, `incarnation`, `sequence`, `schema`, `codec`, `page_size`, `page_count`, `database_checksum`, `sha256`, `bytes`.
4. Unlock (the snapshot and manifest are in memory, so a slow writer never blocks the entity). `archive/tar`: `manifest.json` then `snapshot.ltx`, regular files, mode 0600, zero times, then the end-of-archive blocks. Return `Version{inc, H.sequence}`. Any write error: return it; the partial output is not a backup.

`Import(key, req, r)`:
1. Read the tar strictly: exactly `manifest.json` then `snapshot.ltx`, regular files, sizes within 64 KiB and `EntityBytes`-derived bounds, then end of archive and nothing but zero padding to EOF.
2. Check the manifest; SHA-256 and length of the snapshot; bound frame sizes (§1); strictly decode into a temp image; checksum and page count as the manifest; `integrity_check`; `_state_meta` matches the manifest. `req.Schema ≠ manifest.schema` → `schema_mismatch`.
3. Create (§5) with kind 4, genesis built from the image instead of a callback: meta rebased to `(ns, key, new inc)`, `sequence = manifest.sequence + 1`, floor and receipts preserved, receipt `(ID, bound, empty, sequence)` inserted; snapshot `1..sequence`.
