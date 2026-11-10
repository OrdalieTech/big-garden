# Cost of S3 traffic: money and time

Numbers with their conditions (PLAN §19). Reproduce with `qual cost`
(built-in counts and prices below), `qual cost --prices aws-eu-west-3`,
`qual cost --egress 1` (compute outside the store's region), or with fresh
counts: `qual stress --local --ops 400 --workers 8 --cold 10 --batch 16
--hot 1,8,32 --out DIR && qual cost --stress DIR/stress.json`. Monthly =
730 h, decimal GB, prices excluding VAT, steady state (creation excluded),
no free credits except the providers' standing egress allowances.

## Prices (fetched 2026-11-08)

| Provider, region | Storage /GB-month | PUT, LIST /1000 | GET /1000 | Egress /GB | Free egress /month | Same-region compute |
|---|---:|---:|---:|---:|---:|---|
| Scaleway fr-par Standard Multi-AZ | €0.01606 (€0.000022/GB-h) | included | included | €0.01 | 75 GB (page text, see below) | free |
| Scaleway fr-par Standard One Zone | €0.00803 (€0.000011/GB-h) | included | included | €0.01 | same | free |
| AWS us-east-1 S3 Standard (first 50 TB) | $0.023 | $0.005 | $0.0004 | $0.09 (first 10 TB) | 100 GB, all services | free |
| AWS eu-west-3 S3 Standard (first 50 TB) | $0.024 | $0.0053 | $0.00042 | $0.09 (first 10 TB) | 100 GB, all services | free |
| PLAN §19.2 hypothetical (not a quote) | €0.03 | €0.005 | €0.0004 | €0.09 | 0 | - |

Sources:
- Scaleway: the public product catalog API,
  `https://api.scaleway.com/product-catalog/v2alpha1/public-catalog/products`.
  It has the per-GB-hour storage rates (SKUs `/storage/obj/usage-new-gen-bucket-standard/fr-par`
  and `/storage/obj/usage-onezone_ia/fr-par`), egress to external at €0.01/GB,
  egress to fr-par/internal at €0, and no request items.
- Scaleway pricing page, https://www.scaleway.com/en/pricing/storage/. It
  returned 403 to automated fetches, so its text was read through the search
  index only: €0.01606/GB-month, "Request: Included", 75 GB free egress, and
  intra-regional PAR↔PAR traffic free of charge. Treat the 75 GB and
  "included" as **not directly confirmed** (the catalog agrees on requests).
  A rate change for One Zone took effect on 2026-06-01:
  https://www.scaleway.com/en/blog/a-transparent-update-on-scaleway-pricing/.
- Scaleway Glacier is listed at €0.00246/GB-month on the page, but the
  catalog shows €0.000082 with no time unit. It is **unconfirmed** and
  irrelevant here, since the hot path cannot use it.
- AWS: the Price List API,
  `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/{us-east-1,eu-west-3}/index.json`
  (published 2026-11-04) and `.../AWSDataTransfer/current/...` (2026-11-03).
  S3 to EC2 in the same region is free per https://aws.amazon.com/s3/pricing/.
  A conditional GET (304/412) is billed as a normal GET, failed requests
  included, per
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html.
- Ingress (uploads) is free on both providers.
- Not verified: whether Scaleway aggregates GB-hours per bucket or applies
  a per-object minimum (PLAN §19.2). No minimum is published for Standard
  classes.

## Measured traffic per operation

`qual stress --local` against the pinned RustFS fixture, B=16, 2026-11-08.
Request and byte counts are set by the protocol, not by the server: the
Scaleway run in docs/PERFORMANCE.md gives the same 1.07 PUTs and 29.6 KB
per inline update as the 1.07 PUTs and 30.2 KB measured here.
Maintenance is included.

| Operation | PUT (incl. CAS) | GET | Up | of which objects | Down |
|---|---:|---:|---:|---:|---:|
| 1 KiB update, B=1, inline tail (`inline-seq`, 400 on one entity) | 1.07 | 0.06 | 30.2 KB | 2.5 KB | 2.1 KB |
| 1 KiB update, 8 writers on one entity, B=8 (`hot-8`) | 0.19 | 0.06 | 5.2 KB | 2.6 KB | 2.2 KB |
| 1 KiB update, 32 writers, B=16 (`hot-32`) | 0.115 | 0.02 | 3.3 KB | 2.4 KB | 0.8 KB |
| 48 KiB update, upload path (`upload-seq`) | 2.28 | 0.88 | 121 KB | 117 KB | 48.8 KB |
| strong read, unchanged (`read-conc`) | 0 | 1 (304) | 0 | 0 | 0 |
| create (birth + genesis + head) | 3 | 2 | 2.1 KB | 1.2 KB | 0.6 KB |
| cold activation, 1 MiB (snapshot + 2 deltas) | 0 | 4 | 0 | 0 | 1.066 MB |
| cold activation, 16 MiB | 0 | 6 | 0 | 0 | 17.0 MB |

So a cold activation downloads **1.013 MB per MiB of entity + 4 KB** (the
fixture is incompressible), in 4 to 6 GETs. Compaction reads its input
deltas back from S3. That is the Down column of the write rows: 2 KB per
inline write and 49 KB per 48 KiB write.

S3 time, Scaleway fr-par from one host (docs/PERFORMANCE.md):
- Raw requests, p50 / p95: PUT 33 / 82 ms, CAS 35 / 110 ms, GET 9 / 15 ms.
- Library operations, p50 / p95: inline update 38 / 76 ms, upload update
  76 / 278 ms, strong read 7.6 / 11.4 ms, cold activation 71 / 122 ms
  (1 MiB) and 581 / 639 ms (16 MiB).

`qual cost` reports request-seconds at p50 (PUT 35 ms, GET 9 ms). Divided
by the month, that is the mean number of S3 requests in flight. Size the
connection pool and `Limits` concurrency from it.

## Monthly cost at four scales

All entities are 1 MiB. Superseded objects stay stored until `Collect`,
run every 24 h (1 h for "hot"). Activations are cold. `qual cost`
scenarios:

| Scenario | Load | Scaleway fr-par, same region | Scaleway, compute outside fr-par | AWS eu-west-3, same region | S3 in flight |
|---|---|---:|---:|---:|---:|
| small: 1k entities, 1 write/min + 1 read/min each | 17 w/s, 17 r/s, 0.01 act/s | **€0.07** (storage) | €0.53 (egress) | **$268** (PUT) | 0.8 |
| fleet: 10k entities, hot 5% at 1 w/s + 1 r/s | 500 w/s, 500 r/s, 1 act/s | **€1.90** (storage, 91% uncollected) | €56.87 (egress) | **$8,044** (PUT $7,452) | 23.5 |
| hot: 100 entities x 50 w/s, B=8 | 5,000 w/s, 500 r/s | **€0.76** (storage) | €284 (egress, mostly compaction reads) | **$14,291** (PUT $13,406) | 40.9 |
| read-heavy: 10k entities, hot 10%, 70% reads / 30% writes | 300 w/s, 700 r/s, 2 act/s | **€1.21** (storage) | €73.10 (egress) | **$5,274** (PUT $4,471) | 17.8 |

Dominant terms:
- **Scaleway, same region:** only storage is billed. At these sizes the
  superseded-but-uncollected objects (108 GB for fleet with daily Collect)
  outweigh the 10.6 GB of live state. Collect hourly and fleet's retained
  bytes drop to 4.5 GB, for €0.24 a month in total.
- **Scaleway, compute outside fr-par:** egress dominates. It is every
  downloaded byte (activations + compaction reads) at €0.01/GB after 75 GB.
- **AWS:** PUT requests dominate everywhere: $5.70 per million B=1 writes
  (eu-west-3) and $0.42 per million strong reads. The read-heavy mix is
  still PUT-dominated, because one write costs about 13 reads.
  us-east-1 is about 6% cheaper; the PLAN hypothetical table lands within
  6% of AWS.

Per million logical ops (AWS eu-west-3): $3.06 (small, fleet), $2.01
(read-heavy), $0.99 (hot, batched). On Scaleway the whole bill is a few
euros a month: the cost of state is storage plus compute, and S3 traffic
costs **time**, not money.

## What moves the bill

- **Group commit (D18).** Measured PUTs per write: 1.07 at B=1, 0.19 at
  B=8, 0.115 at B=16. The hot scenario on AWS costs $75,576 a month at
  B=1 (theoretical: 50 w/s on one entity exceeds the ~25 w/s B=1 ceiling
  on fr-par), $14,291 at B=8 and $8,673 at B=16. Uploaded bytes fall
  about 6x too. Batching only happens when writers queue, so a lone writer
  pays B=1.
- **Inline tail (D16/D17).** It halves PUTs: 1.07 per small write against
  2.28 on the upload path. On AWS that is $5.70 instead of about $12 per
  million writes. The price is about 30 KB uploaded per write, because the
  head re-sends the tail. Ingress is free on both providers, and the time
  cost is +3 ms p50 over a raw CAS. Keep it.
- **Maintenance: tail flushes, compaction (fanout 8), snapshots.** It adds
  0.07 PUT + 0.06 GET per inline write and 0.28 PUT + 0.88 GET per 48 KiB
  write. It re-downloads its inputs (2
  to 49 KB per write), which only costs money when egress is billed. It
  bounds activations to 1 snapshot + at most 32 deltas, which keeps
  activation GETs at 4 to 6.
- **Collect cadence.** Retained bytes = object bytes written x the Collect
  interval: 2.5 KB per inline write and 117 KB per 48 KiB write. This is
  the main Scaleway storage knob.
- **Entity size.** It sets the activation download (1.013 MB/MiB) and live
  storage. It does not affect per-write cost: an inline update costs the
  same on a 64 KiB or a 16 MiB entity.
- **Placement.** Compute in the store's region makes every download free
  on both providers. Outside it, egress dominates on Scaleway.

## Warm local cache tier (D21, measured)

A cold reactivation, the first access after eviction or a restart,
downloads the snapshot and every delta. With D21 an idle entity parks its
file; the next activation does the head GET it always does (a 200: a warm
entity has no decoded head to send a 304 against), then:
- **Warm hit:** the file is at the head. No object GET; the local checksum
  and `integrity_check` gate it.
- **Catch-up:** another node wrote since. GET only the links after the
  file's sequence (often none: the inline tail is in the head), overlay
  them, same gate.
- **Cold:** a snapshot newer than the file, an unknown earlier link, or a
  file failing the gate: today's full restore.

Measured with `GARDEN_MEASURE=1 GARDEN_TEST_S3=1 go test -run
TestWarmActivationCost` (local RustFS on tmpfs, so the S3 round trips are
~1 ms; medians of 7; catch-up and cold include a takeover CAS):

| Per reactivation | Cold | Warm hit | Catch-up after 3 updates elsewhere |
|---|---|---|---|
| 1 MiB entity, local | 22.6 ms, 1.07 MB of objects | 4.1 ms, 0 B | 10.6 ms, 0 B (tail in the head) |
| 16 MiB entity, local | 211 ms, 17.0 MB | 29 ms, 0 B | 35 ms; 6 of 7 caught up, 1 went cold (an earlier link compacted by the writer, unknown to the hint) |
| 1 MiB on Scaleway fr-par | 71 / 122 ms (p50 / p95, measured) | ≈ one head GET (8-11 ms) + 4 ms | ≈ head GET + takeover + 11 ms |
| 16 MiB on Scaleway fr-par | 581 / 639 ms (measured) | ≈ head GET + 29 ms | ≈ head GET + 35 ms |

The 16 MiB warm hit is the full-file checksum and `integrity_check`; under
50 ms it does not justify a second path that skips them.

Savings by scenario, if every activation becomes a warm hit:

| Scenario | Bytes avoided per month | Time | Scaleway, same region | Scaleway, egress billed | AWS eu-west-3, same region |
|---|---|---|---|---|---|
| small (0.01 act/s) | 28 GB | 26k activations x ~62 ms ≈ 0.45 h | €0 | ≤ €0.28 | $0.04 → $0.01 |
| fleet (1 act/s) | 2.8 TB (8.5 Mbit/s sustained) | 2.6M x ~62 ms ≈ 45 h | €0 | €28 | $4.42 → $1.10 |
| hot (no activations) | 0 | 0 | 0 | 0 | 0 |
| read-heavy (2 act/s) | 5.6 TB (17 Mbit/s) | ≈ 90 h | €0 | €56 | $8.83 → $2.21 |

With 16 MiB entities the bytes and seconds scale by about 16x: fleet would
avoid 45 TB a month, 136 Mbit/s sustained. So the warm tier is a
**latency and bandwidth** win, cutting a reactivation from tens or
hundreds of ms to one GET RTT. It is a money win only when egress is
billed. On AWS the money saved is three GET requests per activation,
negligible next to PUTs. It never removes the strong-read head GET, which
is already 304 and 0 bytes on a hot entity.

## Monitoring: counters that exist and gaps

Existing `Stats` counters:
- **Operations:** Reads, Mutations, Commits, Replays, Conflicts,
  rejections, UnknownOutcomes, Batches[16].
- **Activations:** Activations, ActivationFailures, and (D21) WarmHits, CatchUps, ActivationBytes, WarmEvictions.
- **Maintenance:** Snapshots/SnapshotBytes, Compactions and compaction
  input/output bytes, Collections/CollectedObjects/CollectedBytes.
- **Storage requests:** CASAttempts/CASConflicts/AmbiguousPuts, and
  Storage[GET, PUT, LIST, DELETE] attempts, failures, uploaded and
  downloaded bytes, and Time spent in those calls (retries included):
  the time cost of S3, as a mean latency per class over any interval.
- **Gauges:** CacheBytes, StagingBytes, ResidentEntities, InflightStorage, WarmEntities, WarmBytes.

From these you get PUT and GET counts (the AWS bill) and total downloaded
bytes (the egress upper bound). Object PUTs = PUT attempts − CASAttempts.

With D21: `WarmHits` and `CatchUps` count activations from a warm file
(cold = `Activations` − both), `ActivationBytes` the object bytes they
downloaded, `WarmEvictions` warm files deleted for space, and the gauges
`WarmEntities`/`WarmBytes` the tier itself.

Still missing to watch cost in production:
- **Head GET outcomes.** 304 vs 200 vs 404. Head GETs and object GETs share
  `Storage[GET]`; `qual stress` counts them on the wire only.
- **Downloaded bytes by purpose.** Activation, compaction input and
  resolution/read. Compaction's re-download of its inputs is invisible
  except through `CompactionInputBytes`, which counts logical input,
  whether or not it was downloaded.
- **Stored bytes.** A gauge of live referenced bytes and of superseded,
  not-yet-collected bytes. These drive the Scaleway bill, and Collect
  cadence is the knob. Today only bytes collected are counted.
- **Creations.** Birth + genesis + head cost 3 PUT + 2 GET + 1 LIST,
  counted today among Mutations.
- **Egress.** Which bytes are billable depends on placement and cannot be
  known in-library. Downloaded bytes is the right upper bound.
