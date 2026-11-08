# Measured performance and store behavior

Numbers with their conditions. Raw logs stay out of the repository; rerun the
commands to reproduce. Remote = Scaleway Object Storage fr-par, bucket in
production use, client on one workstation in France over the public
internet, prefix `qual-state/<random>/`, deleted afterwards.

## Stalled PUT versus GET (2026-11-08, `qual stall --remote --stall 40s`)

A raw signed PUT declares Content-Length, sends half its 2 KiB body, stalls
40 s, then completes. GETs of the key from fresh connection pools start 1 s
into the stall. "Racer" is a competing CAS with the same condition sent
during the stall.

| Condition | Scaleway fr-par: GET during stall | Scaleway: stalled PUT | Pinned MinIO: GET during stall | MinIO: stalled PUT |
|---|---|---|---|---|
| If-Match current ETag | 22-34 ms, old value | 200 at 40 s | **29.3 s**, then old value | 503 |
| unconditional | 21-32 ms, old value | 200 at 40 s | 1-5 ms | 503 |
| If-None-Match: * (absent key) | 25-32 ms, not found | 200 at 40 s | **29.3 s**, then not found | 503 |
| If-Match + racer | 19-33 ms; racer CAS OK in 23 ms | **412** at 40 s; racer's value stored | 29.3 s; racer OK | 503; racer's value |
| If-None-Match + racer | 25-27 ms; racer create OK in 31 ms | **412** at 40 s; racer's value stored | 29.3 s; racer OK | 503; racer's value |

On Scaleway a stalled conditional write blocks neither readers nor
competing writers, and its condition is evaluated when the body completes
(so a stalled owner's late CAS loses to a takeover). The ~30 s read
unavailability after an owner stalls mid-CAS is MinIO-only.

## Library latency on Scaleway (2026-11-08, `qual stress --remote --ops 400 --workers 8 --cold 10`)

One node on the workstation, default limits except EntityBytes 32 MiB and
CacheBytes 2 GiB (StorageAttemptTimeout 2 s, so the D3 hedge fires after 1 s),
B=1, incompressible values. Inline = 1 KiB value per update (D16 inline
tail); upload = 48 KiB value per update (object PUT then head CAS); strong
read = `SELECT count(*)` (one conditional head GET, D7); cold = Evict then
Read. PUTs and bytes include background maintenance during the phase.

| Phase | n | p50 | p95 | p99 | max | errors / unknown | PUTs per mutation | uploaded per mutation |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| inline update, sequential | 400 | 38.3 ms | 76.4 ms | 173 ms | 318 ms | 0 / 0 | 1.07 | 29.6 KB |
| inline update, 8 clients on 8 entities | 400 | 40.0 ms | 115 ms | 338 ms | 363 ms | 0 / 0 | 1.03 | 21.2 KB |
| upload update (48 KiB), sequential | 100 | 75.6 ms | 278 ms | 889 ms | 889 ms | 0 / 0 | 2.26 | 118 KB |
| strong read, sequential | 400 | 7.6 ms | 11.4 ms | 16.2 ms | 23.3 ms | 0 / 0 | - | - |
| strong read, 8 clients | 400 | 7.4 ms | 12.2 ms | 15.7 ms | 19.7 ms | 0 / 0 | - | - |
| fill 16 MiB (512 KiB updates) | 33 | 92.3 ms | 558 ms | 744 ms | 744 ms | 0 / 0 | 2.21 | 909 KB |
| cold activation, 1 MiB (2 deltas) | 10 | 71.0 ms | 122 ms | 122 ms | 122 ms | 0 / 0 | - | - |
| cold activation, 16 MiB | 10 | 581 ms | 639 ms | 639 ms | 639 ms | 0 / 0 | - | - |

Hedges: 0 of 963 head CAS in this run; 2 of 507 (0.4%) in an earlier
`--ops 200` run, each in an operation that took over 1 s (inline max
1.04 s, upload max 2.36 s; medians within 20% of the table above).

Raw store, same endpoint (`qual doctor --remote --latency-samples`, earlier
run): PUT 4 KiB 33/82/282 ms, CAS 35/110/307 ms, GET 9/15/25 ms
(p50/p95/p99). An inline update costs about one CAS (+3 ms p50), a strong
read about one GET, an upload update about one PUT plus one CAS. Against the
PLAN §18.2 healthy-remote targets (180/450/1000 ms mutation, 80/180/400 ms
strong read, 2 s p95 1 MiB cold activation) every phase of this run
passes; in the earlier run the upload-path p99 (the max of 50, 2.36 s,
hedged) exceeded 1 s.
Small n, one client host: indicative, not a qualification.

An inline 1 KiB update uploads about 21-30 KB: each head CAS carries the
whole inline tail (up to 31 KiB, D16) until it is flushed to an object.

## Suites run against Scaleway (2026-11-08)

`GARDEN_TEST_MINIO=1 GARDEN_TEST_REMOTE=1` points every `storetest.MinIO`
caller at `QUAL_S3_*`. Root core suite (create/update inline and upload,
read, lookup, takeover/release, migrate, export/import, collect, restart
with an empty cache, fault-scripted resolve branches through the proxy):
all pass in 88 s. `internal/store`: all pass. Kill harness
(`GARDEN_KILL_RUNS=10`, then 20 for the M5 tests): TestProcessKill (10
failpoints), TestStaleOwner, TestConcurrentTakeover, TestSameIDSeveralNodes,
TestRoutedHistories (40 histories, 3321 ops, all linearizable, 0
inconclusive) pass. TestProcessKill net.lost/net.late cannot run remotely
yet: the node process addresses the proxy as its endpoint, so the signed
Host is the proxy's.
