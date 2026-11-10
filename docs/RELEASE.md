# Milestone v0.1.0 (2026-11-10)

The implementation is complete against PLAN M0-M9 and the 2026-11-10 review,
and every local safety check passes. It is **not** the PLAN §22 production
release: `qual report` refuses that, because the full-length remote campaigns
and some local campaigns have not run (list at the end). Evidence is regenerated
by `qual report --artifacts=artifacts` (qualification.md, manifest.json); this
file summarizes it at commit `ccd9576` plus this file.

## What it is

A pure-Go library that stores each entity as one SQLite database in S3: a
mutation is a local transaction, an immutable LTX object and one conditional PUT
of the head. Guarantees and non-guarantees: doc.go. Protocol: docs/PROTOCOL.md.
Every amendment to the plan: docs/DECISIONS.md D1-D26. Operating it:
docs/OPERATIONS.md. Measurements: docs/PERFORMANCE.md, docs/COST.md.

## Evidence

- **Fault matrix** (`qual faults`, PLAN §16): 29 of 29 rows pass locally at 100
  repetitions per failpoint; 18 rows fully covered, 11 partially (gaps listed by
  `qual faults --list`). Kill harness lanes, with and without batching, pass on
  the release commit.
- **Model checker**: group commit, shared validation and reads riding a batch,
  follower reads, tombstones and recreation are modelled. Complete without
  violation: D, DT, DU, C0, CD, G, I, BR, F (up to 99 M states each).
  Inconclusive at the state budget, without violation: DR, B and P (150-200 M).
  1 M random traces pass; every one of the 16 negative controls is caught.
- **Soaks, re-verified from the raw histories by `qual check`**:
  - 11.5 h local (2026-11-08): 1.99 M acknowledged writes, 3.66 M strong reads.
  - 3.5 h local and 3.5 h from three Boat hosts on Scaleway fr-par (D21 build):
    597,560 and 449,101 writes.
  - 1 h compressed on the release commit (300 ops/s, a fault every 20 s, 179
    faults): 979,433 operations, 294,251 acknowledged writes.
  - In all of them: no lost, duplicated or phantom write and no
    linearizability violation (1,472 + 1,472 + 398 windows checked).
- **Resources**: the 1 h soak's long-lived node held a 35-50 MiB Go heap with
  31-47 resident entities (the publication-chain leak fixed on 2026-11-10 had
  reached 1.1 GB); its RSS still rose 12% in the last 15 minutes, which only a
  24 h run can tell from warm-up (the §18.3 growth gate is marked fail).
- **Performance**: inside fr-par a mutation is one conditional PUT (p50 34 ms;
  Scaleway's own 4 KiB PUT is 32 ms), a strong read one head GET (p50 4 ms), a
  cold activation 38 ms for 1 MiB and 158 ms for 16 MiB; a warm reactivation
  (D21) 4-29 ms locally. One hot entity: 170 updates/s with 32 writers, 1,409
  reads/s with 16 readers, 168 writes/s and 173 reads/s mixed.
- **Footprint and build**: 4,703 production lines of the 6,000 budget, 15
  exported types and 26 callables, 4 direct and 21 runtime modules, a 13.7 MB
  stripped consumer, no native code; two clean builds (one offline) hash
  identically.

## Open

- **kit directory** (uniqueness claims): designed (.work/FABLE_KIT_DESIGN.md),
  not built until a consumer needs it; backup, router and wakes are built (D26).
- **Campaigns** that `qual report` requires and that have not run on the
  release commit: `qual faults --remote` (20 repetitions), Q0-Q7 ×3 on an idle
  machine, a 24 h local and a 24 h remote soak, the race campaign artifact.
- **Partial fault rows**: EIO beside ENOSPC, a 60 s outage, Export racing
  Collect, a clock jump on the owner, an entity-level 32-way create; seeded
  replay of failing runs.
