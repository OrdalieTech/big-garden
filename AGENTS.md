# Working on this repository

Generic Go library: durable SQLite entities over S3. `docs/PLAN.md` is the contract, and `docs/DECISIONS.md` lists the deliberate amendments to it. Read both before changing code. Nothing in here may mention a specific application (no Monogram, Ordalie or CellD names in code or public docs; CellD may be cited only as research in docs).

## Rules

- **Ambitious within scope.** The owner wants everything in the plan done now: do not defer fixes, tests or in-scope features to "later" or to a follow-up; implement and verify them in your workstream. Only out-of-plan features stay out.
- **Minimal code.** Reuse before adding, and never add speculative abstractions, registries, options frameworks or hooks. Every production line counts against the PLAN §12.1 budget.
- **No implicit state machines.** Write straight-line functions under the entity mutex (DECISIONS D8). Prefer close-and-rehydrate over repair.
- **Safety assertions are not negotiable.** Never weaken a test, widen an allowed outcome, raise a limit or skip a check to turn red into green. If a test is wrong, explain why against the contract in the commit message.
- **Pure Go.** `CGO_ENABLED=0` for builds and tests. The race lane is the only exception: `CGO_ENABLED=1 go test -race`.

## This machine (15 GB RAM, shared with other agents)

- Run every `go build`, `go test` and `go vet` as `systemd-run --user --scope -q -p MemoryMax=4G -- env GOFLAGS=-p=2 GOTOOLCHAIN=local CGO_ENABLED=0 go test ...`. Without this, an out-of-memory kill takes down every agent on the machine.
- Run only one heavy job at a time per agent. Test your own packages (`./internal/engine/...`), not `./...`, while other workstreams are editing the tree.
- Keep a progress log at `.work/<your-workstream>.md` (gitignored), so a successor can resume if your session is cut.

## S3

- Local: a pinned real server under rootless podman, image digest recorded in `research.lock.json`. `GARDEN_TEST_S3=1` (the older `GARDEN_TEST_MINIO=1` still works) runs the local S3 tests; `storetest.StartLocal` starts the fixture selected by `GARDEN_TEST_S3_SERVER`, detached with `setsid`:
  - `rustfs` (default): container `state-rustfs`, RustFS (`rustfs_fixture`), host network on 127.0.0.1:9100, tmpfs data.
  - `minio`: container `state-minio`, MinIO (`minio_fixture`), 127.0.0.1:9000/9001. Keep it for the one behavior no other fixture reproduces: a GET blocks ~30 s behind a stalled conditional PUT (docs/PERFORMANCE.md).
  - `qual doctor --local` and `qual stall --local` probe the selected fixture; rerun both after any digest change.
- Remote (Scaleway fr-par): load `~/code/monogram/.env.deploy`, then map `AWS_ACCESS_KEY_ID=$SCW_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY=$SCW_SECRET_KEY`, region `fr-par`, endpoint `https://s3.fr-par.scw.cloud`, bucket `big-garden-qual` (dedicated to qualification since 2026-11-10; never use the `monogram` bucket, which holds live production data). **Only ever write, list or delete under the `qual-state/` prefix.** Under `qual-state/d14/` the bucket policy refuses deleting heads, births and namespace descriptors (docs/OPERATIONS.md). Clean up your random sub-prefix when done. Never print or commit credentials.

## Git

Commit each verified step to `main` as you go (small commits, each stating what was tested); a post-commit hook pushes every commit to github.com/OrdalieTech/big-garden. Never force-push or rewrite pushed history. End every commit message with:

```
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```
