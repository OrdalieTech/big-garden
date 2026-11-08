# counter

A CLI keeping named counters, one big-garden entity per counter. Each run
opens a Node, runs one command and closes, so two runs with different
`-node` IDs are two processes competing for ownership.

It shows:

- `Create` of an entity at schema 1, and a `Migrate` to schema 2 (a column
  added), explicit (`migrate`) or lazy on the first `schema_mismatch`;
- `Update` with a caller-held operation ID: `incr -op HEX` retries an earlier
  request and gets its stored result back (`"replayed":true`); the same ID with
  other input is refused (`id_conflict`);
- strong `Read` (`get`) and `Lookup` of an operation ID;
- takeover: a Node that gets `not_owner` calls `Takeover` with the Route of the
  error and retries the same request (`"took_over":true`); a new process with
  the same NodeID takes over its previous session by itself;
- `Export` to a tar file and `Import` of it under another key.

## Run

Against the local RustFS fixture (`127.0.0.1:9100`, bucket `state-test`):

```sh
export AWS_ACCESS_KEY_ID=staterustfs AWS_SECRET_ACCESS_KEY=staterustfs-secret
# optional: GARDEN_S3_ENDPOINT, GARDEN_S3_REGION, GARDEN_S3_BUCKET, GARDEN_S3_PREFIX
go build -o counter ./examples/counter
./counter -node a create foo
./counter -node a incr -by 2 foo        # prints "op":"<hex>"
./counter -node a incr -by 2 -op <hex> foo   # replayed, same value and sequence
./counter -node b incr foo              # b takes over from a
./counter -node a get foo               # a takes it back
./counter -node a export foo foo.tar
./counter -node a import foo-copy foo.tar
```

Known issue (main 0f91e4f): `Export` of an entity this Node activated from S3
(a fresh process, or after `Evict`) fails with `corrupt: snapshot differs from
the head`; it succeeds only after a mutation in the same session. The test's
`export-import` subtest fails until this is fixed in the library.

Test (builds the binary and runs it as two nodes):

```sh
GARDEN_TEST_S3=1 go test ./examples/counter
```
