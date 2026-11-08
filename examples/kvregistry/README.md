# kvregistry

A key/value registry per tenant, one big-garden entity per tenant, with
full-text search over the values and vector similarity over an embedding of
each value. The embeddings are a deterministic stand-in (hashed words), so the
example needs no model.

It shows:

- an FTS5 virtual table kept in step with the `kv` table by triggers: the
  index is pages of the entity, so nothing else is replicated;
- `Config.Extend = vec.Register`, vectors written with `vec_f32(json)` and
  nearest neighbours found with `ORDER BY vec_distance_cosine(emb, ?)` over a
  little-endian float32 BLOB; Extend functions are used in statements only,
  never in the triggers;
- query results returned as the `Read` result value (bounded by
  `Limits.ResultBytes`);
- recovery: after `Close`, a Node with another NodeID and an empty cache dir
  takes the tenant over and finds every value, search hit and neighbour again;
- `Export` and `Import` of a tenant, searchable in the copy.

## Run

```sh
export AWS_ACCESS_KEY_ID=staterustfs AWS_SECRET_ACCESS_KEY=staterustfs-secret
go build -o kvregistry ./examples/kvregistry
./kvregistry acme create
./kvregistry acme put fruit "bananas and apples"
./kvregistry acme put car "a red sports car, fast"
./kvregistry acme search 'sport*'
./kvregistry acme near "apples bananas" 1
./kvregistry -node kv-b -cache /tmp/empty acme get fruit
```

Test:

```sh
GARDEN_TEST_S3=1 go test ./examples/kvregistry
```
