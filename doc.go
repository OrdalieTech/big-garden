// Package garden stores durable SQLite entities in S3.
//
// Each entity is one SQLite database identified by a caller-chosen key. A
// mutation runs a caller callback in a local SQLite transaction, uploads the
// changed pages as an immutable LTX object, and publishes it by a conditional
// replacement of the entity's head object. That compare-and-swap is the
// commit: a successful return means the mutation is recoverable from S3
// alone, after every node and local cache is gone.
//
// Guarantees, per entity: acknowledged mutations are durable; mutations and
// strong reads are linearizable; only the current owner session can publish;
// no unpublished state is ever returned; a Request ID with the same hash
// commits at most once within the receipt retention window; missing or
// corrupt state fails closed and is never recreated empty.
//
// Not provided: cross-entity transactions, durable timers, exactly-once
// callback execution, sandboxing of callbacks, or reads without a storage
// round trip. Callbacks are trusted, synchronous and must not cause external
// effects; a callback may run again after an unacknowledged attempt.
//
// # Extensions
//
// Pages are replicated, not SQL, so an extension whose state lives in the
// SQLite file needs nothing more. Callbacks may create virtual tables of the
// built-in fts5, fts5vocab, rtree, rtree_i32 and geopoly modules, and use the
// JSON and math functions. Config.Extend registers Go functions and virtual
// table modules on the Node's private driver; they may be called from
// statements but not from schema objects (index expressions, triggers,
// views, CHECK, generated columns), so activation never needs them. Native
// extensions (load_extension) are not available. Package vec adds vector
// search over float32 BLOBs:
//
//	cfg.Extend = vec.Register
//	// in an Update: CREATE TABLE docs(id INTEGER PRIMARY KEY, emb BLOB);
//	//               INSERT INTO docs VALUES (1, vec_f32('[0.1, 0.7, 0.2]'))
//	// in a Read:    SELECT id FROM docs ORDER BY vec_distance_cosine(emb, vec_f32(?)) LIMIT 10
//
// Errors are *Error values with a stable Code. CodeOutcomeUnknown means the
// mutation may have committed: retry with the identical Request, never with
// a new ID.
//
// # Ownership
//
// Update, Migrate, Read, Lookup and Export run only on the entity's owner
// session. Reads too: they run on the owner's cache, validated by a head
// request issued after the call began, which makes them strong
// (linearizable) with no read lease. Concurrent reads share one such GET,
// and reads queued behind a publication share its CAS. A Node that does not own the entity, including an unowned
// one (after Release), gets CodeNotOwner with Error.Route set;
// Takeover(*Error.Route) claims it, then retry the identical Request.
// CodeRouteChanged means another session moved the entity first; its
// Error.Route is current. A Node takes over implicitly from earlier sessions
// of its own NodeID (a restarted process), never from other NodeIDs. Close
// does not release ownership; Release does. Resolve costs one GET: keep the
// Ref of each key rather than resolving before every call.
//
//	for {
//		res, err := n.Update(ctx, ref, req, fn) // same req on every attempt
//		var e *garden.Error
//		if !errors.As(err, &e) || e.Code != garden.CodeNotOwner {
//			return res, err
//		}
//		if _, err := n.Takeover(ctx, *e.Route); err != nil &&
//			(!errors.As(err, &e) || e.Code != garden.CodeRouteChanged) {
//			return res, err
//		} // route_changed: retry, the next not_owner carries the new route
//	}
//
// A real loop bounds its attempts: two Nodes taking over from each other
// otherwise trade the entity indefinitely.
package garden
