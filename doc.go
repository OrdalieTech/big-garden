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
// Errors are *Error values with a stable Code. CodeOutcomeUnknown means the
// mutation may have committed: retry with the identical Request, never with
// a new ID.
package garden
