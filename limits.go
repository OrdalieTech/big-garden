package garden

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/OrdalieTech/big-garden/internal/head"
)

// Limits bounds the resources of one Node. A zero field selects its
// default; negative, overflowing or mutually inconsistent values are
// rejected by Open. Protocol-format limits (64 KiB heads, 32 deltas, 4 KiB
// pages) are fixed and not configurable here. Defaults [bounds]:
//
//	EntityBytes            64 MiB [4 KiB..256 MiB]  database size
//	CacheBytes             8 GiB  local cache; >= Recoveries*(4*EntityBytes+1 MiB)
//	ResultBytes            16 KiB [..64 KiB]        a callback's result (Result.Value)
//	ResidentEntities       256                      open entities
//	InflightStorage        64                       concurrent storage requests
//	PendingPerEntity       64                       queued requests per entity
//	Recoveries             8                        concurrent activations
//	BatchMax               16 [..16]                mutations per publication (1: no batching)
//	StorageAttemptTimeout  2s                       one storage request
//	CommitTimeout          10s [>= StorageAttemptTimeout]
//	IdleTimeout            60s                      idle entity eviction
//	ReceiptRetention       1h [1h..720h]            receipts kept; older IDs get id_expired
type Limits struct {
	EntityBytes, CacheBytes                        int64
	ResultBytes, ResidentEntities, InflightStorage int
	PendingPerEntity, Recoveries, BatchMax         int
	StorageAttemptTimeout, CommitTimeout           time.Duration
	IdleTimeout, ReceiptRetention                  time.Duration
}

// Defaults and qualified upper bounds (PLAN §11.2, DECISIONS D6).
const (
	defaultEntityBytes   = 64 << 20
	maxEntityBytes       = head.MaxPages * head.PageSize // 256 MiB
	defaultCacheBytes    = 8 << 30
	defaultResultBytes   = 16 << 10
	maxResultBytes       = 64 << 10
	maxCount             = 1 << 20 // keeps every product below overflow
	maxBatch             = 16
	minReceiptRetention  = time.Hour
	maxReceiptRetention  = 30 * 24 * time.Hour
	defaultRetention     = time.Hour
	recoveryHeadroomMult = 4 // database + snapshot download + replay deltas below 2x live (§9.1)
)

// resolve returns l with defaults applied, or an invalid_argument Error.
func (l Limits) resolve() (Limits, error) {
	for _, v := range []int64{l.EntityBytes, l.CacheBytes, int64(l.StorageAttemptTimeout),
		int64(l.CommitTimeout), int64(l.IdleTimeout), int64(l.ReceiptRetention)} {
		if v < 0 {
			return l, invalid("limits must not be negative")
		}
	}
	for _, v := range []int{l.ResultBytes, l.ResidentEntities, l.InflightStorage, l.PendingPerEntity, l.Recoveries, l.BatchMax} {
		if v < 0 || v > maxCount {
			return l, invalid(fmt.Sprintf("count limits must be within 0..%d", maxCount))
		}
	}
	orDefault(&l.EntityBytes, int64(defaultEntityBytes))
	orDefault(&l.CacheBytes, int64(defaultCacheBytes))
	orDefault(&l.ResultBytes, defaultResultBytes)
	orDefault(&l.ResidentEntities, 256)
	orDefault(&l.InflightStorage, 64)
	orDefault(&l.PendingPerEntity, 64)
	orDefault(&l.Recoveries, 8)
	orDefault(&l.BatchMax, maxBatch) // group commit on (DECISIONS D18)
	orDefault(&l.StorageAttemptTimeout, 2*time.Second)
	orDefault(&l.CommitTimeout, 10*time.Second)
	orDefault(&l.IdleTimeout, 60*time.Second)
	orDefault(&l.ReceiptRetention, defaultRetention)
	switch {
	case l.EntityBytes < head.PageSize || l.EntityBytes > maxEntityBytes:
		return l, invalid(fmt.Sprintf("EntityBytes must be within %d..%d", head.PageSize, maxEntityBytes))
	case l.ResultBytes > maxResultBytes:
		return l, invalid(fmt.Sprintf("ResultBytes must be at most %d", maxResultBytes))
	case l.BatchMax > maxBatch:
		return l, invalid(fmt.Sprintf("BatchMax must be at most %d", maxBatch))
	case l.CommitTimeout < l.StorageAttemptTimeout:
		return l, invalid("CommitTimeout must be at least StorageAttemptTimeout")
	case l.ReceiptRetention < minReceiptRetention || l.ReceiptRetention > maxReceiptRetention:
		return l, invalid("ReceiptRetention must be within 1h..720h")
	case l.CacheBytes/int64(l.Recoveries) < recoveryHeadroomMult*l.EntityBytes+1<<20:
		return l, invalid("CacheBytes cannot hold Recoveries concurrent activations of EntityBytes")
	}
	return l, nil
}

// validate checks c and returns it with resolved Limits.
func (c Config) validate() (Config, error) {
	var err error
	switch {
	case c.S3 == nil:
		return c, invalid("Config.S3 is required")
	case c.Bucket == "":
		return c, invalid("Config.Bucket is required")
	case !validPrefix(c.Prefix):
		return c, invalid("Config.Prefix must be 1..256 bytes of UTF-8 '/'-separated segments, none empty, '.' or '..'")
	case c.CacheDir == "":
		return c, invalid("Config.CacheDir is required")
	}
	if err = head.ValidNode(c.NodeID); err != nil {
		return c, invalid(err.Error())
	}
	c.Limits, err = c.Limits.resolve()
	return c, err
}

func validPrefix(p string) bool {
	if p == "" || len(p) > 256 || !utf8.ValidString(p) {
		return false
	}
	for _, s := range strings.Split(p, "/") {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}
	return true
}

func orDefault[T comparable](v *T, d T) {
	var zero T
	if *v == zero {
		*v = d
	}
}

func invalid(msg string) *Error { return newError(CodeInvalidArgument, msg, nil) }
