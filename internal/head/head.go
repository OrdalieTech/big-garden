// Package head holds the strict format-1 codecs, object keys and invariants
// of the three JSON descriptors stored in S3: namespace.json, birth.json and
// head.json (PLAN §5.1-5.2, docs/PROTOCOL.md).
//
// Decoding is canonical: a descriptor is accepted only if re-encoding the
// decoded value reproduces the input byte for byte. That single rule rejects
// duplicate keys, unknown or differently-cased fields, missing fields, null
// for non-nullable values, floats and exponents, non-canonical escapes,
// insignificant whitespace, excessive nesting and trailing bytes. Integers
// decode into explicit unsigned Go types, never through float64.
package head

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Fixed format-1 protocol limits. None of them is configurable.
const (
	Format         = 1
	Codec          = "ltx-v3"
	PageSize       = 4096
	MaxPages       = 256 << 20 / PageSize // 256 MiB qualified hard ceiling
	MaxDeltas      = 32
	MaxLevel       = 15
	MaxKeyBytes    = 512
	MaxNodeBytes   = 128
	MaxInt         = 1<<63 - 1                      // revisions, epochs, sequences
	MaxObjectBytes = MaxPages*(PageSize+64) + 1<<20 // LTX frame overhead plus index
	InlineBytes    = 31 << 10                       // largest inline tail (raw LTX bytes), D16
	ChecksumFlag   = Checksum(1 << 63)              // set on every LTX database checksum
	maxHeadBytes   = 64 << 10
	maxBirthBytes  = 4 << 10
	maxNSBytes     = 1 << 10
)

// ID is a 128-bit random identifier: namespace, incarnation, session, nonce
// and attempt. It encodes as 32 lower-case hex characters.
type ID [16]byte

// OpID is a MutationID: 8-byte big-endian Unix ms + 16 random bytes (48 hex).
type OpID [24]byte

// Digest is a SHA-256 value (64 hex).
type Digest [32]byte

// Checksum is an LTX rolling database checksum (16 hex).
type Checksum uint64

// NewID returns a fresh cryptographically random ID.
func NewID() ID {
	var id ID
	rand.Read(id[:]) // never returns an error since Go 1.24
	return id
}

func (v ID) String() string                     { return hex.EncodeToString(v[:]) }
func (v ID) MarshalText() ([]byte, error)       { return hex.AppendEncode(nil, v[:]), nil }
func (v *ID) UnmarshalText(t []byte) error      { return unhex(v[:], t) }
func (v OpID) MarshalText() ([]byte, error)     { return hex.AppendEncode(nil, v[:]), nil }
func (v *OpID) UnmarshalText(t []byte) error    { return unhex(v[:], t) }
func (v Digest) String() string                 { return hex.EncodeToString(v[:]) }
func (v Digest) MarshalText() ([]byte, error)   { return hex.AppendEncode(nil, v[:]), nil }
func (v *Digest) UnmarshalText(t []byte) error  { return unhex(v[:], t) }
func (c Checksum) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "%016x", uint64(c)), nil }
func (c *Checksum) UnmarshalText(t []byte) error {
	var b [8]byte
	if err := unhex(b[:], t); err != nil {
		return err
	}
	*c = Checksum(binary.BigEndian.Uint64(b[:]))
	return nil
}

// unhex decodes exactly len(dst) bytes of lower-case hex.
func unhex(dst, t []byte) error {
	if len(t) != 2*len(dst) {
		return fmt.Errorf("hex value has %d characters, want %d", len(t), 2*len(dst))
	}
	for _, c := range t {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return errors.New("hex value is not lower-case hex")
		}
	}
	_, err := hex.Decode(dst, t)
	return err
}

// Namespace is the immutable namespace.json descriptor.
type Namespace struct {
	Format       uint32 `json:"format"`
	Namespace    ID     `json:"namespace"`
	EntityFormat uint32 `json:"entity_format"`
}

// Birth is the immutable birth.json descriptor written once per incarnation.
type Birth struct {
	Format      uint32 `json:"format"`
	Namespace   ID     `json:"namespace"`
	Key         string `json:"key"`
	Incarnation ID     `json:"incarnation"`
	OperationID OpID   `json:"operation_id"`
	InputHash   Digest `json:"input_hash"`
	Schema      uint32 `json:"schema"`
	Attempt     ID     `json:"attempt"`
}

// Head is the head.json descriptor: ownership fence plus committed root.
type Head struct {
	Format           uint32   `json:"format"`
	Namespace        ID       `json:"namespace"`
	Key              string   `json:"key"`
	Incarnation      ID       `json:"incarnation"`
	Revision         uint64   `json:"revision"`
	Nonce            ID       `json:"nonce"`
	Owner            *Owner   `json:"owner"`
	Epoch            uint64   `json:"epoch"`
	Sequence         uint64   `json:"sequence"`
	Schema           uint32   `json:"schema"`
	Codec            string   `json:"codec"`
	PageSize         uint32   `json:"page_size"`
	PageCount        uint32   `json:"page_count"`
	DatabaseChecksum Checksum `json:"database_checksum"`
	Snapshot         Ref      `json:"snapshot"`
	Deltas           []Ref    `json:"deltas"`
	Tail             *Tail    `json:"tail"`
}

// Owner identifies the session allowed to publish; nil in a Head means
// released (unowned).
type Owner struct {
	Node    string `json:"node"`
	Session ID     `json:"session"`
}

// Ref names one immutable LTX object and the sequence range it covers.
type Ref struct {
	Key          string   `json:"key"`
	SHA256       Digest   `json:"sha256"`
	Bytes        uint64   `json:"bytes"`
	BaseRevision uint64   `json:"base_revision"`
	MinSequence  uint64   `json:"min_sequence"`
	MaxSequence  uint64   `json:"max_sequence"`
	PreChecksum  Checksum `json:"pre_checksum"`
	PostChecksum Checksum `json:"post_checksum"`
	Level        uint32   `json:"level"`
}

// Tail is one LTX delta stored inline in the head (D16): the last link of the
// chain, after the delta refs, ending at the head's sequence. Data encodes as
// standard padded base64; nil in a Head means no tail.
type Tail struct {
	SHA256       Digest   `json:"sha256"`
	Bytes        uint64   `json:"bytes"`
	MinSequence  uint64   `json:"min_sequence"`
	MaxSequence  uint64   `json:"max_sequence"`
	PreChecksum  Checksum `json:"pre_checksum"`
	PostChecksum Checksum `json:"post_checksum"`
	Data         []byte   `json:"data"`
}

// Ref returns the tail as a key-less Ref (no key, base revision or level) for
// code that checks and applies an LTX by its hash, length, range and checksums.
func (t *Tail) Ref() Ref {
	return Ref{SHA256: t.SHA256, Bytes: t.Bytes, MinSequence: t.MinSequence, MaxSequence: t.MaxSequence,
		PreChecksum: t.PreChecksum, PostChecksum: t.PostChecksum}
}

// DecodeNamespace strictly decodes and validates namespace.json.
func DecodeNamespace(b []byte) (Namespace, error) {
	var n Namespace
	err := decode(b, maxNSBytes, &n, func() error { return n.validate() })
	return n, err
}

// DecodeBirth strictly decodes and validates birth.json.
func DecodeBirth(b []byte) (Birth, error) {
	var v Birth
	err := decode(b, maxBirthBytes, &v, func() error { return v.validate() })
	return v, err
}

// Decode strictly decodes and validates head.json.
func Decode(b []byte) (Head, error) {
	var h Head
	err := decode(b, maxHeadBytes, &h, func() error {
		if h.Deltas == nil {
			return errors.New("deltas must be an array")
		}
		return h.Validate()
	})
	return h, err
}

// Encode validates n and returns its canonical bytes.
func (n Namespace) Encode() ([]byte, error) { return encode(n, maxNSBytes, n.validate) }

// Encode validates b and returns its canonical bytes.
func (b Birth) Encode() ([]byte, error) { return encode(b, maxBirthBytes, b.validate) }

// Encode validates h and returns its canonical bytes. A nil Deltas encodes
// as an empty array.
func (h Head) Encode() ([]byte, error) {
	if h.Deltas == nil {
		h.Deltas = []Ref{}
	}
	return encode(h, maxHeadBytes, h.Validate)
}

func decode(b []byte, limit int, v any, validate func() error) error {
	if len(b) > limit {
		return fmt.Errorf("descriptor has %d bytes, limit %d", len(b), limit)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("descriptor: %w", err)
	}
	if c, err := json.Marshal(v); err != nil || !bytes.Equal(c, b) {
		return errors.New("descriptor is not in canonical format-1 encoding")
	}
	return validate()
}

func encode(v any, limit int, validate func() error) ([]byte, error) {
	if err := validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(v)
	if err == nil && len(b) > limit {
		err = fmt.Errorf("descriptor has %d bytes, limit %d", len(b), limit)
	}
	return b, err
}

// ValidKey reports whether k is an acceptable entity key.
func ValidKey(k string) error { return validName("key", k, MaxKeyBytes) }

// ValidNode reports whether n is an acceptable NodeID.
func ValidNode(n string) error { return validName("node ID", n, MaxNodeBytes) }

func validName(what, s string, max int) error {
	if s == "" || len(s) > max || !utf8.ValidString(s) {
		return fmt.Errorf("%s must be 1..%d bytes of valid UTF-8", what, max)
	}
	return nil
}

func (n Namespace) validate() error {
	if n.Format != Format || n.EntityFormat != Format {
		return fmt.Errorf("unsupported namespace format %d/%d", n.Format, n.EntityFormat)
	}
	if n.Namespace == (ID{}) {
		return errors.New("zero namespace ID")
	}
	return nil
}

func (b Birth) validate() error {
	switch {
	case b.Format != Format:
		return fmt.Errorf("unsupported birth format %d", b.Format)
	case b.Namespace == (ID{}) || b.Incarnation == (ID{}) || b.Attempt == (ID{}):
		return errors.New("zero namespace, incarnation or attempt")
	case b.OperationID == (OpID{}) || b.InputHash == (Digest{}):
		return errors.New("zero operation ID or input hash")
	case b.Schema == 0:
		return errors.New("schema must be positive")
	}
	return ValidKey(b.Key)
}

// Validate checks every single-head invariant of PLAN §5.2.
func (h *Head) Validate() error {
	switch {
	case h.Format != Format:
		return fmt.Errorf("unsupported head format %d", h.Format)
	case h.Codec != Codec || h.PageSize != PageSize:
		return fmt.Errorf("unsupported codec %q or page size %d", h.Codec, h.PageSize)
	case h.Namespace == (ID{}) || h.Incarnation == (ID{}) || h.Nonce == (ID{}):
		return errors.New("zero namespace, incarnation or nonce")
	case h.Revision == 0 || h.Revision > MaxInt || h.Epoch == 0 || h.Epoch > h.Revision:
		return fmt.Errorf("bad revision %d or epoch %d", h.Revision, h.Epoch)
	case h.Sequence == 0 || h.Sequence > MaxInt || h.Schema == 0:
		return fmt.Errorf("bad sequence %d or schema %d", h.Sequence, h.Schema)
	case h.PageCount == 0 || h.PageCount > MaxPages:
		return fmt.Errorf("page count %d outside 1..%d", h.PageCount, MaxPages)
	case h.DatabaseChecksum&ChecksumFlag == 0:
		return errors.New("database checksum lacks the LTX checksum flag")
	case len(h.Deltas) > MaxDeltas:
		return fmt.Errorf("%d deltas exceed %d", len(h.Deltas), MaxDeltas)
	}
	if err := ValidKey(h.Key); err != nil {
		return err
	}
	if h.Owner != nil {
		if err := ValidNode(h.Owner.Node); err != nil {
			return err
		}
		if h.Owner.Session == (ID{}) {
			return errors.New("zero owner session")
		}
	}
	prefix := ObjectPrefix(h.Key, h.Incarnation)
	refs := append([]Ref{h.Snapshot}, h.Deltas...)
	seen := make(map[string]bool, len(refs))
	uploaded := false // some ref at base >= 1: a used head always leaves an object (D16, D5)
	for i, r := range refs {
		if err := r.validate(prefix, h.Revision); err != nil {
			return fmt.Errorf("ref %d: %w", i, err)
		}
		if seen[r.Key] {
			return fmt.Errorf("ref %d: duplicate object %s", i, r.Key)
		}
		seen[r.Key] = true
		uploaded = uploaded || r.BaseRevision >= 1
	}
	if t := h.Tail; t != nil {
		switch err := t.Ref().span(InlineBytes); {
		case err != nil:
			return fmt.Errorf("tail: %w", err)
		case uint64(len(t.Data)) != t.Bytes || sha256.Sum256(t.Data) != t.SHA256:
			return errors.New("tail: data disagrees with bytes or sha256")
		case !uploaded:
			return errors.New("tail without a ref at base revision >= 1")
		}
		refs = append(refs, t.Ref())
	}
	next, chk := uint64(1), Checksum(0) // the snapshot starts at 1 from an empty database
	for i, r := range refs {
		if r.MinSequence != next || r.PreChecksum != chk {
			return fmt.Errorf("ref %d: breaks the chain at sequence %d", i, next)
		}
		next, chk = r.MaxSequence+1, r.PostChecksum
	}
	if next-1 != h.Sequence || chk != h.DatabaseChecksum {
		return errors.New("chain does not end at the head's sequence and checksum")
	}
	return nil
}

func (r Ref) validate(prefix string, revision uint64) error {
	base, _, sum, err := ParseObjectKey(prefix, r.Key)
	switch {
	case err != nil:
		return err
	case base != r.BaseRevision || sum != r.SHA256:
		return errors.New("key disagrees with base_revision or sha256")
	case r.BaseRevision >= revision:
		return fmt.Errorf("base revision %d not below head revision %d", r.BaseRevision, revision)
	case r.Level > MaxLevel:
		return fmt.Errorf("level %d exceeds %d", r.Level, MaxLevel)
	}
	return r.span(MaxObjectBytes)
}

// span checks the size, sequence range and checksum flags of one LTX.
func (r Ref) span(maxBytes uint64) error {
	switch {
	case r.Bytes == 0 || r.Bytes > maxBytes:
		return fmt.Errorf("object size %d outside 1..%d", r.Bytes, maxBytes)
	case r.MinSequence == 0 || r.MinSequence > r.MaxSequence || r.MaxSequence > MaxInt:
		return fmt.Errorf("bad sequence range %d..%d", r.MinSequence, r.MaxSequence)
	case r.PostChecksum&ChecksumFlag == 0 || (r.PreChecksum != 0 && r.PreChecksum&ChecksumFlag == 0):
		return errors.New("checksum lacks the LTX checksum flag")
	}
	return nil
}

// CheckGenesis checks the extra invariants of a head created by Create or
// Import: revision 1, epoch 1, an owner, no deltas or tail, snapshot at base 0.
func CheckGenesis(h *Head) error {
	if h.Revision != 1 || h.Epoch != 1 || h.Owner == nil || len(h.Deltas) != 0 || h.Tail != nil ||
		h.Snapshot.BaseRevision != 0 {
		return errors.New("not a genesis head")
	}
	return nil
}

// Successor checks that next is a legal replacement of prev (both already
// valid): same identity, revision+1, fresh nonce, epoch+1 exactly when the
// owner changes, data changes only by the unchanged owner, the tail carried
// byte-identical unless the data changes or a snapshot (no deltas) empties it,
// and the no-resurrection rule: every ref is carried over unchanged from prev
// or was produced under base revision prev.Revision.
func Successor(prev, next *Head) error {
	sameOwner := (prev.Owner == nil && next.Owner == nil) ||
		(prev.Owner != nil && next.Owner != nil && *prev.Owner == *next.Owner)
	sameTail := (prev.Tail == nil && next.Tail == nil) || (prev.Tail != nil && next.Tail != nil &&
		prev.Tail.Ref() == next.Tail.Ref() && bytes.Equal(prev.Tail.Data, next.Tail.Data))
	switch {
	case next.Namespace != prev.Namespace || next.Key != prev.Key || next.Incarnation != prev.Incarnation:
		return errors.New("identity changed")
	case next.Revision != prev.Revision+1 || next.Nonce == prev.Nonce:
		return errors.New("revision must advance by one with a fresh nonce")
	case !(sameOwner && next.Epoch == prev.Epoch) && !(!sameOwner && next.Epoch == prev.Epoch+1):
		return errors.New("epoch must advance by one exactly when the owner changes")
	case next.Sequence < prev.Sequence || next.Schema < prev.Schema:
		return errors.New("sequence or schema went backwards")
	case next.Sequence == prev.Sequence && (next.Schema != prev.Schema ||
		next.DatabaseChecksum != prev.DatabaseChecksum || next.PageCount != prev.PageCount):
		return errors.New("data changed without a new sequence")
	case next.Sequence == prev.Sequence && !sameTail && !(next.Tail == nil && len(next.Deltas) == 0):
		return errors.New("tail changed without a new sequence or a snapshot")
	case next.Sequence > prev.Sequence && (!sameOwner || next.Owner == nil):
		return errors.New("data changed by a non-owner")
	}
	old := make(map[string]Ref, 1+len(prev.Deltas))
	for _, r := range append([]Ref{prev.Snapshot}, prev.Deltas...) {
		old[r.Key] = r
	}
	for _, r := range append([]Ref{next.Snapshot}, next.Deltas...) {
		if o, ok := old[r.Key]; ok && o == r {
			continue
		}
		if r.BaseRevision != prev.Revision {
			return fmt.Errorf("ref %s is neither carried over nor new at base %d", r.Key, prev.Revision)
		}
	}
	return nil
}

// Object keys, relative to the namespace prefix.
const NamespaceKey = "namespace.json"

// EntityDir is "entities/<first-2-hex>/<sha256-exact-key>".
func EntityDir(key string) string {
	s := sha256.Sum256([]byte(key))
	x := hex.EncodeToString(s[:])
	return "entities/" + x[:2] + "/" + x
}

func BirthKey(key string) string { return EntityDir(key) + "/birth.json" }
func HeadKey(key string) string  { return EntityDir(key) + "/head.json" }

// ObjectPrefix is the directory holding every LTX object of one incarnation.
func ObjectPrefix(key string, inc ID) string {
	return EntityDir(key) + "/objects/" + inc.String() + "/"
}

// ObjectKey is "<ObjectPrefix><base-revision-16-hex>/<attempt-32-hex>/<sha256-64-hex>.ltx".
func ObjectKey(key string, inc ID, base uint64, attempt ID, sum Digest) string {
	return fmt.Sprintf("%s%016x/%s/%s.ltx", ObjectPrefix(key, inc), base, attempt, sum)
}

// ParseObjectKey checks that k is an object key directly under prefix and
// returns its components. Anything else (other incarnations, path escapes,
// upper-case hex, extra segments) is rejected.
func ParseObjectKey(prefix, k string) (base uint64, attempt ID, sum Digest, err error) {
	rest, ok := strings.CutPrefix(k, prefix)
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 3 || !strings.HasSuffix(parts[2], ".ltx") {
		return 0, ID{}, Digest{}, fmt.Errorf("object key %q is not under %q", k, prefix)
	}
	var b [8]byte
	if err = unhex(b[:], []byte(parts[0])); err == nil {
		base = binary.BigEndian.Uint64(b[:])
		if err = unhex(attempt[:], []byte(parts[1])); err == nil {
			err = unhex(sum[:], []byte(strings.TrimSuffix(parts[2], ".ltx")))
		}
	}
	if err == nil && (base > MaxInt || attempt == (ID{})) {
		err = errors.New("base revision out of range or zero attempt")
	}
	if err != nil {
		return 0, ID{}, Digest{}, fmt.Errorf("object key %q: %w", k, err)
	}
	return base, attempt, sum, nil
}
