package head

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func id(b byte) ID { var v ID; v[0], v[15] = b, b; return v }

func ref(key string, inc ID, base, lo, hi uint64, pre, post Checksum, level uint32) Ref {
	sum := sha256.Sum256(fmt.Appendf(nil, "%d-%d-%d", base, lo, hi))
	return Ref{
		Key: ObjectKey(key, inc, base, id(byte(lo)), sum), SHA256: sum, Bytes: 4096 + lo,
		BaseRevision: base, MinSequence: lo, MaxSequence: hi, PreChecksum: pre, PostChecksum: post, Level: level,
	}
}

// fixture: revision 5, snapshot 1..10 (base 0), deltas 11..12 (base 3) and 13 (base 4).
func fixture() Head {
	inc := id(2)
	c1, c2, c3 := ChecksumFlag|0x11, ChecksumFlag|0x22, ChecksumFlag|0x33
	return Head{
		Format: 1, Namespace: id(1), Key: "tenant/é<&>\u2028\x01", Incarnation: inc,
		Revision: 5, Nonce: id(3), Owner: &Owner{Node: "node-b", Session: id(4)},
		Epoch: 2, Sequence: 13, Schema: 2, Codec: "ltx-v3", PageSize: 4096, PageCount: 128,
		DatabaseChecksum: c3,
		Snapshot:         ref("tenant/é<&>\u2028\x01", inc, 0, 1, 10, 0, c1, 0),
		Deltas: []Ref{
			ref("tenant/é<&>\u2028\x01", inc, 3, 11, 12, c1, c2, 0),
			ref("tenant/é<&>\u2028\x01", inc, 4, 13, 13, c2, c3, 0),
		},
	}
}

func tail(lo, hi uint64, pre, post Checksum, n int) *Tail {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*7 + int(lo))
	}
	return &Tail{SHA256: sha256.Sum256(data), Bytes: uint64(n), MinSequence: lo, MaxSequence: hi,
		PreChecksum: pre, PostChecksum: post, Data: data}
}

// tailFixture: fixture plus an inline tail 14..15 of 100 bytes (revision 5).
func tailFixture() Head {
	h := fixture()
	c5 := ChecksumFlag | 0x55
	h.Tail = tail(14, 15, h.DatabaseChecksum, c5, 100)
	h.Sequence, h.DatabaseChecksum = 15, c5
	return h
}

func mustEncode(t *testing.T, h Head) []byte {
	t.Helper()
	b, err := h.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	h := fixture()
	b := mustEncode(t, h)
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v\n%s", err, b)
	}
	if b2 := mustEncode(t, got); !bytes.Equal(b, b2) {
		t.Fatalf("re-encode differs:\n%s\n%s", b, b2)
	}
	if !bytes.HasSuffix(b, []byte(`],"tail":null}`)) {
		t.Fatalf("absent tail not encoded as null: %s", b)
	}
	tb := mustEncode(t, tailFixture())
	if got, err := Decode(tb); err != nil || !bytes.Equal(mustEncode(t, got), tb) {
		t.Fatalf("tail head: %v\n%s", err, tb)
	}
	h.Owner = nil
	if _, err := Decode(mustEncode(t, h)); err != nil {
		t.Fatalf("released head: %v", err)
	}
	h.Deltas = nil // encodes as []
	h.Sequence, h.DatabaseChecksum = 10, h.Snapshot.PostChecksum
	b = mustEncode(t, h)
	if !bytes.Contains(b, []byte(`"deltas":[]`)) {
		t.Fatalf("nil deltas not encoded as []: %s", b)
	}
}

// TestGolden pins the canonical encoding. If a toolchain changes
// encoding/json output, existing heads would stop decoding: fail here first.
func TestGolden(t *testing.T) {
	h := fixture()
	h.Deltas = h.Deltas[:0]
	h.Sequence, h.DatabaseChecksum = 10, h.Snapshot.PostChecksum
	h.Key = "k\"\\é<\x01"
	h.Snapshot = ref(h.Key, h.Incarnation, 0, 1, 10, 0, h.DatabaseChecksum, 0)
	want := `{"format":1,"namespace":"01000000000000000000000000000001","key":"k\"\\é\u003c\u0001",` +
		`"incarnation":"02000000000000000000000000000002","revision":5,"nonce":"03000000000000000000000000000003",` +
		`"owner":{"node":"node-b","session":"04000000000000000000000000000004"},"epoch":2,"sequence":10,"schema":2,` +
		`"codec":"ltx-v3","page_size":4096,"page_count":128,"database_checksum":"8000000000000011",` +
		`"snapshot":{"key":"` + h.Snapshot.Key + `","sha256":"` + h.Snapshot.SHA256.String() + `","bytes":4097,` +
		`"base_revision":0,"min_sequence":1,"max_sequence":10,"pre_checksum":"0000000000000000",` +
		`"post_checksum":"8000000000000011","level":0},"deltas":[],"tail":null}`
	if got := string(mustEncode(t, h)); got != want {
		t.Fatalf("canonical encoding changed:\n got %s\nwant %s", got, want)
	}
	tf := tailFixture()
	tf.Tail = tail(14, 15, ChecksumFlag|0x33, ChecksumFlag|0x55, 4)
	want = `,"tail":{"sha256":"` + tf.Tail.SHA256.String() + `","bytes":4,"min_sequence":14,"max_sequence":15,` +
		`"pre_checksum":"8000000000000033","post_checksum":"8000000000000055","data":"DhUcIw=="}}`
	if got := string(mustEncode(t, tf)); !strings.HasSuffix(got, want) {
		t.Fatalf("canonical tail encoding changed:\n got %s\nwant suffix %s", got, want)
	}
}

func TestDecodeRejectsNonCanonical(t *testing.T) {
	good := string(mustEncode(t, fixture()))
	edits := map[string]func(string) string{
		"duplicate key": func(s string) string { return strings.Replace(s, `"epoch":2,`, `"epoch":2,"epoch":2,`, 1) },
		"duplicate key nested": func(s string) string {
			return strings.Replace(s, `"node":"node-b",`, `"node":"node-b","node":"node-b",`, 1)
		},
		"unknown field":     func(s string) string { return strings.Replace(s, `"epoch":2,`, `"epoch":2,"extra":1,`, 1) },
		"case-folded field": func(s string) string { return strings.Replace(s, `"epoch":`, `"Epoch":`, 1) },
		"missing field":     func(s string) string { return strings.Replace(s, `"epoch":2,`, ``, 1) },
		"reordered fields": func(s string) string {
			return strings.Replace(strings.Replace(s, `"epoch":2,`, ``, 1), `"revision":`, `"epoch":2,"revision":`, 1)
		},
		"null integer": func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":null`, 1) },
		"null hex": func(s string) string {
			return strings.Replace(s, `"nonce":"03000000000000000000000000000003"`, `"nonce":null`, 1)
		},
		"null deltas":      func(s string) string { return s[:strings.Index(s, `"deltas":`)] + `"deltas":null}` },
		"float":            func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":2.0`, 1) },
		"exponent":         func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":2e0`, 1) },
		"negative":         func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":-2`, 1) },
		"string integer":   func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":"2"`, 1) },
		"uint64 overflow":  func(s string) string { return strings.Replace(s, `"revision":5`, `"revision":18446744073709551616`, 1) },
		"leading zero":     func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch":02`, 1) },
		"whitespace":       func(s string) string { return strings.Replace(s, `"epoch":2`, `"epoch": 2`, 1) },
		"trailing newline": func(s string) string { return s + "\n" },
		"trailing object":  func(s string) string { return s + "{}" },
		"upper-case hex": func(s string) string {
			i := strings.Index(s, `"sha256":"`) + 10
			return s[:i] + strings.ToUpper(s[i:i+64]) + s[i+64:]
		},
		"short hex": func(s string) string {
			return strings.Replace(s, `"nonce":"03000000000000000000000000000003"`, `"nonce":"0300000000000000000000000000003"`, 1)
		},
		"escaped ascii":     func(s string) string { return strings.Replace(s, `"codec":"ltx-v3"`, `"codec":"ltx\u002dv3"`, 1) },
		"invalid utf-8 key": func(s string) string { return strings.Replace(s, "é", "\xc3", 1) },
		"nested deeply": func(s string) string {
			return strings.Replace(s, `"epoch":2,`, `"x":`+strings.Repeat("[", 5000)+strings.Repeat("]", 5000)+`,"epoch":2,`, 1)
		},
		"array not object": func(s string) string { return "[" + s + "]" },
		"empty":            func(string) string { return "" },
		"oversized":        func(s string) string { return s + strings.Repeat(" ", 64<<10) },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			b := edit(good)
			if b == good {
				t.Fatal("edit did not change the input")
			}
			if _, err := Decode([]byte(b)); err == nil {
				t.Fatalf("accepted:\n%s", b)
			}
		})
	}
}

func TestDecodeRejectsNonCanonicalTail(t *testing.T) {
	good := string(mustEncode(t, tailFixture()))
	plain := string(mustEncode(t, fixture()))
	data := func(s string) (int, int) { // bounds of the base64 text
		i := strings.Index(s, `"data":"`) + 8
		return i, i + strings.IndexByte(s[i:], '"')
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	for name, b := range map[string]string{
		"missing tail":   strings.Replace(plain, `,"tail":null`, ``, 1),
		"empty tail":     strings.Replace(plain, `"tail":null`, `"tail":{}`, 1),
		"tail unknown":   strings.Replace(good, `"data":`, `"level":0,"data":`, 1),
		"tail duplicate": strings.Replace(good, `"bytes":100,`, `"bytes":100,"bytes":100,`, 1),
		"data null":      func() string { i, j := data(good); return good[:i-1] + "null" + good[j+1:] }(),
		"data unpadded":  strings.Replace(good, `=="}}`, `"}}`, 1),
		"data newline":   func() string { i, _ := data(good); return good[:i+4] + `\n` + good[i+4:] }(),
		"data url-safe": func() string {
			i, j := data(good)
			return good[:i] + strings.NewReplacer("+", "-", "/", "_").Replace(good[i:j]) + good[j:]
		}(),
		"data padding bits": func() string { // same bytes under non-strict base64
			_, j := data(good)
			k := j - 3 // last significant character before "=="
			c := alphabet[strings.IndexByte(alphabet, good[k])^1]
			return good[:k] + string(c) + good[k+1:]
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if b == good || b == plain {
				t.Fatal("edit did not change the input")
			}
			if _, err := Decode([]byte(b)); err == nil {
				t.Fatalf("accepted:\n%s", b)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]func(h *Head){
		"format 2":             func(h *Head) { h.Format = 2 },
		"codec":                func(h *Head) { h.Codec = "ltx-v2" },
		"page size":            func(h *Head) { h.PageSize = 8192 },
		"zero namespace":       func(h *Head) { h.Namespace = ID{} },
		"zero incarnation":     func(h *Head) { h.Incarnation = ID{} },
		"zero nonce":           func(h *Head) { h.Nonce = ID{} },
		"zero revision":        func(h *Head) { h.Revision = 0 },
		"revision over int63":  func(h *Head) { h.Revision = MaxInt + 1 },
		"zero epoch":           func(h *Head) { h.Epoch = 0 },
		"epoch above revision": func(h *Head) { h.Epoch = 6 },
		"zero sequence":        func(h *Head) { h.Sequence = 0 },
		"zero schema":          func(h *Head) { h.Schema = 0 },
		"zero pages":           func(h *Head) { h.PageCount = 0 },
		"too many pages":       func(h *Head) { h.PageCount = MaxPages + 1 },
		"checksum flag":        func(h *Head) { h.DatabaseChecksum = 0x33 },
		"empty key":            func(h *Head) { h.Key = "" },
		"long key":             func(h *Head) { h.Key = strings.Repeat("k", MaxKeyBytes+1) },
		"invalid key":          func(h *Head) { h.Key = "\xff" },
		"empty owner node":     func(h *Head) { h.Owner.Node = "" },
		"long owner node":      func(h *Head) { h.Owner.Node = strings.Repeat("n", MaxNodeBytes+1) },
		"zero session":         func(h *Head) { h.Owner.Session = ID{} },
		"33 deltas": func(h *Head) {
			for len(h.Deltas) <= MaxDeltas {
				h.Deltas = append(h.Deltas, h.Deltas[1])
			}
		},
		"sequence gap": func(h *Head) {
			h.Deltas[1] = ref(h.Key, h.Incarnation, 4, 14, 14, h.Deltas[0].PostChecksum, h.DatabaseChecksum, 0)
		},
		"sequence overlap": func(h *Head) {
			h.Deltas[1] = ref(h.Key, h.Incarnation, 4, 12, 13, h.Deltas[0].PostChecksum, h.DatabaseChecksum, 0)
		},
		"checksum chain":    func(h *Head) { h.Deltas[1].PreChecksum = ChecksumFlag | 0x99 },
		"delta zero pre":    func(h *Head) { h.Deltas[0].PreChecksum = 0 },
		"end sequence":      func(h *Head) { h.Sequence = 14 },
		"end checksum":      func(h *Head) { h.DatabaseChecksum = ChecksumFlag | 0x44 },
		"snapshot not at 1": func(h *Head) { h.Snapshot.MinSequence = 2 },
		"snapshot pre":      func(h *Head) { h.Snapshot.PreChecksum = ChecksumFlag | 1 },
		"reversed range":    func(h *Head) { h.Deltas[0].MinSequence, h.Deltas[0].MaxSequence = 12, 11 },
		"zero bytes":        func(h *Head) { h.Deltas[0].Bytes = 0 },
		"huge object":       func(h *Head) { h.Deltas[0].Bytes = MaxObjectBytes + 1 },
		"level":             func(h *Head) { h.Deltas[0].Level = MaxLevel + 1 },
		"base not below rev": func(h *Head) {
			h.Deltas[1] = ref(h.Key, h.Incarnation, 5, 13, 13, h.Deltas[0].PostChecksum, h.DatabaseChecksum, 0)
		},
		"base disagrees": func(h *Head) { h.Deltas[0].BaseRevision = 2 },
		"sha disagrees":  func(h *Head) { h.Deltas[0].SHA256[0] ^= 1 },
		"other incarnation": func(h *Head) {
			h.Deltas[0].Key = strings.Replace(h.Deltas[0].Key, h.Incarnation.String(), id(9).String(), 1)
		},
		"other entity": func(h *Head) {
			h.Deltas[0].Key = strings.Replace(h.Deltas[0].Key, EntityDir(h.Key), EntityDir("other"), 1)
		},
		"path escape": func(h *Head) {
			h.Deltas[0].Key = ObjectPrefix(h.Key, h.Incarnation) + "../x/" + h.Deltas[0].Key[len(ObjectPrefix(h.Key, h.Incarnation)):]
		},
		"absolute key":     func(h *Head) { h.Deltas[0].Key = "/" + h.Deltas[0].Key },
		"duplicate object": func(h *Head) { h.Deltas[1].Key = h.Deltas[0].Key },
	}
	tailCases := map[string]func(h *Head){
		"tail on genesis-only head": func(h *Head) {
			h.Deltas = nil
			h.Snapshot = ref(h.Key, h.Incarnation, 0, 1, 13, 0, h.Tail.PreChecksum, 0)
		},
		"tail on base-0 refs only": func(h *Head) {
			h.Deltas = h.Deltas[1:]
			h.Snapshot = ref(h.Key, h.Incarnation, 0, 1, 12, 0, h.Deltas[0].PreChecksum, 0)
			h.Deltas[0] = ref(h.Key, h.Incarnation, 0, 13, 13, h.Deltas[0].PreChecksum, h.Deltas[0].PostChecksum, 0)
		},
		"tail gap":            func(h *Head) { h.Tail = tail(15, 15, h.Tail.PreChecksum, h.DatabaseChecksum, 100) },
		"tail overlap":        func(h *Head) { h.Tail = tail(13, 15, h.Tail.PreChecksum, h.DatabaseChecksum, 100) },
		"tail pre checksum":   func(h *Head) { h.Tail = tail(14, 15, ChecksumFlag|0x99, h.DatabaseChecksum, 100) },
		"tail zero pre":       func(h *Head) { h.Tail = tail(14, 15, 0, h.DatabaseChecksum, 100) },
		"tail short of head":  func(h *Head) { h.Sequence = 16 },
		"tail past head":      func(h *Head) { h.Tail = tail(14, 16, h.Tail.PreChecksum, h.DatabaseChecksum, 100) },
		"tail post checksum":  func(h *Head) { h.DatabaseChecksum = ChecksumFlag | 0x66 },
		"tail post flag":      func(h *Head) { h.Tail.PostChecksum, h.DatabaseChecksum = 0x55, 0x55 },
		"tail reversed range": func(h *Head) { h.Tail.MinSequence, h.Tail.MaxSequence = 15, 14 },
		"tail bytes":          func(h *Head) { h.Tail.Bytes++ },
		"tail sha":            func(h *Head) { h.Tail.SHA256[0] ^= 1 },
		"tail data":           func(h *Head) { h.Tail.Data[0] ^= 1 },
		"tail empty":          func(h *Head) { h.Tail = tail(14, 15, h.Tail.PreChecksum, h.DatabaseChecksum, 0) },
		"tail nil data":       func(h *Head) { h.Tail.Data, h.Tail.Bytes = nil, 0 },
		"tail over inline": func(h *Head) {
			h.Tail = tail(14, 15, h.Tail.PreChecksum, h.DatabaseChecksum, InlineBytes+1)
		},
		"tail duplicates delta": func(h *Head) {
			h.Tail = tail(13, 13, h.Deltas[0].PostChecksum, h.Deltas[1].PostChecksum, 100)
			h.Sequence, h.DatabaseChecksum = 13, h.Deltas[1].PostChecksum
		},
	}
	h := tailFixture()
	if err := h.Validate(); err != nil {
		t.Fatalf("tail fixture: %v", err)
	}
	h.Tail = tail(14, 15, h.Tail.PreChecksum, h.DatabaseChecksum, InlineBytes)
	if _, err := h.Encode(); err != nil {
		t.Fatalf("tail of InlineBytes rejected: %v", err)
	}
	for name, mutate := range tailCases {
		cases[name] = mutate
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h := fixture()
			if strings.HasPrefix(name, "tail") {
				h = tailFixture()
			}
			mutate(&h)
			if err := h.Validate(); err == nil {
				t.Fatal("accepted")
			}
			if _, err := h.Encode(); err == nil {
				t.Fatal("encoded")
			}
		})
	}
}

func TestParseObjectKey(t *testing.T) {
	inc, sum := id(7), Digest{1, 2, 3}
	p := ObjectPrefix("k", inc)
	k := ObjectKey("k", inc, 0x1f, id(8), sum)
	if !strings.HasPrefix(k, "entities/") || !strings.HasSuffix(k, ".ltx") {
		t.Fatalf("bad key shape %q", k)
	}
	base, att, s, err := ParseObjectKey(p, k)
	if err != nil || base != 0x1f || att != id(8) || s != sum {
		t.Fatalf("parse %q: %v %x %v %v", k, base, att, s, err)
	}
	bad := []string{
		k + "x", strings.ToUpper(k), p + "000000000000001f/" + id(8).String() + "/x.ltx",
		p + "1f/" + id(8).String() + "/" + sum.String() + ".ltx",
		p + "000000000000001f/" + ID{}.String() + "/" + sum.String() + ".ltx",
		p + "800000000000001f/" + id(8).String() + "/" + sum.String() + ".ltx",
		p + "000000000000001f/" + id(8).String() + "/" + sum.String() + ".ltx/x",
		p + "000000000000001f/" + id(8).String() + "/" + sum.String(),
		p + "x/000000000000001f/" + id(8).String() + "/" + sum.String() + ".ltx",
		ObjectPrefix("k", id(9)) + k[len(p):],
	}
	for _, b := range bad {
		if _, _, _, err := ParseObjectKey(p, b); err == nil {
			t.Errorf("accepted %q", b)
		}
	}
	if EntityDir("k")[9:11] != EntityDir("k")[12:14] || len(EntityDir("k")) != len("entities/xx/")+64 {
		t.Fatalf("entity dir shape %q", EntityDir("k"))
	}
}

func TestNamespaceAndBirth(t *testing.T) {
	ns := Namespace{Format: 1, Namespace: id(1), EntityFormat: 1}
	b, err := ns.Encode()
	if err != nil || string(b) != `{"format":1,"namespace":"01000000000000000000000000000001","entity_format":1}` {
		t.Fatalf("namespace: %s %v", b, err)
	}
	if got, err := DecodeNamespace(b); err != nil || got != ns {
		t.Fatalf("namespace decode: %v %v", got, err)
	}
	for _, bad := range []string{
		strings.Replace(string(b), `"entity_format":1`, `"entity_format":2`, 1),
		strings.Replace(string(b), `"format":1`, `"format":2`, 1),
		strings.Replace(string(b), `01000000000000000000000000000001`, `00000000000000000000000000000000`, 1),
		strings.Replace(string(b), `}`, `,"endpoint":"x"}`, 1),
		string(b) + strings.Repeat(" ", 1024),
	} {
		if _, err := DecodeNamespace([]byte(bad)); err == nil {
			t.Errorf("namespace accepted %s", bad)
		}
	}

	birth := Birth{Format: 1, Namespace: id(1), Key: "k", Incarnation: id(2), OperationID: OpID{1},
		InputHash: Digest{2}, Schema: 1, Attempt: id(3)}
	b, err = birth.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeBirth(b); err != nil || got != birth {
		t.Fatalf("birth decode: %v %v", got, err)
	}
	for name, mutate := range map[string]func(*Birth){
		"zero op": func(b *Birth) { b.OperationID = OpID{} }, "zero hash": func(b *Birth) { b.InputHash = Digest{} },
		"zero schema": func(b *Birth) { b.Schema = 0 }, "zero attempt": func(b *Birth) { b.Attempt = ID{} },
		"bad key": func(b *Birth) { b.Key = "" }, "format": func(b *Birth) { b.Format = 0 },
	} {
		v := birth
		mutate(&v)
		if _, err := v.Encode(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	birth.Key = strings.Repeat("\x01", MaxKeyBytes) // worst case: 6-byte escapes
	if b, err := birth.Encode(); err != nil {
		t.Errorf("maximal birth: %v", err)
	} else if _, err := DecodeBirth(b); err != nil {
		t.Errorf("maximal birth decode: %v", err)
	}
}

func TestMaxHeadFits(t *testing.T) {
	// The largest valid head (512-byte key of control characters, 32 deltas
	// at maximal integers, a tail of InlineBytes) must fit the 64 KiB limit.
	h := fixture()
	h.Key = strings.Repeat("\x01", MaxKeyBytes)
	h.Owner.Node = strings.Repeat("\x01", MaxNodeBytes)
	h.Revision, h.Epoch = MaxInt, MaxInt
	c := Checksum(MaxInt) | ChecksumFlag
	h.Snapshot = ref(h.Key, h.Incarnation, MaxInt-1, 1, MaxInt-MaxDeltas, 0, c, MaxLevel)
	h.Snapshot.Bytes = MaxObjectBytes
	h.Deltas = nil
	for i := uint64(0); i < MaxDeltas; i++ {
		r := ref(h.Key, h.Incarnation, MaxInt-1, MaxInt-MaxDeltas+i, MaxInt-MaxDeltas+i, c, c, MaxLevel)
		r.Bytes = MaxObjectBytes
		h.Deltas = append(h.Deltas, r)
	}
	h.Snapshot.MaxSequence = MaxInt - MaxDeltas - 1
	h.Tail = tail(MaxInt, MaxInt, c, c, InlineBytes)
	h.Sequence, h.DatabaseChecksum = MaxInt, c
	b := mustEncode(t, h)
	if _, err := Decode(b); err != nil {
		t.Fatalf("decode max head (%d bytes): %v", len(b), err)
	}
	t.Logf("max head is %d bytes", len(b))
}

func TestSuccessor(t *testing.T) {
	prev := fixture()
	c4 := ChecksumFlag | 0x44
	mutation := func() Head {
		n := fixture()
		n.Revision, n.Nonce, n.Sequence, n.DatabaseChecksum = 6, id(5), 14, c4
		n.Deltas = append(n.Deltas, ref(n.Key, n.Incarnation, 5, 14, 14, prev.DatabaseChecksum, c4, 0))
		return n
	}
	takeover := func() Head {
		n := fixture()
		n.Revision, n.Nonce, n.Epoch, n.Owner = 6, id(5), 3, &Owner{Node: "node-c", Session: id(6)}
		return n
	}
	compaction := func() Head {
		n := fixture()
		n.Revision, n.Nonce = 6, id(5)
		n.Deltas = []Ref{ref(n.Key, n.Incarnation, 5, 11, 13, n.Snapshot.PostChecksum, n.DatabaseChecksum, 1)}
		return n
	}
	snapshot := func() Head {
		n := mutation()
		n.Snapshot = ref(n.Key, n.Incarnation, 5, 1, 14, 0, c4, 0)
		n.Deltas = nil
		return n
	}
	ok := map[string]func() Head{
		"mutation": mutation, "takeover": takeover, "compaction": compaction, "snapshot on cap": snapshot,
		"release":               func() Head { n := takeover(); n.Owner = nil; return n },
		"fence":                 func() Head { n := fixture(); n.Revision, n.Nonce = 6, id(5); return n },
		"migration":             func() Head { n := mutation(); n.Schema = 3; return n },
		"same node new session": func() Head { n := takeover(); n.Owner.Node = "node-b"; return n },
	}
	for name, mk := range ok {
		n := mk()
		if err := n.Validate(); err != nil {
			t.Fatalf("%s: invalid fixture: %v", name, err)
		}
		if err := Successor(&prev, &n); err != nil {
			t.Errorf("%s: rejected: %v", name, err)
		}
	}

	// D16: prev carries tail 14..15; every same-sequence successor keeps it
	// byte-identical, except a snapshot, which empties it.
	tprev := tailFixture()
	c6 := ChecksumFlag | 0x66
	carry := func(f func(n *Head)) func() Head {
		return func() Head { n := tailFixture(); n.Revision, n.Nonce = 6, id(5); f(&n); return n }
	}
	inline := carry(func(n *Head) { // compact(tail, delta 16) stays inline
		n.Tail = tail(14, 16, n.Tail.PreChecksum, c6, 200)
		n.Sequence, n.DatabaseChecksum = 16, c6
	})
	tailOK := map[string]func() Head{
		"inline mutation": inline,
		"flush to delta": carry(func(n *Head) {
			n.Deltas = append(n.Deltas, ref(n.Key, n.Incarnation, 5, 14, 16, n.Tail.PreChecksum, c6, 0))
			n.Tail, n.Sequence, n.DatabaseChecksum = nil, 16, c6
		}),
		"takeover": carry(func(n *Head) { n.Epoch, n.Owner = 3, &Owner{Node: "node-c", Session: id(6)} }),
		"release":  carry(func(n *Head) { n.Epoch, n.Owner = 3, nil }),
		"fence":    carry(func(n *Head) {}),
		"compaction": carry(func(n *Head) {
			n.Deltas = []Ref{ref(n.Key, n.Incarnation, 5, 11, 13, n.Snapshot.PostChecksum, n.Tail.PreChecksum, 1)}
		}),
		"snapshot": carry(func(n *Head) {
			n.Snapshot = ref(n.Key, n.Incarnation, 5, 1, 15, 0, n.DatabaseChecksum, 0)
			n.Deltas, n.Tail = nil, nil
		}),
	}
	for name, mk := range tailOK {
		n := mk()
		if err := n.Validate(); err != nil {
			t.Fatalf("tail %s: invalid fixture: %v", name, err)
		}
		if err := Successor(&tprev, &n); err != nil {
			t.Errorf("tail %s: rejected: %v", name, err)
		}
	}
	tailBad := map[string]func() Head{
		"takeover rewrites tail bytes": carry(func(n *Head) {
			n.Epoch, n.Owner = 3, &Owner{Node: "node-c", Session: id(6)}
			n.Tail = tail(14, 15, n.Tail.PreChecksum, n.Tail.PostChecksum, 101)
		}),
		"fence moves tail into a delta": carry(func(n *Head) {
			n.Deltas = append(n.Deltas, ref(n.Key, n.Incarnation, 5, 14, 15, n.Tail.PreChecksum, n.Tail.PostChecksum, 0))
			n.Tail = nil
		}),
		"compaction swallows tail": carry(func(n *Head) {
			n.Deltas = []Ref{ref(n.Key, n.Incarnation, 5, 11, 15, n.Snapshot.PostChecksum, n.DatabaseChecksum, 1)}
			n.Tail = nil
		}),
		"inline mutation by new owner": func() Head {
			n := inline()
			n.Epoch, n.Owner = 3, &Owner{Node: "node-c", Session: id(6)}
			return n
		},
	}
	for name, mk := range tailBad {
		n := mk()
		if err := n.Validate(); err != nil {
			t.Fatalf("tail %s: invalid fixture: %v", name, err)
		}
		if err := Successor(&tprev, &n); err == nil {
			t.Errorf("tail %s: accepted", name)
		}
	}
	bad := map[string]func() Head{
		"same revision":       func() Head { n := mutation(); n.Revision = 5; return n },
		"skipped revision":    func() Head { n := fixture(); n.Revision, n.Nonce = 7, id(5); return n },
		"reused nonce":        func() Head { n := mutation(); n.Nonce = prev.Nonce; return n },
		"other key":           func() Head { n := mutation(); n.Key = "x"; return n },
		"other incarnation":   func() Head { n := mutation(); n.Incarnation = id(9); return n },
		"other namespace":     func() Head { n := mutation(); n.Namespace = id(9); return n },
		"owner without epoch": func() Head { n := takeover(); n.Epoch = 2; return n },
		"epoch without owner": func() Head { n := fixture(); n.Revision, n.Nonce, n.Epoch = 6, id(5), 3; return n },
		"epoch jump":          func() Head { n := takeover(); n.Epoch = 4; return n },
		"epoch backwards": func() Head {
			n := takeover()
			n.Epoch = 1
			return n
		},
		"mutation by new owner": func() Head {
			n := mutation()
			n.Epoch, n.Owner = 3, &Owner{Node: "node-c", Session: id(6)}
			return n
		},
		"sequence backwards": func() Head {
			n := fixture()
			n.Revision, n.Nonce, n.Sequence, n.DatabaseChecksum = 6, id(5), 12, n.Deltas[0].PostChecksum
			n.Deltas = n.Deltas[:1]
			return n
		},
		"data changed silently":   func() Head { n := compaction(); n.PageCount = 129; return n },
		"schema without sequence": func() Head { n := compaction(); n.Schema = 3; return n },
		"schema backwards":        func() Head { n := mutation(); n.Schema = 1; return n },
		"resurrection": func() Head {
			n := mutation()
			n.Deltas[2] = ref(n.Key, n.Incarnation, 4, 14, 14, prev.DatabaseChecksum, c4, 0)
			return n
		},
		"altered carried ref": func() Head { n := mutation(); n.Deltas[0].Level = 3; return n },
	}
	for name, mk := range bad {
		n := mk()
		if err := Successor(&prev, &n); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	g := fixture()
	g.Revision, g.Epoch, g.Deltas, g.Sequence, g.DatabaseChecksum = 1, 1, nil, 10, g.Snapshot.PostChecksum
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := Successor(nil, &g); err != nil {
		t.Errorf("genesis rejected: %v", err)
	}
	if err := Successor(nil, &prev); err == nil {
		t.Error("non-genesis accepted")
	}
	g.Tail = tail(11, 11, g.DatabaseChecksum, g.DatabaseChecksum, 1)
	if err := Successor(nil, &g); err == nil {
		t.Error("genesis with a tail accepted")
	}
}

func FuzzHeadDecode(f *testing.F) {
	h := fixture()
	b, _ := h.Encode()
	f.Add(b)
	h.Owner, h.Deltas = nil, nil
	h.Sequence, h.DatabaseChecksum = 10, h.Snapshot.PostChecksum
	b, _ = h.Encode()
	f.Add(b)
	b, _ = tailFixture().Encode()
	f.Add(b)
	h = tailFixture()
	h.Tail = tail(14, 15, h.Tail.PreChecksum, h.DatabaseChecksum, 1)
	b, _ = h.Encode()
	f.Add(b)
	f.Add([]byte(`{"format":1}`))
	f.Add([]byte(`{"format":1,"format":1}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := Decode(b)
		if err != nil {
			return
		}
		// Anything accepted is valid and is exactly its own canonical encoding.
		if err := h.Validate(); err != nil {
			t.Fatalf("accepted invalid head: %v", err)
		}
		e, err := h.Encode()
		if err != nil || !bytes.Equal(e, b) {
			t.Fatalf("accepted non-canonical head: %v", err)
		}
	})
}
