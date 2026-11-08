package garden

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var update = flag.Bool("update", false, "rewrite testdata/api.txt")

// TestAPIFrozen renders every exported declaration of the package (without
// comments or unexported fields) and compares it with testdata/api.txt.
// The public API is frozen at M1 (PLAN §4.1, §9.2, §11.4): a diff here is a
// contract change and needs a DECISIONS.md entry, not a -update.
func TestAPIFrozen(t *testing.T) {
	fset := token.NewFileSet()
	files, _ := filepath.Glob("*.go")
	var decls []string
	render := func(n any) {
		var b bytes.Buffer
		if err := printer.Fprint(&b, fset, n); err != nil {
			t.Fatal(err)
		}
		decls = append(decls, b.String())
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() || (d.Recv != nil && !ast.IsExported(recvName(d.Recv))) {
					continue
				}
				d.Doc, d.Body = nil, nil
				for _, fl := range []*ast.FieldList{d.Recv, d.Type.Params, d.Type.Results} {
					if fl != nil { // parameter names are not part of the API
						for _, f := range fl.List {
							f.Names = nil
						}
					}
				}
				render(d)
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							ast.Inspect(s.Type, func(n ast.Node) bool {
								if st, ok := n.(*ast.StructType); ok {
									st.Fields.List = slices.DeleteFunc(st.Fields.List, func(f *ast.Field) bool {
										return len(f.Names) > 0 && !f.Names[0].IsExported()
									})
								}
								return true
							})
							s.Doc, s.Comment = nil, nil
							render(&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{s}})
						}
					case *ast.ValueSpec:
						if s.Names[0].IsExported() {
							s.Doc, s.Comment = nil, nil
							render(&ast.GenDecl{Tok: d.Tok, Specs: []ast.Spec{s}})
						}
					}
				}
			}
		}
	}
	slices.Sort(decls)
	got := strings.Join(decls, "\n") + "\n"
	if *update {
		if err := os.WriteFile("testdata/api.txt", []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile("testdata/api.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("exported API differs from testdata/api.txt:\n%s", got)
	}
	types, callables := 0, 0
	for _, d := range decls {
		switch {
		case strings.HasPrefix(d, "type "):
			types++
		case strings.HasPrefix(d, "func "):
			callables++
		}
	}
	if types > 18 || callables > 30 {
		t.Fatalf("API budget exceeded: %d types (max 18), %d functions/methods (max 30)", types, callables)
	}
}

func recvName(r *ast.FieldList) string {
	t := r.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	return t.(*ast.Ident).Name
}

func TestErrorCodes(t *testing.T) {
	codes := []Code{CodeNotFound, CodeNotOwner, CodeRouteChanged, CodeIncarnationMismatch, CodeSchemaMismatch,
		CodeIDConflict, CodeIDExpired, CodeAbsentAtVersion, CodeOutcomeUnknown, CodeUnavailable, CodeCapacity,
		CodeUnsupported, CodeCorrupt, CodeStateMissing, CodeClosed, CodeInvalidArgument}
	want := "not_found not_owner route_changed incarnation_mismatch schema_mismatch id_conflict id_expired " +
		"absent_at_version outcome_unknown unavailable capacity unsupported corrupt state_missing closed invalid_argument"
	var got []string
	for _, c := range codes {
		got = append(got, string(c))
	}
	if strings.Join(got, " ") != want {
		t.Fatalf("codes changed: %v", got)
	}

	cause := context.DeadlineExceeded
	var err error = newError(CodeUnavailable, strings.Repeat("é", 200), cause)
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeUnavailable || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.As/Is: %v", err)
	}
	if len(e.Message) > maxMessage || !utf8.ValidString(e.Message) {
		t.Fatalf("message not bounded to valid UTF-8: %d bytes", len(e.Message))
	}
	if s := newError(CodeCorrupt, "", errors.New("provider body")).Error(); s != "state: corrupt" {
		t.Fatalf("Error() leaks the cause: %q", s)
	}
}

func TestLimits(t *testing.T) {
	l, err := Limits{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	want := Limits{EntityBytes: 64 << 20, CacheBytes: 8 << 30, ResultBytes: 16 << 10, ResidentEntities: 256,
		InflightStorage: 64, PendingPerEntity: 64, Recoveries: 8, BatchMax: 16, StorageAttemptTimeout: 2 * time.Second,
		CommitTimeout: 10 * time.Second, IdleTimeout: time.Minute, ReceiptRetention: time.Hour}
	if l != want {
		t.Fatalf("defaults:\n got %+v\nwant %+v", l, want)
	}
	if _, err := (Limits{EntityBytes: 256 << 20, CacheBytes: 16 << 30}).resolve(); err != nil {
		t.Fatalf("256 MiB ceiling: %v", err)
	}
	bad := map[string]Limits{
		"negative entity":       {EntityBytes: -1},
		"negative timeout":      {CommitTimeout: -time.Second},
		"negative count":        {Recoveries: -1},
		"huge count":            {PendingPerEntity: maxCount + 1},
		"entity above ceiling":  {EntityBytes: 256<<20 + 1, CacheBytes: 1 << 40},
		"entity below one page": {EntityBytes: 100},
		"result above 64 KiB":   {ResultBytes: 64<<10 + 1},
		"batch above 16":        {BatchMax: 17},
		"retention below 1h":    {ReceiptRetention: 59 * time.Minute},
		"retention above 720h":  {ReceiptRetention: 721 * time.Hour},
		"commit below attempt":  {StorageAttemptTimeout: 20 * time.Second},
		"cache too small":       {CacheBytes: 1 << 30},
		"256 MiB default cache": {EntityBytes: 256 << 20},
	}
	for name, l := range bad {
		var e *Error
		if _, err := l.resolve(); !errors.As(err, &e) || e.Code != CodeInvalidArgument {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestOpenValidatesConfig(t *testing.T) {
	ok := Config{S3: s3.New(s3.Options{Region: "x"}), Bucket: "b", Prefix: "a/b", NodeID: "n", CacheDir: "/tmp/x"}
	var e *Error
	// A valid config passes validation and reaches storage, which is unreachable here.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Open(ctx, ok); !errors.As(err, &e) || e.Code != CodeUnavailable {
		t.Fatalf("valid config: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"no client": func(c *Config) { c.S3 = nil }, "no bucket": func(c *Config) { c.Bucket = "" },
		"no prefix": func(c *Config) { c.Prefix = "" }, "leading slash": func(c *Config) { c.Prefix = "/a" },
		"trailing slash": func(c *Config) { c.Prefix = "a/" }, "dot dot": func(c *Config) { c.Prefix = "a/../b" },
		"empty segment": func(c *Config) { c.Prefix = "a//b" }, "bad utf8": func(c *Config) { c.Prefix = "\xff" },
		"no node": func(c *Config) { c.NodeID = "" }, "long node": func(c *Config) { c.NodeID = strings.Repeat("n", 129) },
		"no cache": func(c *Config) { c.CacheDir = "" }, "bad limits": func(c *Config) { c.Limits.BatchMax = -1 },
	} {
		c := ok
		mutate(&c)
		if _, err := Open(context.Background(), c); !errors.As(err, &e) || e.Code != CodeInvalidArgument {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestPreferred(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	counts := map[string]int{}
	for i := range 2000 {
		key := "key-" + string(rune('A'+i%26)) + strings.Repeat("x", i%7) + string(rune(i))
		p, err := Preferred(key, nodes)
		if err != nil {
			t.Fatal(err)
		}
		rev := slices.Clone(nodes)
		slices.Reverse(rev)
		if q, _ := Preferred(key, rev); q != p {
			t.Fatalf("order dependent: %q vs %q", p, q)
		}
		// Removing a non-preferred node never moves the key.
		other := slices.DeleteFunc(slices.Clone(nodes), func(n string) bool { return n != p && n == nodes[i%5] })
		if q, _ := Preferred(key, other); q != p {
			t.Fatalf("removing another node moved %q from %q to %q", key, p, q)
		}
		counts[p]++
	}
	for _, n := range nodes {
		if counts[n] < 300 {
			t.Errorf("node %s preferred only %d/2000 times", n, counts[n])
		}
	}
	// Pinned vector: the placement function is a cross-version contract.
	if p, _ := Preferred("tenant-42", []string{"node-a", "node-b", "node-c"}); p != "node-a" { // computed independently with Python hashlib
		t.Fatalf("placement changed: %q", p)
	}
	for name, ids := range map[string][]string{"empty": nil, "dup": {"a", "a"}, "blank": {"a", ""}} {
		if _, err := Preferred("k", ids); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := Preferred("", nodes); err == nil {
		t.Error("empty key accepted")
	}
}

func TestNewMutationID(t *testing.T) {
	now := time.UnixMilli(1_762_473_600_123)
	a, err := NewMutationID(now)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewMutationID(now)
	if a == b || !bytes.Equal(a[:8], []byte{0, 0, 0x01, 0x9a, 0x5b, 0x9c, 0xe4, 0x7b}) {
		t.Fatalf("bad IDs %x %x", a, b)
	}
	if _, err := NewMutationID(time.Time{}); err == nil {
		t.Fatal("zero time accepted")
	}
}
