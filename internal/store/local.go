package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Local is a linearizable filesystem-backed Store under one root directory,
// intended for tests and single-machine runs without an S3 bucket.
type Local struct {
	root string
	mu   sync.Mutex
	n    [4]struct{ attempts, failures, up, down atomic.Uint64 }
	cas  [3]atomic.Uint64
}

// NewLocal returns a Local store rooted at dir/prefix (or dir if prefix is "").
func NewLocal(dir, prefix string) (*Local, error) {
	if dir == "" {
		return nil, errors.New("store: local dir is empty")
	}
	root := filepath.Clean(dir)
	if prefix != "" {
		root = filepath.Join(root, filepath.FromSlash(prefix))
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

// Counts returns physical request counters matching S3.Counts.
func (l *Local) Counts() Counts {
	var c Counts
	for i := range l.n {
		c.Attempts[i], c.Failures[i] = l.n[i].attempts.Load(), l.n[i].failures.Load()
		c.Uploaded[i], c.Downloaded[i] = l.n[i].up.Load(), l.n[i].down.Load()
	}
	c.CASAttempts, c.CASConflicts, c.AmbiguousPuts = l.cas[0].Load(), l.cas[1].Load(), l.cas[2].Load()
	return c
}

func (l *Local) done(op int, up, down int64, err error) error {
	n := &l.n[op]
	n.attempts.Add(1)
	n.up.Add(uint64(up))
	n.down.Add(uint64(down))
	if o := OutcomeOf(err); o == Unknown || o == Failed {
		n.failures.Add(1)
	}
	return err
}

func (l *Local) withLock(fn func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(l.root, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := lockFile(f); err != nil {
		return err
	}
	defer unlockFile(f)
	return fn()
}

func (l *Local) pathOf(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", fmt.Errorf("invalid key %q", key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

func (l *Local) readETag(p string) (string, bool, error) {
	b, err := os.ReadFile(p + ".etag")
	if errors.Is(err, os.ErrNotExist) {
		if _, serr := os.Stat(p); serr != nil {
			if errors.Is(serr, os.ErrNotExist) {
				return "", false, nil
			}
			return "", false, serr
		}
		return "\"0\"", true, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(b), true, nil
}

func freshETag() string {
	var b [12]byte
	rand.Read(b[:])
	return `"` + hex.EncodeToString(b[:]) + `"`
}

func writeAtomic(path string, r io.Reader) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	n, err := io.Copy(tmp, r)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpName)
		return n, err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return n, err
	}
	return n, nil
}

func (l *Local) Get(ctx context.Context, key, ifNoneMatch string, max int64) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, l.done(opGet, 0, 0, &Error{Outcome: Unknown, Op: "get", Key: key, Err: err})
	}
	p, err := l.pathOf(key)
	if err != nil {
		return Object{}, l.done(opGet, 0, 0, failed("get", key, Failed, err.Error()))
	}
	var obj Object
	err = l.withLock(func() error {
		etag, ok, err := l.readETag(p)
		if err != nil {
			return &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		if !ok {
			return failed("get", key, NotFound, "no such key")
		}
		if ifNoneMatch != "" && etag == ifNoneMatch {
			return failed("get", key, NotModified, "not modified")
		}
		st, err := os.Stat(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return failed("get", key, NotFound, "no such key")
			}
			return &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		if st.Size() > max {
			return failed("get", key, Failed, fmt.Sprintf("object of %d bytes exceeds %d", st.Size(), max))
		}
		body, err := os.ReadFile(p)
		if err != nil {
			return &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		obj = Object{Body: body, ETag: etag}
		return nil
	})
	return obj, l.done(opGet, 0, int64(len(obj.Body)), err)
}

func (l *Local) put(ctx context.Context, op, key string, body []byte, ifMatch string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", l.done(opPut, 0, 0, &Error{Outcome: Unknown, Op: op, Key: key, Err: err})
	}
	p, err := l.pathOf(key)
	if err != nil {
		return "", l.done(opPut, 0, 0, failed(op, key, Failed, err.Error()))
	}
	var etag string
	err = l.withLock(func() error {
		cur, ok, err := l.readETag(p)
		if err != nil {
			return &Error{Outcome: Unknown, Op: op, Key: key, Err: err}
		}
		if (ifMatch == "" && ok) || (ifMatch != "" && (!ok || cur != ifMatch)) {
			return failed(op, key, PreconditionFailed, "precondition failed")
		}
		if _, err := writeAtomic(p, strings.NewReader(string(body))); err != nil {
			return &Error{Outcome: Unknown, Op: op, Key: key, Err: err}
		}
		etag = freshETag()
		if _, err := writeAtomic(p+".etag", strings.NewReader(etag)); err != nil {
			return &Error{Outcome: Unknown, Op: op, Key: key, Err: err}
		}
		return nil
	})
	return etag, l.done(opPut, int64(len(body)), 0, err)
}

func (l *Local) PutImmutable(ctx context.Context, key string, body []byte) error {
	_, err := l.put(ctx, "put", key, body, "")
	return err
}

func (l *Local) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	l.cas[0].Add(1)
	etag, err := l.put(ctx, "cas", key, body, ifMatch)
	if OutcomeOf(err) == PreconditionFailed {
		l.cas[1].Add(1)
	}
	return etag, err
}

func (l *Local) PutFile(ctx context.Context, key string, body io.ReaderAt, size int64, sum [32]byte) error {
	if err := ctx.Err(); err != nil {
		return l.done(opPut, 0, 0, &Error{Outcome: Unknown, Op: "put", Key: key, Err: err})
	}
	p, err := l.pathOf(key)
	if err != nil {
		return l.done(opPut, 0, 0, failed("put", key, Failed, err.Error()))
	}
	// Stage outside the lock so concurrent reads are not blocked by large writes.
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return l.done(opPut, 0, 0, &Error{Outcome: Unknown, Op: "put", Key: key, Err: err})
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return l.done(opPut, 0, 0, &Error{Outcome: Unknown, Op: "put", Key: key, Err: err})
	}
	tmpName := tmp.Name()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.NewSectionReader(body, 0, size))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil || n != size || (sum != [32]byte{} && [32]byte(h.Sum(nil)) != sum) {
		os.Remove(tmpName)
		if err == nil {
			err = errors.New("putfile size or checksum mismatch")
		}
		return l.done(opPut, n, 0, &Error{Outcome: Unknown, Op: "put", Key: key, Err: err})
	}
	err = l.withLock(func() error {
		if err := os.Rename(tmpName, p); err != nil {
			return &Error{Outcome: Unknown, Op: "put", Key: key, Err: err}
		}
		if _, err := writeAtomic(p+".etag", strings.NewReader(freshETag())); err != nil {
			return &Error{Outcome: Unknown, Op: "put", Key: key, Err: err}
		}
		return nil
	})
	if err != nil {
		os.Remove(tmpName)
	}
	return l.done(opPut, size, 0, err)
}

func (l *Local) Copy(ctx context.Context, from, to string) error {
	if err := ctx.Err(); err != nil {
		return l.done(opPut, 0, 0, &Error{Outcome: Unknown, Op: "put", Key: to, Err: err})
	}
	src, err := l.pathOf(from)
	if err != nil {
		return l.done(opPut, 0, 0, failed("put", to, Failed, err.Error()))
	}
	dst, err := l.pathOf(to)
	if err != nil {
		return l.done(opPut, 0, 0, failed("put", to, Failed, err.Error()))
	}
	err = l.withLock(func() error {
		f, err := os.Open(src)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return failed("put", from, NotFound, "no such key")
			}
			return &Error{Outcome: Unknown, Op: "put", Key: to, Err: err}
		}
		defer f.Close()
		if _, err := writeAtomic(dst, f); err != nil {
			return &Error{Outcome: Unknown, Op: "put", Key: to, Err: err}
		}
		if _, err := writeAtomic(dst+".etag", strings.NewReader(freshETag())); err != nil {
			return &Error{Outcome: Unknown, Op: "put", Key: to, Err: err}
		}
		return nil
	})
	return l.done(opPut, 0, 0, err)
}

func (l *Local) Open(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, l.done(opGet, 0, 0, &Error{Outcome: Unknown, Op: "get", Key: key, Err: err})
	}
	if off < 0 || n <= 0 {
		return nil, l.done(opGet, 0, 0, failed("get", key, Failed, "invalid range"))
	}
	p, err := l.pathOf(key)
	if err != nil {
		return nil, l.done(opGet, 0, 0, failed("get", key, Failed, err.Error()))
	}
	var f *os.File
	var size int64
	err = l.withLock(func() error {
		var err error
		f, err = os.Open(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return failed("get", key, NotFound, "no such key")
			}
			return &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		size = st.Size()
		return nil
	})
	if err != nil {
		return nil, l.done(opGet, 0, 0, err)
	}
	if off >= size {
		f.Close()
		return nil, l.done(opGet, 0, 0, failed("get", key, Failed, "offset past end"))
	}
	rem := min(n, size-off)
	return &localReader{l: l, f: f, r: io.NewSectionReader(f, off, rem)}, nil
}

type localReader struct {
	l    *Local
	f    *os.File
	r    io.Reader
	n    int64
	done bool
}

func (r *localReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *localReader) Close() error {
	if !r.done {
		r.done = true
		r.l.done(opGet, 0, r.n, nil)
	}
	return r.f.Close()
}

func (l *Local) List(ctx context.Context, prefix, after string, limit int) ([]Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, l.done(opList, 0, 0, &Error{Outcome: Unknown, Op: "list", Key: prefix, Err: err})
	}
	if limit < 1 || limit > maxListKeys {
		return nil, false, l.done(opList, 0, 0, failed("list", prefix, Failed, "limit outside 1..1000"))
	}
	var entries []Entry
	err := l.withLock(func() error {
		return filepath.WalkDir(l.root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			base := d.Name()
			if base == ".lock" || strings.HasPrefix(base, ".tmp-") || strings.HasSuffix(base, ".etag") {
				return nil
			}
			rel, err := filepath.Rel(l.root, p)
			if err != nil {
				return err
			}
			k := filepath.ToSlash(rel)
			if !strings.HasPrefix(k, prefix) || k <= after {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			entries = append(entries, Entry{Key: k, Size: info.Size()})
			return nil
		})
	})
	if err != nil {
		return nil, false, l.done(opList, 0, 0, &Error{Outcome: Unknown, Op: "list", Key: prefix, Err: err})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	more := len(entries) > limit
	if more {
		entries = entries[:limit]
	}
	return entries, more, l.done(opList, 0, 0, nil)
}

func (l *Local) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return l.done(opDelete, 0, 0, &Error{Outcome: Unknown, Op: "delete", Key: key, Err: err})
	}
	p, err := l.pathOf(key)
	if err != nil {
		return l.done(opDelete, 0, 0, failed("delete", key, Failed, err.Error()))
	}
	err = l.withLock(func() error {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return &Error{Outcome: Unknown, Op: "delete", Key: key, Err: err}
		}
		if err := os.Remove(p + ".etag"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return &Error{Outcome: Unknown, Op: "delete", Key: key, Err: err}
		}
		return nil
	})
	return l.done(opDelete, 0, 0, err)
}
