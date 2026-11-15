package store

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
)

// Sealed encrypts the bodies of entity objects and blobs (keys holding
// "/objects/" or "/blobs/") under key, a 32-byte secret, and passes every
// other key through (heads, births, the namespace: DECISIONS D36). A sealed
// body is a header (magic, a random salt, the plaintext length) then the
// plaintext in Segment-byte segments, each sealed with AES-256-GCM under a
// key derived from key and the salt, its index as nonce and the header as
// additional data: a segment cannot move, the length cannot change, and an
// object can be copied as is. A range is read by fetching the segments that
// hold it. Integrity of what an object should contain stays the caller's
// (SHA-256 of refs and chunks); sealing adds confidentiality and refuses
// bytes that were not produced under key.
func Sealed(st Store, key []byte) (Store, error) {
	if len(key) != 32 {
		return nil, errors.New("store: a sealing key has 32 bytes")
	}
	return &sealed{Store: st, key: key, heads: map[string][]byte{}}, nil
}

// ErrSealBroken reports sealed bytes that fail authentication: corruption
// or forgery, never a transient fault.
var ErrSealBroken = errors.New("store: a sealed segment fails authentication")

// Segment is the plaintext size of a sealed segment, the blob chunk size.
const Segment = 1 << 20

const sealHeader = 4 + 16 + 8 // "BGS1", salt, plaintext length

type sealed struct {
	Store
	key   []byte
	mu    sync.Mutex
	heads map[string][]byte // headers of objects read, for ranges (bounded)
}

func sealable(key string) bool {
	return strings.Contains(key, "/objects/") || strings.Contains(key, "/blobs/")
}

// SealedSize is the stored size of n bytes of plaintext.
func SealedSize(n int64) int64 { return sealHeader + n + 16*max(1, (n+Segment-1)/Segment) }

func (s *sealed) aead(hdr []byte) (cipher.AEAD, error) {
	k, err := hkdf.Key(sha256.New, s.key, hdr[4:20], "big-garden sealed object v1", 32)
	if err != nil {
		return nil, err
	}
	b, _ := aes.NewCipher(k)
	return cipher.NewGCM(b)
}

// sealer is a ReaderAt over the sealed form of n plaintext bytes of body,
// safe for concurrent use (parallel parts); it keeps the segments it sealed
// last, as callers read a segment in many small reads.
type sealer struct {
	body io.ReaderAt
	n    int64
	hdr  []byte
	gcm  cipher.AEAD
	mu   sync.Mutex
	segs map[int64][]byte
}

func (s *sealed) seal(body io.ReaderAt, n int64) (*sealer, error) {
	hdr := make([]byte, sealHeader)
	copy(hdr, "BGS1")
	rand.Read(hdr[4:20])
	binary.BigEndian.PutUint64(hdr[20:], uint64(n))
	gcm, err := s.aead(hdr)
	return &sealer{body: body, n: n, hdr: hdr, gcm: gcm, segs: map[int64][]byte{}}, err
}

func (z *sealer) segment(i int64) ([]byte, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if seg, ok := z.segs[i]; ok {
		return seg, nil
	}
	plain := make([]byte, min(Segment, z.n-i*Segment))
	if _, err := z.body.ReadAt(plain, i*Segment); err != nil && err != io.EOF {
		return nil, err
	}
	if len(z.segs) >= 8 {
		clear(z.segs)
	}
	z.segs[i] = z.gcm.Seal(nil, nonce(i), plain, z.hdr)
	return z.segs[i], nil
}

func nonce(i int64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[4:], uint64(i))
	return b
}

func (z *sealer) ReadAt(p []byte, off int64) (int, error) {
	k := 0
	for k < len(p) && off < SealedSize(z.n) {
		var seg []byte
		pos := off
		if off < sealHeader {
			seg = z.hdr
		} else {
			i := (off - sealHeader) / (Segment + 16)
			var err error
			if seg, err = z.segment(i); err != nil {
				return k, err
			}
			pos = off - sealHeader - i*(Segment+16)
		}
		m := copy(p[k:], seg[pos:])
		k, off = k+m, off+int64(m)
	}
	if k < len(p) {
		return k, io.EOF
	}
	return k, nil
}

func (s *sealed) PutImmutable(ctx context.Context, key string, body []byte) error {
	if !sealable(key) {
		return s.Store.PutImmutable(ctx, key, body)
	}
	z, err := s.seal(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}
	b := make([]byte, SealedSize(z.n))
	z.ReadAt(b, 0)
	return s.Store.PutImmutable(ctx, key, b)
}

func (s *sealed) PutFile(ctx context.Context, key string, body io.ReaderAt, size int64, sum [32]byte) error {
	if !sealable(key) {
		return s.Store.PutFile(ctx, key, body, size, sum)
	}
	z, err := s.seal(body, size)
	if err != nil {
		return err
	}
	n := SealedSize(size)
	h := sha256.New() // the stored bytes' SHA-256, which the store may check
	if _, err := io.Copy(h, io.NewSectionReader(z, 0, n)); err != nil {
		return &Error{Outcome: Failed, Op: "put", Key: key, Err: err}
	}
	return s.Store.PutFile(ctx, key, z, n, [32]byte(h.Sum(nil)))
}

func (s *sealed) Get(ctx context.Context, key, ifNoneMatch string, max int64) (Object, error) {
	if !sealable(key) {
		return s.Store.Get(ctx, key, ifNoneMatch, max)
	}
	o, err := s.Store.Get(ctx, key, ifNoneMatch, SealedSize(max))
	if err != nil {
		return o, err
	}
	if len(o.Body) < sealHeader || int64(binary.BigEndian.Uint64(o.Body[20:sealHeader])) > max {
		return Object{}, &Error{Outcome: Failed, Op: "get", Key: key, Err: errors.New("sealed object above the bound, or no header")}
	}
	r, err := s.open(o.Body, 0, max, bytes.NewReader(o.Body[sealHeader:]))
	if err == nil {
		o.Body, err = io.ReadAll(r)
	}
	if err != nil {
		return Object{}, &Error{Outcome: Failed, Op: "get", Key: key, Err: err}
	}
	return o, nil
}

func (s *sealed) Open(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	if !sealable(key) {
		return s.Store.Open(ctx, key, off, n)
	}
	s.mu.Lock()
	hdr := s.heads[key]
	s.mu.Unlock()
	if hdr == nil {
		r, err := s.Store.Open(ctx, key, 0, sealHeader)
		if err != nil {
			return nil, err
		}
		hdr = make([]byte, sealHeader)
		_, err = io.ReadFull(r, hdr)
		r.Close()
		if err != nil {
			return nil, &Error{Outcome: Unknown, Op: "get", Key: key, Err: err}
		}
		s.mu.Lock()
		if len(s.heads) >= 4096 {
			clear(s.heads)
		}
		s.heads[key] = hdr
		s.mu.Unlock()
	}
	size := int64(binary.BigEndian.Uint64(hdr[20:]))
	first, last := off/Segment, (min(off+n, size)-1)/Segment
	if off >= size || n <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	from := sealHeader + first*(Segment+16)
	body, err := s.Store.Open(ctx, key, from, SealedSize(min(size, (last+1)*Segment))-from)
	if err != nil {
		return nil, err
	}
	r, err := s.open(hdr, off, n, body)
	if err != nil {
		body.Close()
		return nil, &Error{Outcome: Failed, Op: "get", Key: key, Err: err}
	}
	return opened{r, body}, nil
}

type opened struct {
	*io.PipeReader
	body io.Closer
}

func (o opened) Close() error {
	o.PipeReader.Close() // ends the decrypting goroutine
	return o.body.Close()
}

// open returns a reader of at most n plaintext bytes from off, decrypting
// the segments body yields from the one holding off.
func (s *sealed) open(hdr []byte, off, n int64, body io.Reader) (*io.PipeReader, error) {
	if len(hdr) < sealHeader || string(hdr[:4]) != "BGS1" {
		return nil, errors.New("store: not a sealed object")
	}
	hdr = hdr[:sealHeader]
	gcm, err := s.aead(hdr)
	if err != nil {
		return nil, err
	}
	size := int64(binary.BigEndian.Uint64(hdr[20:]))
	if size < 0 || off > size {
		return nil, errors.New("store: a sealed object shorter than the range")
	}
	seg, src := off/Segment, body
	pr, pw := io.Pipe()
	go func() {
		skip, left := off-seg*Segment, min(n, size-off)
		buf := make([]byte, Segment+16)
		for i := seg; left > 0; i++ {
			c := buf[:min(Segment, size-i*Segment)+16]
			if _, err := io.ReadFull(src, c); err != nil {
				pw.CloseWithError(err)
				return
			}
			plain, err := gcm.Open(c[:0], nonce(i), c, hdr)
			if err != nil {
				pw.CloseWithError(ErrSealBroken)
				return
			}
			plain = plain[skip:min(int64(len(plain)), skip+left)]
			if _, err := pw.Write(plain); err != nil {
				return
			}
			skip, left = 0, left-int64(len(plain))
		}
		pw.Close()
	}()
	return pr, nil
}
