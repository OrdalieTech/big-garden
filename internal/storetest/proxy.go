package storetest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Fault is the fate of one request through a Proxy. The zero Fault forwards
// the request and its reply unchanged.
type Fault struct {
	// Status, when nonzero, replies with this status without forwarding
	// anything: nothing applies. The body is an S3 error document with Code,
	// or empty when Code is "".
	Status int
	Code   string
	// Hold delays forwarding until it is closed, even after the client gave
	// up: the upstream request then runs and applies with nobody waiting.
	Hold <-chan struct{}
	// CorruptRequest flips the middle byte of the request body after signing.
	CorruptRequest bool
	// Delay holds the upstream reply this long before relaying it.
	Delay time.Duration
	// Drop cuts the client connection instead of relaying the upstream reply.
	Drop bool
	// Truncate relays the full Content-Length but only half of the body.
	Truncate bool
	// Corrupt flips the middle byte of the relayed body.
	Corrupt bool
}

// Request is one journal entry of a Proxy.
type Request struct {
	Seq                    int
	Method, Path, RawQuery string
	IfMatch, IfNoneMatch   string
	Body                   []byte // request body as received (before CorruptRequest)
	Fault                  Fault
	Upstream               int // upstream status; 0 if never forwarded or no reply
	Relayed                bool
}

// Proxy forwards S3 requests byte for byte, Host and signature included, to
// one upstream endpoint, and injects the faults its Plan chooses. The
// upstream request never depends on the client's connection: a client that
// cancels leaves it running, which is the dangerous late-apply window.
type Proxy struct {
	plan     func(Request) Fault
	upstream *url.URL
	ln       net.Listener
	srv      *http.Server
	fwd      *http.Client
	mu       sync.Mutex
	idle     *sync.Cond // signaled when inflight drops to 0
	inflight int        // upstream requests not finished
	journal  []Request
}

// NewProxy listens on 127.0.0.1 and forwards to upstream (scheme://host[:port]).
func NewProxy(upstream string) (*Proxy, error) {
	u, err := url.Parse(upstream)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("proxy upstream %q: want scheme://host", upstream)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	p := &Proxy{upstream: u, ln: ln, fwd: &http.Client{Transport: tr}}
	p.idle = sync.NewCond(&p.mu)
	p.srv = &http.Server{Handler: p}
	go p.srv.Serve(ln)
	return p, nil
}

// Close stops the listener and waits for every forwarded request.
func (p *Proxy) Close() {
	p.srv.Close()
	p.Wait()
}

// SetPlan makes plan choose the fault of every later request (nil: none).
// plan runs under the proxy's lock and must not call the proxy.
func (p *Proxy) SetPlan(plan func(Request) Fault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plan = plan
}

// Wait blocks until every upstream request started so far has finished.
func (p *Proxy) Wait() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.inflight > 0 {
		p.idle.Wait()
	}
}

// Journal returns a copy of every request seen, in arrival order.
func (p *Proxy) Journal() []Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Request(nil), p.journal...)
}

// HTTPClient returns a client that sends every request to the proxy over
// plain HTTP while keeping the Host the SDK signed. Use it as the SDK
// client's HTTPClient with the real endpoint URL.
func (p *Proxy) HTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	return &http.Client{Transport: redirect{tr, p.ln.Addr().String()}}
}

type redirect struct {
	base *http.Transport
	addr string
}

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	req.URL.Scheme, req.URL.Host = "http", r.addr
	return r.base.RoundTrip(req)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	p.mu.Lock()
	req := Request{Seq: len(p.journal), Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery,
		IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match"), Body: bytes.Clone(body)}
	if p.plan != nil {
		req.Fault = p.plan(req)
	}
	p.journal = append(p.journal, req)
	if req.Fault.Status == 0 {
		p.inflight++
	}
	p.mu.Unlock()
	f := req.Fault
	if f.Status != 0 {
		if f.Code == "" {
			w.WriteHeader(f.Status)
			p.record(req.Seq, 0, true)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(f.Status)
		fmt.Fprintf(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>%s</Code><Message>injected</Message></Error>", f.Code)
		p.record(req.Seq, 0, true)
		return
	}
	if f.CorruptRequest && len(body) > 0 {
		body[len(body)/2] ^= 0xff
	}
	type reply struct {
		resp *http.Response
		body []byte
	}
	replies := make(chan reply, 1)
	go func() { // detached from the client: it runs to completion regardless
		defer func() {
			p.mu.Lock()
			if p.inflight--; p.inflight == 0 {
				p.idle.Broadcast()
			}
			p.mu.Unlock()
		}()
		if f.Hold != nil {
			<-f.Hold
		}
		out, err := http.NewRequestWithContext(context.Background(), r.Method,
			p.upstream.Scheme+"://"+p.upstream.Host+r.RequestURI, bytes.NewReader(body))
		if err != nil {
			replies <- reply{}
			return
		}
		out.Header = r.Header.Clone()
		dropHop(out.Header)
		out.Host = r.Host
		out.ContentLength = int64(len(body))
		resp, err := p.fwd.Do(out)
		if err != nil {
			replies <- reply{}
			return
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		p.record(req.Seq, resp.StatusCode, false)
		if err != nil {
			replies <- reply{}
			return
		}
		replies <- reply{resp, b}
	}()
	var rep reply
	select {
	case rep = <-replies:
	case <-r.Context().Done():
		return // client gone; the upstream request carries on
	}
	if rep.resp == nil || f.Drop {
		panic(http.ErrAbortHandler)
	}
	if f.Delay > 0 {
		select {
		case <-time.After(f.Delay):
		case <-r.Context().Done():
			return
		}
	}
	for k, v := range rep.resp.Header {
		w.Header()[k] = v
	}
	dropHop(w.Header())
	b := rep.body
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	switch {
	case f.Truncate:
		b = b[:len(b)/2]
	case f.Corrupt && len(b) > 0:
		b = bytes.Clone(b)
		b[len(b)/2] ^= 0xff
	}
	w.WriteHeader(rep.resp.StatusCode)
	w.Write(b)
	p.record(req.Seq, 0, true)
}

func (p *Proxy) record(seq, upstream int, relayed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if upstream != 0 {
		p.journal[seq].Upstream = upstream
	}
	p.journal[seq].Relayed = p.journal[seq].Relayed || relayed
}

func dropHop(h http.Header) {
	for _, k := range []string{"Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade", "Te", "Trailer"} {
		h.Del(k)
	}
}
