package storetest

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/OrdalieTech/big-garden/internal/store"
)

// Handler serves the path-style S3 subset the adapter uses (GetObject,
// conditional PutObject, ListObjectsV2, DeleteObject) from m, for adapter
// tests without a real S3 server. It does not check signatures.
func Handler(m *Mem) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		ctx := context.WithoutCancel(r.Context()) // a sent request applies
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodGet && q.Get("list-type") == "2":
			limit, _ := strconv.Atoi(q.Get("max-keys"))
			page, more, err := m.List(ctx, q.Get("prefix"), q.Get("start-after"), limit)
			if err != nil {
				s3Error(w, err)
				return
			}
			type content struct {
				Key  string
				Size int64
			}
			res := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				IsTruncated bool
				KeyCount    int
				Contents    []content
			}{IsTruncated: more, KeyCount: len(page)}
			for _, e := range page {
				res.Contents = append(res.Contents, content{e.Key, e.Size})
			}
			w.Header().Set("Content-Type", "application/xml")
			xml.NewEncoder(w).Encode(res)
		case r.Method == http.MethodGet:
			o, err := m.Get(ctx, key, r.Header.Get("If-None-Match"), 1<<30)
			if store.OutcomeOf(err) == store.NotModified {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			if err != nil {
				s3Error(w, err)
				return
			}
			w.Header().Set("ETag", o.ETag)
			w.Header().Set("Content-Length", strconv.Itoa(len(o.Body)))
			w.Write(o.Body)
		case r.Method == http.MethodPut:
			body, err := io.ReadAll(r.Body)
			cond := r.Header.Get("If-Match")
			if err != nil || r.Header.Get("Content-Encoding") != "" || (cond == "") != (r.Header.Get("If-None-Match") == "*") {
				s3Error(w, &store.Error{Outcome: store.Failed, Err: fmt.Errorf("unsupported request")})
				return
			}
			etag, err := m.PutCAS(ctx, key, body, cond)
			if err != nil {
				s3Error(w, err)
				return
			}
			w.Header().Set("ETag", etag)
		case r.Method == http.MethodDelete:
			if err := m.Delete(ctx, key); err != nil {
				s3Error(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			s3Error(w, &store.Error{Outcome: store.Failed, Err: fmt.Errorf("unsupported request")})
		}
	})
}

// s3Error replies with the status and code a real S3 server uses for o.
func s3Error(w http.ResponseWriter, err error) {
	status, code := 500, "InternalError"
	switch store.OutcomeOf(err) {
	case store.NotFound:
		status, code = 404, "NoSuchKey"
	case store.PreconditionFailed:
		status, code = 412, "PreconditionFailed"
	case store.Failed:
		status, code = 400, "InvalidRequest"
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>%s</Code><Message>%s</Message></Error>", code, code)
}
