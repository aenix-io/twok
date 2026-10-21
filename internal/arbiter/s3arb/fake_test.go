package s3arb

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// fakeS3 implements the part of the S3 REST API the backend uses: GET and
// PUT of an object addressed path-style, with If-Match and If-None-Match on
// PUT. ETags are quoted MD5 digests of the content, as AWS returns them for
// single-part uploads.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	types   map[string]string

	// ignoreIfNoneMatch and ignoreIfMatch make the fake behave like a store
	// that accepts the header, answers 200 and overwrites.
	ignoreIfNoneMatch bool
	ignoreIfMatch     bool
	// failPut, when set, answers the next PUTs with this error instead of
	// evaluating them, and is consumed one PUT at a time.
	failPut []s3Error
	// applyThenFail applies the next PUT and answers it with 500, as if
	// the response was lost after the store committed the write.
	applyThenFail bool
	puts          int
}

type s3Error struct {
	status int
	code   string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: make(map[string][]byte), types: make(map[string]string)}
}

func etag(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if bucket, key, ok := strings.Cut(name, "/"); !ok || bucket == "" || key == "" {
		writeError(w, s3Error{http.StatusBadRequest, "InvalidRequest"})
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		body, ok := f.objects[name]
		if !ok {
			writeError(w, s3Error{http.StatusNotFound, "NoSuchKey"})
			return
		}
		w.Header().Set("ETag", etag(body))
		w.Header().Set("Content-Type", f.types[name])
		_, _ = w.Write(body)
	case http.MethodPut:
		f.put(w, r, name)
	default:
		writeError(w, s3Error{http.StatusMethodNotAllowed, "MethodNotAllowed"})
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, name string) {
	f.puts++
	if r.Header.Get("Content-Encoding") == "aws-chunked" {
		// Many compatible stores reject chunked uploads; the backend must
		// not send them.
		writeError(w, s3Error{http.StatusNotImplemented, "NotImplemented"})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, s3Error{http.StatusBadRequest, "IncompleteBody"})
		return
	}
	if len(f.failPut) > 0 {
		e := f.failPut[0]
		f.failPut = f.failPut[1:]
		writeError(w, e)
		return
	}
	current, exists := f.objects[name]
	if inm := r.Header.Get("If-None-Match"); inm != "" && !f.ignoreIfNoneMatch {
		if inm != "*" {
			writeError(w, s3Error{http.StatusNotImplemented, "NotImplemented"})
			return
		}
		if exists {
			writeError(w, s3Error{http.StatusPreconditionFailed, "PreconditionFailed"})
			return
		}
	}
	if im := r.Header.Get("If-Match"); im != "" && !f.ignoreIfMatch {
		if !exists {
			writeError(w, s3Error{http.StatusNotFound, "NoSuchKey"})
			return
		}
		if im != etag(current) {
			writeError(w, s3Error{http.StatusPreconditionFailed, "PreconditionFailed"})
			return
		}
	}
	f.objects[name] = body
	f.types[name] = r.Header.Get("Content-Type")
	if f.applyThenFail {
		f.applyThenFail = false
		writeError(w, s3Error{http.StatusInternalServerError, "InternalError"})
		return
	}
	w.Header().Set("ETag", etag(body))
	w.WriteHeader(http.StatusOK)
}

// do runs fn under the fake's lock, for tests that inspect or arrange its
// state between requests.
func (f *fakeS3) do(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func writeError(w http.ResponseWriter, e s3Error) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>fake</Message></Error>`, e.code)
}
