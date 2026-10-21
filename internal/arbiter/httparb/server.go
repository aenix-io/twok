package httparb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aenix-io/twok/internal/arbiter/writeid"
)

// server is the reference implementation of the contract in
// docs/arbiter-http.md. It keeps one file per record path under its
// directory and serialises all requests with one mutex, which is what
// makes the comparison of ETags and the write one atomic step. Writes go
// to a temporary file that is synced and renamed over the record, so a
// crash leaves either the old record or the new one.
//
// Files are reached through os.Root, so no request path, however crafted,
// resolves outside the directory, symbolic links included.
//
// It is a single point of failure for arbitration, not for the clusters'
// writes, which is the trade-off the arbiter model makes everywhere.
type server struct {
	dir string
	mu  sync.Mutex
	// ignoreConditions makes the server accept every PUT, like a store
	// that ignores conditional headers. Only tests set it.
	ignoreConditions bool
}

// NewServer returns the reference arbiter server, storing records under
// dir, which must exist. Every path is a record; a path that is not clean
// or has a segment that starts with a dot is refused, so that one record
// has one URL and the server's temporary files cannot be addressed. The
// server does no authentication: put it behind a proxy that does, or on a
// network only the members reach.
func NewServer(dir string) http.Handler {
	return &server{dir: dir}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, ok := recordName(r.URL.Path)
	if !ok {
		http.Error(w, "invalid record path", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.get(w, name)
	case http.MethodPut:
		s.put(w, r, name)
	default:
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// recordName maps a request path to a name relative to the directory.
func recordName(p string) (string, bool) {
	if p == "" || p == "/" || p != path.Clean(p) || !strings.HasPrefix(p, "/") || strings.ContainsAny(p, "\\\x00") {
		return "", false
	}
	for seg := range strings.SplitSeq(p[1:], "/") {
		if seg == "" || strings.HasPrefix(seg, ".") {
			return "", false
		}
	}
	return p[1:], true
}

func (s *server) get(w http.ResponseWriter, name string) {
	s.mu.Lock()
	data, err := s.read(name)
	s.mu.Unlock()
	switch {
	case errors.Is(err, fs.ErrNotExist):
		http.Error(w, "no such record", http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, "cannot read the record", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("ETag", contentETag(data))
	w.WriteHeader(http.StatusOK)
	// The stored body was checked to be a JSON object when it was written,
	// and it is served as JSON with sniffing off, so no browser renders it.
	_, _ = w.Write(data) //nolint:gosec // See above.
}

func (s *server) put(w http.ResponseWriter, r *http.Request, name string) {
	ifNoneMatch, ifMatch := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
	switch {
	case ifNoneMatch == "" && ifMatch == "":
		http.Error(w, "a PUT needs If-None-Match: * or If-Match: <etag>", http.StatusPreconditionRequired)
		return
	case ifNoneMatch != "" && ifMatch != "":
		http.Error(w, "If-None-Match and If-Match together", http.StatusBadRequest)
		return
	case ifNoneMatch != "" && ifNoneMatch != "*":
		http.Error(w, "If-None-Match must be *", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRecordSize))
	if err != nil {
		http.Error(w, "body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	if !json.Valid(data) || data[0] != '{' {
		http.Error(w, "the body must be a JSON object", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.read(name)
	exists := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		http.Error(w, "cannot read the record", http.StatusInternalServerError)
		return
	}
	if !s.ignoreConditions {
		// RFC 9110: If-None-Match: * fails when a representation exists,
		// and If-Match fails when none exists or its ETag differs. Both
		// failures are 412.
		if ifNoneMatch == "*" && exists ||
			ifMatch != "" && (!exists || ifMatch != "*" && ifMatch != contentETag(current)) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
	}
	if err := s.write(name, data); err != nil {
		http.Error(w, "cannot write the record", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", contentETag(data))
	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

// contentETag is the quoted SHA-256 of the content. A content hash would
// repeat if the same bytes were written twice, which would let a stale
// swap through; twok's records carry a fresh write identifier, so they
// never repeat.
func contentETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (s *server) read(name string) ([]byte, error) {
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", s.dir, err)
	}
	defer root.Close()
	return root.ReadFile(name)
}

func (s *server) write(name string, data []byte) (err error) {
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return err
	}
	defer root.Close()
	dir := path.Dir(name)
	if err := root.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	// The temporary name starts with a dot, which no request can name.
	tmpName := path.Join(dir, ".tmp-"+writeid.New())
	tmp, err := root.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = root.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := root.Rename(tmpName, name); err != nil {
		return err
	}
	// The rename is durable only once the directory is synced.
	d, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
