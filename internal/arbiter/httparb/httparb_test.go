package httparb

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/castest"
)

func open(t *testing.T, raw string) *Arbiter {
	t.Helper()
	a, err := arbiter.Open(raw)
	if err != nil {
		t.Fatal(err)
	}
	return a.(*Arbiter)
}

func TestArbiterAgainstReferenceServer(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(NewServer(dir))
	t.Cleanup(srv.Close)
	castest.Exercise(t, open(t, srv.URL+"/clusters/prod"))
	for _, name := range []string{"prod", "prod.probe"} {
		if _, err := os.Stat(filepath.Join(dir, "clusters", name)); err != nil {
			t.Errorf("record file: %v", err)
		}
	}
}

func TestVerifyRefusesServerThatIgnoresConditions(t *testing.T) {
	srv := httptest.NewServer(&server{dir: t.TempDir(), ignoreConditions: true})
	t.Cleanup(srv.Close)
	castest.ExpectUnsupported(t, open(t, srv.URL+"/prod"))
}

// TestConcurrentCreates checks that the server's comparison and write are
// one step: of many creates at once, exactly one wins.
func TestConcurrentCreates(t *testing.T) {
	srv := httptest.NewServer(NewServer(t.TempDir()))
	t.Cleanup(srv.Close)
	a := open(t, srv.URL+"/prod")
	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			_, errs[i] = a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(uint8(i)))
		})
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, arbiter.ErrConflict):
			t.Errorf("unexpected error: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d creates won, want 1", won)
	}
}

// lossy forwards to the reference server and then replaces the answer to
// a PUT with 502, as a proxy that lost the upstream response would.
type lossy struct {
	next    http.Handler
	forward bool
	mu      sync.Mutex
	armed   bool
}

func (l *lossy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	armed := l.armed && r.Method == http.MethodPut
	if armed {
		l.armed = false
	}
	l.mu.Unlock()
	if !armed {
		l.next.ServeHTTP(w, r)
		return
	}
	if l.forward {
		l.next.ServeHTTP(httptest.NewRecorder(), r)
	}
	http.Error(w, "upstream response lost", http.StatusBadGateway)
}

func TestLostResponse(t *testing.T) {
	tests := []struct {
		name    string
		forward bool
	}{
		{name: "applied, answer lost", forward: true},
		{name: "never applied", forward: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &lossy{next: NewServer(t.TempDir()), forward: tt.forward}
			srv := httptest.NewServer(l)
			t.Cleanup(srv.Close)
			a := open(t, srv.URL+"/prod")
			v1, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
			if err != nil {
				t.Fatal(err)
			}
			l.mu.Lock()
			l.armed = true
			l.mu.Unlock()
			v2, err := a.CompareAndSwap(t.Context(), v1, castest.Record(2))
			rec, v, rerr := a.Read(t.Context())
			if rerr != nil {
				t.Fatal(rerr)
			}
			if tt.forward {
				if err != nil || v2 != v || rec.Generation != 2 {
					t.Fatalf("CompareAndSwap = %q, %v; Read = generation %d at %q", v2, err, rec.Generation, v)
				}
				return
			}
			if err == nil || errors.Is(err, arbiter.ErrConflict) {
				t.Fatalf("CompareAndSwap on a write that never happened: err = %v, want a non-conflict error", err)
			}
			if rec.Generation != 1 || v != v1 {
				t.Fatalf("Read = generation %d at %q, want 1 at %q", rec.Generation, v, v1)
			}
		})
	}
}

func TestTokenIsSentAndReread(t *testing.T) {
	var mu sync.Mutex
	want := "token-1"
	inner := NewServer(t.TempDir())
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ok := r.Header.Get("Authorization") == "Bearer "+want
		mu.Unlock()
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	write := func(token string) {
		if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		want = token
		mu.Unlock()
	}
	write("token-1")
	t.Setenv(TokenFileEnv, tokenFile)
	a := open(t, srv.URL+"/prod")
	a.client = srv.Client()
	if _, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1)); err != nil {
		t.Fatal(err)
	}
	write("token-2")
	if _, _, err := a.Read(t.Context()); err != nil {
		t.Fatalf("Read after rotation: %v", err)
	}
	if strings.Contains(a.String(), "token") {
		t.Fatalf("String() = %q", a)
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		raw        string
		token      bool
		errContain string
	}{
		{raw: "https://arbiter.example/twok/prod"},
		{raw: "http://127.0.0.1:8080/prod"},
		{raw: "http://127.0.0.1:8080/prod", token: true, errContain: "plain http"},
		{raw: "https://user:secret@arbiter.example/prod", errContain: "credentials"},
		{raw: "https://arbiter.example/prod?x=1", errContain: "no query"},
		{raw: "https://arbiter.example", errContain: "name a record"},
		{raw: "https://arbiter.example/dir/", errContain: "name a record"},
		{raw: "https:///prod", errContain: "URL must be"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			t.Setenv(TokenFileEnv, map[bool]string{true: "/token"}[tt.token])
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(u)
			if tt.errContain == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.errContain) {
				t.Fatalf("New(%q) err = %v, want one containing %q", tt.raw, err, tt.errContain)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error %q leaks the password", err)
			}
		})
	}
}

func TestServerContract(t *testing.T) {
	h := NewServer(t.TempDir())
	type result struct {
		status int
		etag   string
		body   string
	}
	do := func(method, path string, header map[string]string, body string) result {
		t.Helper()
		req := httptest.NewRequest(method, "http://arbiter"+path, strings.NewReader(body))
		req.URL.Path = path // keep the path exactly as given, unclean or not
		for k, v := range header {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		resp := rec.Result()
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return result{status: resp.StatusCode, etag: resp.Header.Get("ETag"), body: string(b)}
	}
	const body = `{"schemaVersion":1,"generation":1}`

	created := do(http.MethodPut, "/a/prod", map[string]string{"If-None-Match": "*"}, body)
	etag := created.etag
	tests := []struct {
		name   string
		resp   result
		status int
	}{
		{name: "create", resp: created, status: http.StatusCreated},
		{name: "get", resp: do(http.MethodGet, "/a/prod", nil, ""), status: http.StatusOK},
		{name: "get missing", resp: do(http.MethodGet, "/a/other", nil, ""), status: http.StatusNotFound},
		{name: "create existing", resp: do(http.MethodPut, "/a/prod", map[string]string{"If-None-Match": "*"}, body), status: http.StatusPreconditionFailed},
		{name: "swap from a wrong etag", resp: do(http.MethodPut, "/a/prod", map[string]string{"If-Match": `"x"`}, body), status: http.StatusPreconditionFailed},
		{name: "swap missing", resp: do(http.MethodPut, "/a/other", map[string]string{"If-Match": etag}, body), status: http.StatusPreconditionFailed},
		{name: "unconditional put", resp: do(http.MethodPut, "/a/prod", nil, body), status: http.StatusPreconditionRequired},
		{name: "If-None-Match other than *", resp: do(http.MethodPut, "/a/prod", map[string]string{"If-None-Match": etag}, body), status: http.StatusBadRequest},
		{name: "both conditions", resp: do(http.MethodPut, "/a/prod", map[string]string{"If-None-Match": "*", "If-Match": etag}, body), status: http.StatusBadRequest},
		{name: "not JSON", resp: do(http.MethodPut, "/a/new", map[string]string{"If-None-Match": "*"}, "generation=1"), status: http.StatusBadRequest},
		{name: "JSON but not an object", resp: do(http.MethodPut, "/a/new", map[string]string{"If-None-Match": "*"}, "[]"), status: http.StatusBadRequest},
		{name: "too large", resp: do(http.MethodPut, "/a/new", map[string]string{"If-None-Match": "*"}, `{"x":"`+strings.Repeat("a", maxRecordSize)+`"}`), status: http.StatusRequestEntityTooLarge},
		{name: "root", resp: do(http.MethodGet, "/", nil, ""), status: http.StatusBadRequest},
		{name: "unclean path", resp: do(http.MethodGet, "/a/../a/prod", nil, ""), status: http.StatusBadRequest},
		{name: "double slash", resp: do(http.MethodGet, "//a/prod", nil, ""), status: http.StatusBadRequest},
		{name: "dot segment", resp: do(http.MethodGet, "/a/.tmp-123", nil, ""), status: http.StatusBadRequest},
		{name: "delete", resp: do(http.MethodDelete, "/a/prod", nil, ""), status: http.StatusMethodNotAllowed},
		{name: "swap", resp: do(http.MethodPut, "/a/prod", map[string]string{"If-Match": etag}, `{"generation":2}`), status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.resp.status != tt.status {
				t.Fatalf("status = %d (%s), want %d", tt.resp.status, tt.resp.body, tt.status)
			}
			if tt.status/100 == 2 && tt.resp.etag == "" {
				t.Fatal("no ETag")
			}
		})
	}
}
