package k8sarb

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/castest"
)

const (
	testNamespace = "twok"
	testURL       = "kubernetes://" + testNamespace + "/twok-arbiter"
)

// fakeAPI implements GET, POST and PUT of v1 ConfigMaps in one namespace
// with the API server's resourceVersion semantics: every write takes the
// next value of a global counter, a create of an existing name is 409
// AlreadyExists, and an update whose resourceVersion is not the current
// one is 409 Conflict.
type fakeAPI struct {
	mu      sync.Mutex
	token   string
	rev     uint64
	objects map[string]configMap

	// ignoreExists makes a create overwrite, ignoreVersion makes an update
	// ignore the resourceVersion: a store that does not honour conditions.
	ignoreExists  bool
	ignoreVersion bool
	// status, when set, answers every write with it.
	status int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{rev: 1000, objects: make(map[string]configMap)}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		writeStatus(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	collection := "/api/v1/namespaces/" + testNamespace + "/configmaps"
	name, isObject := strings.CutPrefix(r.URL.Path, collection+"/")
	if !isObject && r.URL.Path != collection {
		writeStatus(w, http.StatusNotFound, "NotFound")
		return
	}
	switch {
	case r.Method == http.MethodGet && isObject:
		cm, ok := f.objects[name]
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound")
			return
		}
		writeJSON(w, http.StatusOK, cm)
	case r.Method == http.MethodPost && !isObject:
		f.write(w, r, "", true)
	case r.Method == http.MethodPut && isObject:
		f.write(w, r, name, false)
	default:
		writeStatus(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (f *fakeAPI) write(w http.ResponseWriter, r *http.Request, name string, create bool) {
	if r.Header.Get("Content-Type") != "application/json" {
		writeStatus(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType")
		return
	}
	var cm configMap
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &cm); err != nil || cm.Kind != "ConfigMap" || cm.Metadata.Namespace != testNamespace {
		writeStatus(w, http.StatusBadRequest, "BadRequest")
		return
	}
	if f.status != 0 {
		writeStatus(w, f.status, http.StatusText(f.status))
		return
	}
	if create {
		name = cm.Metadata.Name
		if cm.Metadata.ResourceVersion != "" {
			writeStatus(w, http.StatusBadRequest, "BadRequest")
			return
		}
		if _, exists := f.objects[name]; exists && !f.ignoreExists {
			writeStatus(w, http.StatusConflict, "AlreadyExists")
			return
		}
	} else {
		if cm.Metadata.Name != name {
			writeStatus(w, http.StatusBadRequest, "BadRequest")
			return
		}
		current, exists := f.objects[name]
		if !exists && !f.ignoreVersion {
			writeStatus(w, http.StatusNotFound, "NotFound")
			return
		}
		if cm.Metadata.ResourceVersion != current.Metadata.ResourceVersion && !f.ignoreVersion {
			writeStatus(w, http.StatusConflict, "Conflict")
			return
		}
	}
	f.rev++
	cm.Metadata.ResourceVersion = strconv.FormatUint(f.rev, 10)
	f.objects[name] = cm
	code := http.StatusOK
	if create {
		code = http.StatusCreated
	}
	writeJSON(w, code, cm)
}

func (f *fakeAPI) do(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeStatus(w http.ResponseWriter, code int, reason string) {
	writeJSON(w, code, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"reason": reason, "message": "fake: " + reason, "code": code,
	})
}

// setup starts the fake behind TLS and points the backend at it through
// the out-of-cluster environment, with the token and CA in files.
func setup(t *testing.T, f *fakeAPI) (tokenFile string) {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	tokenFile = filepath.Join(dir, "token")
	setToken(t, f, tokenFile, "token-1")
	t.Setenv(ServerEnv, srv.URL)
	t.Setenv(TokenFileEnv, tokenFile)
	t.Setenv(CAFileEnv, caFile)
	return tokenFile
}

func setToken(t *testing.T, f *fakeAPI, file, token string) {
	t.Helper()
	if err := os.WriteFile(file, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.do(func() { f.token = token })
}

func open(t *testing.T) arbiter.Arbiter {
	t.Helper()
	a, err := arbiter.Open(testURL)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestArbiter(t *testing.T) {
	f := newFakeAPI()
	setup(t, f)
	castest.Exercise(t, open(t))
	f.do(func() {
		if _, ok := f.objects["twok-arbiter.probe"]; !ok {
			t.Error("Verify did not leave the probe ConfigMap")
		}
		if got := f.objects["twok-arbiter"].Metadata.Labels["app.kubernetes.io/managed-by"]; got != "twok" {
			t.Errorf("managed-by label = %q", got)
		}
	})
}

func TestVerifyRefusesStoreThatIgnoresConditions(t *testing.T) {
	tests := []struct {
		name                        string
		ignoreExists, ignoreVersion bool
	}{
		{name: "create overwrites", ignoreExists: true},
		{name: "update ignores resourceVersion", ignoreVersion: true},
		{name: "both", ignoreExists: true, ignoreVersion: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeAPI()
			f.ignoreExists, f.ignoreVersion = tt.ignoreExists, tt.ignoreVersion
			setup(t, f)
			castest.ExpectUnsupported(t, open(t))
		})
	}
}

func TestUpdateOfDeletedConfigMapIsConflict(t *testing.T) {
	f := newFakeAPI()
	setup(t, f)
	a := open(t)
	v, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
	if err != nil {
		t.Fatal(err)
	}
	f.do(func() { delete(f.objects, "twok-arbiter") })
	if _, err := a.CompareAndSwap(t.Context(), v, castest.Record(2)); !errors.Is(err, arbiter.ErrConflict) {
		t.Fatalf("update of a deleted ConfigMap: err = %v, want ErrConflict", err)
	}
}

func TestWriteErrorsAreNotConflicts(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusInternalServerError, http.StatusUnprocessableEntity} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			f := newFakeAPI()
			f.status = code
			setup(t, f)
			a := open(t)
			_, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
			if err == nil || errors.Is(err, arbiter.ErrConflict) || !strings.Contains(err.Error(), strconv.Itoa(code)) {
				t.Fatalf("err = %v, want a non-conflict error naming %d", err, code)
			}
		})
	}
}

// TestTokenIsReadOnEveryRequest is what makes projected token rotation
// work: the API server stops accepting the old token, and the backend
// must send the new one without a restart.
func TestTokenIsReadOnEveryRequest(t *testing.T) {
	f := newFakeAPI()
	tokenFile := setup(t, f)
	a := open(t)
	if _, _, err := a.Read(t.Context()); !errors.Is(err, arbiter.ErrNotFound) {
		t.Fatalf("Read: %v", err)
	}
	setToken(t, f, tokenFile, "token-2")
	if _, _, err := a.Read(t.Context()); !errors.Is(err, arbiter.ErrNotFound) {
		t.Fatalf("Read after rotation: %v", err)
	}
	if s := a.String(); strings.Contains(s, "token-2") {
		t.Fatalf("String() = %q contains the token", s)
	}
}

func TestConfigMapWithoutRecord(t *testing.T) {
	f := newFakeAPI()
	setup(t, f)
	f.do(func() {
		f.objects["twok-arbiter"] = configMap{
			APIVersion: "v1", Kind: "ConfigMap",
			Metadata: objectMeta{Name: "twok-arbiter", Namespace: testNamespace, ResourceVersion: "5"},
			Data:     map[string]string{"other": "x"},
		}
	})
	a := open(t)
	if _, _, err := a.Read(t.Context()); err == nil || errors.Is(err, arbiter.ErrNotFound) || !strings.Contains(err.Error(), "data.record") {
		t.Fatalf("Read: err = %v, want one naming data.record", err)
	}
}

func TestInCluster(t *testing.T) {
	f := newFakeAPI()
	setup(t, f)
	srvURL, caFile, tokenFile := os.Getenv(ServerEnv), os.Getenv(CAFileEnv), os.Getenv(TokenFileEnv)
	u, err := url.Parse(srvURL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, from := range map[string]string{"ca.crt": caFile, "token": tokenFile} {
		b, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := serviceAccountDir
	serviceAccountDir = dir
	t.Cleanup(func() { serviceAccountDir = old })
	t.Setenv(ServerEnv, "")
	t.Setenv("KUBERNETES_SERVICE_HOST", u.Hostname())
	t.Setenv("KUBERNETES_SERVICE_PORT", u.Port())
	a := open(t)
	if !strings.Contains(a.String(), u.Host) {
		t.Fatalf("String() = %q, want the service address %s", a, u.Host)
	}
	castest.Exercise(t, a)
}

func TestConnectionSettings(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		errContain string
	}{
		{name: "nothing configured", errContain: ServerEnv},
		{name: "not a URL", env: map[string]string{ServerEnv: "api.example:6443"}, errContain: "not an https"},
		{name: "token over plain http", env: map[string]string{ServerEnv: "http://127.0.0.1:8001", TokenFileEnv: "/token"}, errContain: "plain http"},
		{name: "kubectl proxy", env: map[string]string{ServerEnv: "http://127.0.0.1:8001"}},
		{name: "missing CA file", env: map[string]string{ServerEnv: "https://api.example:6443", CAFileEnv: "/nonexistent/ca.crt"}, errContain: "CA"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{ServerEnv, TokenFileEnv, CAFileEnv, "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"} {
				t.Setenv(k, tt.env[k])
			}
			_, err := arbiter.Open("kubernetes://twok/arbiter")
			if tt.errContain == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.errContain) {
				t.Fatalf("err = %v, want one containing %q", err, tt.errContain)
			}
		})
	}
}

func TestParseURL(t *testing.T) {
	long := strings.Repeat("a", 248)
	tests := []struct {
		raw        string
		namespace  string
		name       string
		errContain string
	}{
		{raw: "kubernetes://twok/arbiter", namespace: "twok", name: "arbiter"},
		{raw: "kubernetes://kube-system/twok.prod", namespace: "kube-system", name: "twok.prod"},
		{raw: "kubernetes://user:secret@twok/arbiter", errContain: "credentials"},
		{raw: "kubernetes://twok", errContain: "ConfigMap name"},
		{raw: "kubernetes://twok/a/b", errContain: "ConfigMap name"},
		{raw: "kubernetes://Twok/arbiter", errContain: "namespace"},
		{raw: "kubernetes://twok/Arbiter", errContain: "ConfigMap name"},
		{raw: "kubernetes://twok/" + long, errContain: "at most 247"},
		{raw: "kubernetes://api.example:6443/arbiter", errContain: ServerEnv},
		{raw: "kubernetes://twok/arbiter?context=prod", errContain: "no query"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%.60s", tt.raw), func(t *testing.T) {
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			ns, name, err := parseURL(u)
			if tt.errContain != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContain) {
					t.Fatalf("parseURL(%q) err = %v, want one containing %q", tt.raw, err, tt.errContain)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("error %q leaks the password", err)
				}
				return
			}
			if err != nil || ns != tt.namespace || name != tt.name {
				t.Fatalf("parseURL(%q) = %q, %q, %v", tt.raw, ns, name, err)
			}
		})
	}
}
