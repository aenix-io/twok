// Package k8sarb keeps the arbiter record in a ConfigMap of a Kubernetes
// API server, under the URL scheme kubernetes://namespace/name. The record
// is the JSON in the ConfigMap's data.record.
//
// The version is the ConfigMap's metadata.resourceVersion. A create is a
// POST of the ConfigMap, which the API server refuses with 409
// AlreadyExists when it exists. A swap is a PUT with
// metadata.resourceVersion set to the expected version, which the API
// server refuses with 409 Conflict when the object has changed since; a
// 404 on the PUT means the ConfigMap was deleted, also a conflict. The API
// server evaluates both atomically against its own etcd.
//
// The ConfigMap belongs to twok: an update replaces it whole. A ConfigMap
// of that name without data.record is reported as an error, not as a
// missing record, because twok cannot create over it.
//
// The arbiter must not be the API server of the cluster whose etcd twok
// runs: that API server stops with the etcd it would arbitrate for. So the
// common setup is a Pod in one cluster and the ConfigMap in another, and
// the connection is configured in one of two ways:
//
//   - In a Pod of the arbiter's own cluster: KUBERNETES_SERVICE_HOST and
//     KUBERNETES_SERVICE_PORT, the service account token and CA in
//     /var/run/secrets/kubernetes.io/serviceaccount.
//   - Anywhere else: TWOK_KUBERNETES_SERVER (an https URL),
//     TWOK_KUBERNETES_TOKEN_FILE (a bearer token, such as a service account
//     token of the arbiter's cluster) and TWOK_KUBERNETES_CA_FILE (the API
//     server's CA; the system roots when not set). A plain http server,
//     such as kubectl proxy, is accepted only without a token file.
//
// TWOK_KUBERNETES_SERVER wins when both are present.
//
// The token file is read on every request, so that a rotated projected
// token is picked up without a restart. kubeconfig files are not supported
// on purpose: their exec plugins and auth providers need client-go, which
// twok does not depend on for three REST calls.
//
// The service account needs this Role in the ConfigMap's namespace, also
// in deploy/kubernetes/arbiter-rbac.yaml. RBAC cannot restrict create by
// name, so create is granted on all ConfigMaps of the namespace:
//
//	rules:
//	  - apiGroups: [""]
//	    resources: [configmaps]
//	    verbs: [create]
//	  - apiGroups: [""]
//	    resources: [configmaps]
//	    resourceNames: [<name>, <name>.probe]
//	    verbs: [get, update]
package k8sarb

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Scheme is the URL scheme this backend registers.
const Scheme = "kubernetes"

// The environment variables for use outside the arbiter's cluster.
const (
	ServerEnv    = "TWOK_KUBERNETES_SERVER"
	TokenFileEnv = "TWOK_KUBERNETES_TOKEN_FILE" //nolint:gosec // The name of a variable, not a credential.
	CAFileEnv    = "TWOK_KUBERNETES_CA_FILE"
)

const (
	// timeout bounds one request. A claim that hangs delays the takeover
	// more than one that fails and is retried.
	timeout = 10 * time.Second
	// maxResponseSize bounds what is read from the API server. A ConfigMap
	// is at most 1 MiB of data.
	maxResponseSize = 2 << 20
	// recordKey is the key in data that holds the record.
	recordKey = "record"
)

// serviceAccountDir is where a Pod finds its service account token and
// the cluster's CA. Tests point it elsewhere.
var serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

func init() {
	arbiter.Register(Scheme, New)
}

// Arbiter is the Kubernetes backend. It is safe for concurrent use.
type Arbiter struct {
	client    *http.Client
	server    string
	tokenFile string
	namespace string
	name      string
}

var _ arbiter.Arbiter = (*Arbiter)(nil)

// New builds the backend from a kubernetes://namespace/name URL and the
// environment.
func New(u *url.URL) (arbiter.Arbiter, error) {
	namespace, name, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	server, tokenFile, caFile, err := connectionFromEnv()
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile) //nolint:gosec // The operator names the file; reading it is the point.
		if err != nil {
			return nil, fmt.Errorf("kubernetes arbiter: CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kubernetes arbiter: no certificate in the CA file %s", caFile)
		}
		tlsConfig.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &Arbiter{
		client:    &http.Client{Transport: transport, Timeout: timeout},
		server:    server,
		tokenFile: tokenFile,
		namespace: namespace,
		name:      name,
	}, nil
}

var (
	dnsLabel     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

func parseURL(u *url.URL) (namespace, name string, err error) {
	const form = "kubernetes://namespace/name"
	switch {
	case u.Scheme != Scheme:
		return "", "", fmt.Errorf("kubernetes arbiter: scheme %q, want %q", u.Scheme, Scheme)
	case u.User != nil:
		return "", "", fmt.Errorf("kubernetes arbiter: credentials in the URL are not accepted; use %s or the service account (URL form %s)", TokenFileEnv, form)
	case u.Opaque != "":
		return "", "", fmt.Errorf("kubernetes arbiter: URL must be %s", form)
	case u.RawQuery != "" || u.Fragment != "":
		return "", "", fmt.Errorf("kubernetes arbiter: URL must have no query or fragment; the API server comes from %s or the Pod (URL form %s)", ServerEnv, form)
	case u.Port() != "":
		return "", "", fmt.Errorf("kubernetes arbiter: %q is a host, not a namespace; set the API server with %s (URL form %s)", u.Host, ServerEnv, form)
	}
	namespace, name = u.Host, strings.TrimPrefix(u.Path, "/")
	if len(namespace) > 63 || !dnsLabel.MatchString(namespace) {
		return "", "", fmt.Errorf("kubernetes arbiter: %q is not a namespace name (URL form %s)", namespace, form)
	}
	// The probe's name must be valid too, hence the room for the suffix.
	if len(name)+len(arbiter.ProbeSuffix) > 253 || !dnsSubdomain.MatchString(name) {
		return "", "", fmt.Errorf("kubernetes arbiter: %q is not a ConfigMap name of at most %d characters (URL form %s)", name, 253-len(arbiter.ProbeSuffix), form)
	}
	return namespace, name, nil
}

func connectionFromEnv() (server, tokenFile, caFile string, err error) {
	if server = os.Getenv(ServerEnv); server != "" {
		s, err := url.Parse(server)
		if err != nil || s.Host == "" || (s.Scheme != "https" && s.Scheme != "http") || s.User != nil || s.RawQuery != "" {
			return "", "", "", fmt.Errorf("kubernetes arbiter: %s=%q is not an https://host[:port] URL", ServerEnv, server)
		}
		tokenFile = os.Getenv(TokenFileEnv)
		if s.Scheme == "http" && tokenFile != "" {
			return "", "", "", fmt.Errorf("kubernetes arbiter: refusing to send the token from %s over plain http to %s", TokenFileEnv, server)
		}
		return strings.TrimSuffix(server, "/"), tokenFile, os.Getenv(CAFileEnv), nil
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return "", "", "", fmt.Errorf("kubernetes arbiter: no API server: set %s, %s and %s, or run in a Pod of the arbiter's cluster", ServerEnv, TokenFileEnv, CAFileEnv)
	}
	return "https://" + net.JoinHostPort(host, port),
		filepath.Join(serviceAccountDir, "token"),
		filepath.Join(serviceAccountDir, "ca.crt"),
		nil
}

// String names the API server, namespace and ConfigMap.
func (a *Arbiter) String() string {
	return fmt.Sprintf("kubernetes %s configmap %s/%s", a.server, a.namespace, a.name)
}

// configMap is the part of a v1 ConfigMap the backend reads and writes.
type configMap struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   objectMeta        `json:"metadata"`
	Data       map[string]string `json:"data,omitempty"`
}

type objectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// status is the body of an API error.
type status struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Read gets the ConfigMap and returns its resourceVersion as the version.
func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	code, cm, st, err := a.do(ctx, http.MethodGet, a.objectPath(), nil)
	if err != nil {
		return arbiter.Record{}, arbiter.Zero, err
	}
	switch code {
	case http.StatusOK:
	case http.StatusNotFound:
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	default:
		return arbiter.Record{}, arbiter.Zero, a.apiError("get", code, st)
	}
	raw, ok := cm.Data[recordKey]
	if !ok {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("kubernetes arbiter: configmap %s/%s exists without data.%s; it was not written by twok, delete it or choose another name", a.namespace, a.name, recordKey)
	}
	var rec arbiter.Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("kubernetes arbiter: data.%s of configmap %s/%s is not a record: %w", recordKey, a.namespace, a.name, err)
	}
	if cm.Metadata.ResourceVersion == "" {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("kubernetes arbiter: configmap %s/%s has no resourceVersion", a.namespace, a.name)
	}
	return rec, arbiter.Version(cm.Metadata.ResourceVersion), nil
}

// CompareAndSwap creates the ConfigMap when expected is Zero and updates it
// with metadata.resourceVersion set to expected otherwise.
func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return arbiter.Zero, fmt.Errorf("kubernetes arbiter: %w", err)
	}
	cm := configMap{
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Metadata: objectMeta{
			Name:            a.name,
			Namespace:       a.namespace,
			ResourceVersion: string(expected),
			Labels:          map[string]string{"app.kubernetes.io/managed-by": "twok"},
		},
		Data: map[string]string{recordKey: string(data)},
	}
	method, path, verb, want := http.MethodPut, a.objectPath(), "update", http.StatusOK
	if expected == arbiter.Zero {
		method, path, verb, want = http.MethodPost, a.collectionPath(), "create", http.StatusCreated
	}
	code, out, st, err := a.do(ctx, method, path, &cm)
	if err != nil {
		return arbiter.Zero, err
	}
	switch {
	case code == want || (code == http.StatusOK && method == http.MethodPost):
		if out.Metadata.ResourceVersion == "" {
			return arbiter.Zero, fmt.Errorf("kubernetes arbiter: %s of configmap %s/%s returned no resourceVersion", verb, a.namespace, a.name)
		}
		return arbiter.Version(out.Metadata.ResourceVersion), nil
	// 409 is AlreadyExists for a create and Conflict for an update with a
	// stale resourceVersion; 404 on an update means the ConfigMap is gone.
	case code == http.StatusConflict, code == http.StatusNotFound && method == http.MethodPut:
		return arbiter.Zero, fmt.Errorf("%w: %w", arbiter.ErrConflict, a.apiError(verb, code, st))
	default:
		return arbiter.Zero, a.apiError(verb, code, st)
	}
}

// Verify runs the probe on the ConfigMap with the probe suffix.
func (a *Arbiter) Verify(ctx context.Context) error {
	p := *a
	p.name += arbiter.ProbeSuffix
	return arbiter.VerifyProbe(ctx, &p)
}

func (a *Arbiter) collectionPath() string {
	return "/api/v1/namespaces/" + a.namespace + "/configmaps"
}

func (a *Arbiter) objectPath() string {
	return a.collectionPath() + "/" + a.name
}

// do sends one request and decodes a ConfigMap from a 2xx answer or a
// Status from any other. An error means no answer was decoded; for a
// write the outcome is then unknown.
func (a *Arbiter) do(ctx context.Context, method, path string, body *configMap) (int, configMap, status, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.server+path, reqBody)
	if err != nil {
		return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.tokenFile != "" {
		token, err := os.ReadFile(a.tokenFile)
		if err != nil {
			return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: token: %w", err)
		}
		t := strings.TrimSpace(string(token))
		if t == "" {
			return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: token file %s is empty", a.tokenFile)
		}
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: %s %s: %w", method, path, err)
	}
	if resp.StatusCode/100 == 2 {
		var cm configMap
		if err := json.Unmarshal(data, &cm); err != nil {
			return 0, configMap{}, status{}, fmt.Errorf("kubernetes arbiter: %s %s: answer is not a ConfigMap: %w", method, path, err)
		}
		return resp.StatusCode, cm, status{}, nil
	}
	var st status
	// A body that is not a Status, such as a proxy's error page, still
	// leaves the status code to decide on.
	_ = json.Unmarshal(data, &st)
	return resp.StatusCode, configMap{}, st, nil
}

func (a *Arbiter) apiError(verb string, code int, st status) error {
	msg := st.Message
	if msg == "" {
		msg = http.StatusText(code)
	}
	if st.Reason != "" {
		msg = st.Reason + ": " + msg
	}
	return fmt.Errorf("kubernetes arbiter: %s configmap %s/%s: %d %s", verb, a.namespace, a.name, code, msg)
}
