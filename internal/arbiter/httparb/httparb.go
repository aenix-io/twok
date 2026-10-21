// Package httparb keeps the arbiter record behind any HTTP service that
// implements the small contract in docs/arbiter-http.md, under the URL
// schemes http:// and https://. The package also holds a reference
// server, NewServer, that keeps records in a directory.
//
// The version is the ETag the service returns. A create is a PUT with
// If-None-Match: *, a swap is a PUT with If-Match: <etag>, and the service
// answers 412 when the condition does not hold. A 404 on a swap means the
// record is gone and is a conflict too.
//
// A bearer token is read from the file named by TWOK_HTTP_ARBITER_TOKEN_FILE
// on every request, so that it can be rotated in place. It is sent only
// over https.
package httparb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/writeid"
)

// TokenFileEnv names the file with the bearer token.
const TokenFileEnv = "TWOK_HTTP_ARBITER_TOKEN_FILE" //nolint:gosec // The name of a variable, not a credential.

const (
	// timeout bounds one request. A claim that hangs delays the takeover
	// more than one that fails and is retried.
	timeout = 10 * time.Second
	// maxRecordSize bounds what Read accepts and what the server stores.
	// A record is a few hundred bytes.
	maxRecordSize = 64 << 10
)

func init() {
	arbiter.Register("http", New)
	arbiter.Register("https", New)
}

// Arbiter is the HTTP backend. It is safe for concurrent use.
type Arbiter struct {
	client    *http.Client
	url       string
	tokenFile string
}

var _ arbiter.Arbiter = (*Arbiter)(nil)

// New builds the backend from an http:// or https:// URL and the
// environment.
func New(u *url.URL) (arbiter.Arbiter, error) {
	const form = "https://host[:port]/path"
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("http arbiter: scheme %q, want http or https", u.Scheme)
	case u.User != nil:
		return nil, fmt.Errorf("http arbiter: credentials in the URL are not accepted; put a bearer token in the file named by %s (URL form %s)", TokenFileEnv, form)
	case u.Opaque != "" || u.Host == "":
		return nil, fmt.Errorf("http arbiter: URL must be %s", form)
	case u.RawQuery != "" || u.Fragment != "":
		// The probe is the record's URL with a suffix, which a query
		// would make ambiguous.
		return nil, fmt.Errorf("http arbiter: URL must have no query or fragment (URL form %s)", form)
	case u.Path == "" || strings.HasSuffix(u.Path, "/"):
		return nil, fmt.Errorf("http arbiter: URL must name a record, not end with a slash (URL form %s)", form)
	}
	tokenFile := os.Getenv(TokenFileEnv)
	if tokenFile != "" && u.Scheme != "https" {
		return nil, fmt.Errorf("http arbiter: refusing to send the token from %s over plain http; use https", TokenFileEnv)
	}
	return &Arbiter{
		client:    &http.Client{Timeout: timeout},
		url:       u.String(),
		tokenFile: tokenFile,
	}, nil
}

// String is the record's URL, which cannot carry credentials.
func (a *Arbiter) String() string {
	return a.url
}

// Read gets the record and returns its ETag as the version.
func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	env, v, err := a.read(ctx)
	return env.Record, v, err
}

func (a *Arbiter) read(ctx context.Context) (writeid.Envelope, arbiter.Version, error) {
	resp, err := a.do(ctx, http.MethodGet, nil, nil)
	if err != nil {
		return writeid.Envelope{}, arbiter.Zero, err
	}
	switch resp.status {
	case http.StatusOK:
	case http.StatusNotFound:
		return writeid.Envelope{}, arbiter.Zero, arbiter.ErrNotFound
	default:
		return writeid.Envelope{}, arbiter.Zero, a.statusError("GET", resp)
	}
	etag := resp.header.Get("ETag")
	if etag == "" {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("http arbiter: GET %s: no ETag in the answer", a.url)
	}
	var env writeid.Envelope
	if err := json.Unmarshal(resp.body, &env); err != nil {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("http arbiter: GET %s: not a record: %w", a.url, err)
	}
	return env, arbiter.Version(etag), nil
}

// CompareAndSwap puts the record with If-None-Match: * when expected is
// Zero and with If-Match: expected otherwise.
//
// The answer to a PUT can be lost after the service applied it, and a
// proxy between twok and the service may retry it and answer 412 for the
// write it made itself. So on any failure the record is read back: if it
// carries this call's write identifier, the write happened.
func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	id := writeid.New()
	data, err := json.Marshal(writeid.Envelope{Record: record, WriteID: id})
	if err != nil {
		return arbiter.Zero, fmt.Errorf("http arbiter: %w", err)
	}
	header := http.Header{"Content-Type": {"application/json"}}
	if expected == arbiter.Zero {
		header.Set("If-None-Match", "*")
	} else {
		header.Set("If-Match", string(expected))
	}
	resp, err := a.do(ctx, http.MethodPut, header, data)
	if err == nil && (resp.status == http.StatusOK || resp.status == http.StatusCreated) {
		etag := resp.header.Get("ETag")
		if etag == "" {
			return arbiter.Zero, fmt.Errorf("http arbiter: PUT %s: no ETag in the answer", a.url)
		}
		return arbiter.Version(etag), nil
	}
	// The read back gets its own deadline: the caller's may be what just
	// expired.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if env, v, rerr := a.read(rctx); rerr == nil && env.WriteID == id {
		return v, nil
	}
	if err != nil {
		// No answer: the request may still be applied, so the caller must
		// read again before it decides.
		return arbiter.Zero, err
	}
	switch {
	case resp.status == http.StatusPreconditionFailed,
		resp.status == http.StatusNotFound && expected != arbiter.Zero:
		return arbiter.Zero, fmt.Errorf("%w: %w", arbiter.ErrConflict, a.statusError("PUT", resp))
	default:
		return arbiter.Zero, a.statusError("PUT", resp)
	}
}

// Verify runs the probe on the record's URL with the probe suffix.
func (a *Arbiter) Verify(ctx context.Context) error {
	p := *a
	p.url += arbiter.ProbeSuffix
	return arbiter.VerifyProbe(ctx, &p)
}

// answer is a complete HTTP answer, read and closed.
type answer struct {
	status     int
	statusLine string
	header     http.Header
	body       []byte
}

// do sends one request and reads the answer. An error means there was no
// complete answer.
func (a *Arbiter) do(ctx context.Context, method string, header http.Header, body []byte) (answer, error) {
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.url, reqBody)
	if err != nil {
		return answer{}, fmt.Errorf("http arbiter: %w", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Accept", "application/json")
	if a.tokenFile != "" {
		token, err := os.ReadFile(a.tokenFile)
		if err != nil {
			return answer{}, fmt.Errorf("http arbiter: token: %w", err)
		}
		t := strings.TrimSpace(string(token))
		if t == "" {
			return answer{}, fmt.Errorf("http arbiter: token file %s is empty", a.tokenFile)
		}
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return answer{}, fmt.Errorf("http arbiter: %s %s: %w", method, a.url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxRecordSize+1))
	if err != nil {
		return answer{}, fmt.Errorf("http arbiter: %s %s: %w", method, a.url, err)
	}
	if len(data) > maxRecordSize {
		return answer{}, fmt.Errorf("http arbiter: %s %s: answer is larger than %d bytes", method, a.url, maxRecordSize)
	}
	return answer{status: resp.StatusCode, statusLine: resp.Status, header: resp.Header, body: data}, nil
}

func (a *Arbiter) statusError(method string, resp answer) error {
	msg := strings.TrimSpace(string(resp.body))
	if len(msg) > 200 {
		msg = msg[:200] + "..."
	}
	return fmt.Errorf("http arbiter: %s %s: %s: %s", method, a.url, resp.statusLine, msg)
}
