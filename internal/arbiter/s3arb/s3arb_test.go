package s3arb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/arbitertest"
	"github.com/aenix-io/twok/internal/arbiter/castest"
)

const (
	testBucket = "bucket"
	testKey    = "clusters/prod"
	testSecret = "fake-secret-key"
)

// newTestArbiter points the SDK at the fake with path-style addressing and
// static credentials. Retries happen without backoff, so that a test of
// the retry path does not sleep.
func newTestArbiter(t *testing.T, f *fakeS3) *Arbiter {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("fake-access-key", testSecret, ""),
		BaseEndpoint: aws.String(srv.URL),
		HTTPClient:   srv.Client(),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		},
	}
	return newWithConfig(cfg, testBucket, testKey, true)
}

func TestConformance(t *testing.T) {
	arbitertest.Run(t, func(t *testing.T) arbiter.Arbiter { return newTestArbiter(t, newFakeS3()) })
}

func TestStoredObject(t *testing.T) {
	f := newFakeS3()
	a := newTestArbiter(t, f)
	if _, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1)); err != nil {
		t.Fatal(err)
	}
	if err := a.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	var ct string
	var body []byte
	var probed bool
	f.do(func() {
		ct, body = f.types[testBucket+"/"+testKey], f.objects[testBucket+"/"+testKey]
		_, probed = f.objects[testBucket+"/"+testKey+arbiter.ProbeSuffix]
	})
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(string(body), `"writeID":"`) {
		t.Errorf("stored object %s has no write identifier", body)
	}
	if !probed {
		t.Error("Verify did not leave the probe next to the record")
	}
}

func TestVerifyRefusesStoreThatIgnoresConditions(t *testing.T) {
	tests := []struct {
		name               string
		ignoreNone, ignore bool
	}{
		{name: "ignores If-None-Match", ignoreNone: true},
		{name: "ignores If-Match", ignore: true},
		{name: "ignores both", ignoreNone: true, ignore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			f.ignoreIfNoneMatch, f.ignoreIfMatch = tt.ignoreNone, tt.ignore
			castest.ExpectUnsupported(t, newTestArbiter(t, f))
		})
	}
}

func TestCompareAndSwapErrors(t *testing.T) {
	tests := []struct {
		name         string
		fail         s3Error
		swap         bool
		wantConflict bool
	}{
		{name: "412 PreconditionFailed on create", fail: s3Error{http.StatusPreconditionFailed, "PreconditionFailed"}, wantConflict: true},
		{name: "412 PreconditionFailed on swap", fail: s3Error{http.StatusPreconditionFailed, "PreconditionFailed"}, swap: true, wantConflict: true},
		{name: "409 ConditionalRequestConflict", fail: s3Error{http.StatusConflict, "ConditionalRequestConflict"}, swap: true, wantConflict: true},
		{name: "404 NoSuchKey on swap", fail: s3Error{http.StatusNotFound, "NoSuchKey"}, swap: true, wantConflict: true},
		{name: "403 AccessDenied", fail: s3Error{http.StatusForbidden, "AccessDenied"}, swap: true},
		{name: "501 NotImplemented", fail: s3Error{http.StatusNotImplemented, "NotImplemented"}},
		{name: "500 InternalError on every attempt", fail: s3Error{http.StatusInternalServerError, "InternalError"}, swap: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			a := newTestArbiter(t, f)
			expected := arbiter.Zero
			if tt.swap {
				v, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
				if err != nil {
					t.Fatal(err)
				}
				expected = v
			}
			f.do(func() {
				for range 5 {
					f.failPut = append(f.failPut, tt.fail)
				}
			})
			_, err := a.CompareAndSwap(t.Context(), expected, castest.Record(2))
			if err == nil {
				t.Fatal("CompareAndSwap succeeded")
			}
			if got := errors.Is(err, arbiter.ErrConflict); got != tt.wantConflict {
				t.Fatalf("errors.Is(%v, ErrConflict) = %v, want %v", err, got, tt.wantConflict)
			}
		})
	}
}

// TestLostResponse is the case the write identifier exists for: the store
// applies the PUT and the response is lost, the SDK retries, and the retry
// fails its precondition on the write it made itself.
func TestLostResponse(t *testing.T) {
	for _, swap := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "swap"}[swap], func(t *testing.T) {
			f := newFakeS3()
			a := newTestArbiter(t, f)
			expected := arbiter.Zero
			if swap {
				v, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
				if err != nil {
					t.Fatal(err)
				}
				expected = v
			}
			var before, after int
			f.do(func() { f.applyThenFail, before = true, f.puts })
			v, err := a.CompareAndSwap(t.Context(), expected, castest.Record(2))
			if err != nil {
				t.Fatalf("CompareAndSwap after a lost response: %v", err)
			}
			f.do(func() { after = f.puts })
			if after-before < 2 {
				t.Fatalf("the SDK sent %d PUTs, the test needs the retry", after-before)
			}
			rec, rv, err := a.Read(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rv != v || rec.Generation != 2 {
				t.Fatalf("Read = generation %d at %q, want generation 2 at %q", rec.Generation, rv, v)
			}
		})
	}
}

// TestOpenFromEnvironment goes through the registry and the AWS
// environment, as twok does, with the shared config files pointed away
// from the user's own.
func TestOpenFromEnvironment(t *testing.T) {
	srv := httptest.NewServer(newFakeS3())
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	for k, v := range map[string]string{
		"AWS_CONFIG_FILE":             filepath.Join(dir, "config"),
		"AWS_SHARED_CREDENTIALS_FILE": filepath.Join(dir, "credentials"),
		"AWS_PROFILE":                 "",
		"AWS_EC2_METADATA_DISABLED":   "true",
		"AWS_ACCESS_KEY_ID":           "fake-access-key",
		"AWS_SECRET_ACCESS_KEY":       testSecret,
		"AWS_REGION":                  "eu-central-1",
		"AWS_ENDPOINT_URL":            "",
		"AWS_ENDPOINT_URL_S3":         srv.URL,
		ForcePathStyleEnv:             "true",
	} {
		t.Setenv(k, v)
	}
	n := 0
	open := func(t *testing.T) arbiter.Arbiter {
		n++
		a, err := arbiter.Open(fmt.Sprintf("s3://%s/%s-%d", testBucket, testKey, n))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	arbitertest.Run(t, open)
	s := open(t).String()
	if strings.Contains(s, testSecret) || !strings.Contains(s, srv.URL) || !strings.Contains(s, "eu-central-1") {
		t.Errorf("String() = %q, want the endpoint and region and no secret", s)
	}
}

func TestOpenNeedsRegion(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	if _, err := arbiter.Open("s3://bucket/key"); err == nil || !strings.Contains(err.Error(), "AWS_REGION") {
		t.Fatalf("Open without a region: err = %v, want one naming AWS_REGION", err)
	}
}

func TestOpenRejectsBadPathStyle(t *testing.T) {
	t.Setenv(ForcePathStyleEnv, "yes please")
	if _, err := arbiter.Open("s3://bucket/key"); err == nil || !strings.Contains(err.Error(), ForcePathStyleEnv) {
		t.Fatalf("err = %v, want one naming %s", err, ForcePathStyleEnv)
	}
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		raw        string
		bucket     string
		key        string
		errContain string
	}{
		{raw: "s3://bucket/key", bucket: "bucket", key: "key"},
		{raw: "s3://bucket/clusters/prod/arbiter.json", bucket: "bucket", key: "clusters/prod/arbiter.json"},
		{raw: "s3://user:secret@bucket/key", errContain: "credentials"},
		{raw: "s3://bucket", errContain: "no object key"},
		{raw: "s3://bucket/", errContain: "no object key"},
		{raw: "s3://bucket/dir/", errContain: "ends with a slash"},
		{raw: "s3:///key", errContain: "no bucket"},
		{raw: "s3://minio:9000/bucket/key", errContain: "AWS_ENDPOINT_URL_S3"},
		{raw: "s3://bucket/key?region=eu-west-1", errContain: "no query"},
		{raw: "s3:bucket/key", errContain: "URL must be"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			bucket, key, err := parseURL(u)
			if tt.errContain != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContain) {
					t.Fatalf("parseURL(%q) err = %v, want one containing %q", tt.raw, err, tt.errContain)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("error %q leaks the password", err)
				}
				return
			}
			if err != nil || bucket != tt.bucket || key != tt.key {
				t.Fatalf("parseURL(%q) = %q, %q, %v; want %q, %q", tt.raw, bucket, key, err, tt.bucket, tt.key)
			}
		})
	}
}

// The context is honoured: a cancelled call does not hang on the store.
func TestReadHonoursContext(t *testing.T) {
	a := newTestArbiter(t, newFakeS3())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := a.Read(ctx); err == nil || errors.Is(err, arbiter.ErrNotFound) {
		t.Fatalf("Read with a cancelled context: err = %v", err)
	}
}
