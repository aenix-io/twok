// Package s3arb keeps the arbiter record in an object of an S3-compatible
// bucket, under the URL scheme s3://bucket/key/path.
//
// The version is the object's ETag, exactly as the store returns it,
// quotes included, and never computed locally: an ETag is not always an
// MD5 of the content. A create is a PutObject with If-None-Match: *, a swap
// is a PutObject with If-Match: <etag>. Both are conditional writes the
// store evaluates atomically, which is the only thing twok needs from it.
//
// Not every S3-compatible store implements conditional writes, and some
// accept the headers, answer 200 and overwrite. That silent case is the
// dangerous one, and it is why Verify runs at startup and refuses a store
// that lets a second conditional create through. What is known, as of
// October 2026:
//
//   - AWS S3: If-None-Match on PutObject since 2024-08-20, If-Match since
//     2024-11-25, evaluated atomically
//     (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
//   - MinIO: both headers since RELEASE.2023-02-09T05-16-53Z, with
//     If-None-Match: * handled as AWS does since RELEASE.2024-05-07T06-41-25Z,
//     and If-Match on a missing object rejected since
//     RELEASE.2025-09-07T16-13-09Z. The repository is archived, and a fix
//     for conditions evaluated without read quorum is in no release.
//   - Ceph RGW: accepts the headers, but on versioning-enabled buckets the
//     check is not atomic and two creates can both succeed (ceph/ceph#72118,
//     open). Use an unversioned bucket. Verify cannot detect a race.
//   - Cloudflare R2: documented as atomic.
//   - SeaweedFS: atomic on one filer since 4.18 and across filers since 4.46.
//   - Garage: ignores both headers and overwrites; not usable.
//   - Google Cloud Storage XML API: ignores both headers on writes; not
//     usable.
//   - DigitalOcean Spaces: reported to accept If-None-Match: * over an
//     existing object with 200; treat as not usable.
//   - Backblaze B2: reported to reject the headers with 501.
//
// Configuration comes from the standard AWS environment and shared config
// files, never from the URL: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// AWS_SESSION_TOKEN, AWS_PROFILE, AWS_REGION, and AWS_ENDPOINT_URL_S3 or
// AWS_ENDPOINT_URL for a store that is not AWS. AWS_REGION is required,
// because a request signed for the wrong region fails with an error that
// does not say so; stores that ignore the region accept any value.
// TWOK_S3_FORCE_PATH_STYLE=true addresses the bucket in the path rather
// than in the host name, which MinIO, SeaweedFS and most self-hosted
// stores need.
package s3arb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/writeid"
)

// Scheme is the URL scheme this backend registers.
const Scheme = "s3"

// ForcePathStyleEnv switches to path-style addressing when set to true.
const ForcePathStyleEnv = "TWOK_S3_FORCE_PATH_STYLE"

const (
	// timeout bounds one HTTP request. The arbiter is contacted only when
	// the cluster has lost its leader, and a claim that hangs delays the
	// takeover more than one that fails and is retried.
	timeout = 10 * time.Second
	// maxRecordSize bounds what Read accepts. A record is a few hundred
	// bytes; anything much larger is not ours.
	maxRecordSize = 1 << 20
)

func init() {
	arbiter.Register(Scheme, New)
}

// Arbiter is the S3 backend. It is safe for concurrent use.
type Arbiter struct {
	client   *s3.Client
	bucket   string
	key      string
	endpoint string
	region   string
}

var _ arbiter.Arbiter = (*Arbiter)(nil)

// New builds the backend from an s3://bucket/key URL and the AWS
// environment.
func New(u *url.URL) (arbiter.Arbiter, error) {
	bucket, key, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	pathStyle := false
	if v := os.Getenv(ForcePathStyleEnv); v != "" {
		if pathStyle, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("s3 arbiter: %s=%q is not a boolean", ForcePathStyleEnv, v)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(timeout)),
	)
	if err != nil {
		return nil, fmt.Errorf("s3 arbiter: AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 arbiter: no region configured; set AWS_REGION (any value, such as us-east-1, for a store that ignores it)")
	}
	return newWithConfig(cfg, bucket, key, pathStyle), nil
}

func newWithConfig(cfg aws.Config, bucket, key string, pathStyle bool) *Arbiter {
	// The SDK computes CRC checksums on every upload by default, sent as an
	// aws-chunked trailer that many S3-compatible stores reject. The record
	// is small and its integrity is covered by TLS and the ETag, so ask for
	// checksums only where an operation requires them, unless the operator
	// chose otherwise through AWS_REQUEST_CHECKSUM_CALCULATION.
	if cfg.RequestChecksumCalculation == aws.RequestChecksumCalculationUnset {
		cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	}
	if cfg.ResponseChecksumValidation == aws.ResponseChecksumValidationUnset {
		cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = pathStyle
	})
	endpoint := "AWS"
	if e := client.Options().BaseEndpoint; e != nil {
		endpoint = *e
	}
	return &Arbiter{client: client, bucket: bucket, key: key, endpoint: endpoint, region: cfg.Region}
}

func parseURL(u *url.URL) (bucket, key string, err error) {
	const form = "s3://bucket/key/path"
	switch {
	case u.Scheme != Scheme:
		return "", "", fmt.Errorf("s3 arbiter: scheme %q, want %q", u.Scheme, Scheme)
	case u.User != nil:
		return "", "", fmt.Errorf("s3 arbiter: credentials in the URL are not accepted; use the AWS environment or shared config (URL form %s)", form)
	case u.Opaque != "":
		return "", "", fmt.Errorf("s3 arbiter: URL must be %s", form)
	case u.RawQuery != "" || u.Fragment != "":
		return "", "", fmt.Errorf("s3 arbiter: URL must have no query or fragment; the endpoint and region come from the AWS environment (URL form %s)", form)
	case u.Host == "":
		return "", "", fmt.Errorf("s3 arbiter: no bucket in the URL (URL form %s)", form)
	case u.Port() != "":
		return "", "", fmt.Errorf("s3 arbiter: %q is a host, not a bucket; set the endpoint with AWS_ENDPOINT_URL_S3 (URL form %s)", u.Host, form)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" || strings.HasSuffix(key, "/") {
		return "", "", fmt.Errorf("s3 arbiter: no object key in the URL, or it ends with a slash (URL form %s)", form)
	}
	return u.Host, key, nil
}

// String names the bucket, key, endpoint and region. Credentials never
// appear in it because the URL cannot carry them.
func (a *Arbiter) String() string {
	return fmt.Sprintf("s3://%s/%s (endpoint %s, region %s)", a.bucket, a.key, a.endpoint, a.region)
}

// Read gets the object and returns its ETag as the version.
func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	env, v, err := a.read(ctx)
	return env.Record, v, err
}

func (a *Arbiter) read(ctx context.Context) (writeid.Envelope, arbiter.Version, error) {
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key),
	})
	if err != nil {
		if isNotFound(err) {
			return writeid.Envelope{}, arbiter.Zero, arbiter.ErrNotFound
		}
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("s3 arbiter: get %s: %w", a.key, err)
	}
	defer out.Body.Close()
	if out.ETag == nil || *out.ETag == "" {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("s3 arbiter: get %s: the store returned no ETag", a.key)
	}
	data, err := io.ReadAll(io.LimitReader(out.Body, maxRecordSize+1))
	if err != nil {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("s3 arbiter: get %s: %w", a.key, err)
	}
	if len(data) > maxRecordSize {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("s3 arbiter: get %s: object is larger than %d bytes", a.key, maxRecordSize)
	}
	var env writeid.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return writeid.Envelope{}, arbiter.Zero, fmt.Errorf("s3 arbiter: %s is not a record: %w", a.key, err)
	}
	return env, arbiter.Version(*out.ETag), nil
}

// CompareAndSwap puts the object with If-None-Match: * when expected is
// Zero and with If-Match: expected otherwise.
//
// The SDK retries a request that failed with a 5xx or a transport error.
// If the first attempt was applied and only its response was lost, the
// retry fails its precondition with 412 although the write happened. So on
// any failure the object is read back: if it carries this call's write
// identifier, the write happened and its ETag is returned.
func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	id := writeid.New()
	data, err := json.Marshal(writeid.Envelope{Record: record, WriteID: id})
	if err != nil {
		return arbiter.Zero, fmt.Errorf("s3 arbiter: %w", err)
	}
	in := &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(a.key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}
	if expected == arbiter.Zero {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(string(expected))
	}
	out, err := a.client.PutObject(ctx, in)
	if err == nil {
		if out.ETag == nil || *out.ETag == "" {
			return arbiter.Zero, fmt.Errorf("s3 arbiter: put %s: the store returned no ETag", a.key)
		}
		return arbiter.Version(*out.ETag), nil
	}
	// The read back gets its own deadline: the caller's may be what just
	// expired, and an answer now saves the caller a round of retries.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if env, v, rerr := a.read(rctx); rerr == nil && env.WriteID == id {
		return v, nil
	}
	if isConflict(err, expected) {
		return arbiter.Zero, fmt.Errorf("%w: s3 put %s: %w", arbiter.ErrConflict, a.key, err)
	}
	// Neither ours nor a clean conflict: the request may still be applied
	// later, so the caller must read again before it decides.
	return arbiter.Zero, fmt.Errorf("s3 arbiter: put %s: %w", a.key, err)
}

// Verify runs the probe on the key with the probe suffix.
func (a *Arbiter) Verify(ctx context.Context) error {
	p := *a
	p.key += arbiter.ProbeSuffix
	return arbiter.VerifyProbe(ctx, &p)
}

// isNotFound recognises a missing object. GetObject reports it as
// types.NoSuchKey with code NoSuchKey; some stores answer a bare 404 that
// the SDK turns into the generic code NotFound.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
		return true
	}
	return statusCode(err) == http.StatusNotFound
}

// isConflict recognises a failed precondition. The SDK has no modeled
// error types for it, so the HTTP status is the primary signal and the
// error code the second; codes differ between compatible stores. AWS
// documents, for PutObject with If-Match or If-None-Match, 412
// PreconditionFailed when the condition does not hold and 409
// ConditionalRequestConflict when a concurrent operation interfered; in
// both cases this write did not happen. With If-Match on a missing key the
// answer is 404 NoSuchKey, which for a swap means the record was removed,
// also a conflict. See
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html.
func isConflict(err error, expected arbiter.Version) bool {
	switch statusCode(err) {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	case http.StatusNotFound:
		return expected != arbiter.Zero
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		case "NoSuchKey":
			return expected != arbiter.Zero
		}
	}
	return false
}

func statusCode(err error) int {
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode()
	}
	return 0
}
