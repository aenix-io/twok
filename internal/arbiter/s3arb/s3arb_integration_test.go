//go:build s3

package s3arb

import (
	"os"
	"strings"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/arbitertest"
	"github.com/aenix-io/twok/internal/arbiter/writeid"
)

// TestRealStore runs the conformance suite against a real store:
//
//	TWOK_TEST_S3_URL=s3://bucket/prefix AWS_REGION=... go test -tags s3 ./internal/arbiter/s3arb/
//
// with credentials and, for a store that is not AWS, AWS_ENDPOINT_URL_S3
// and TWOK_S3_FORCE_PATH_STYLE in the environment. Every subtest writes a
// record and its probe under a fresh key below the prefix and leaves them
// there, because twok never deletes from the arbiter; clean the prefix up
// by hand.
func TestRealStore(t *testing.T) {
	prefix := os.Getenv("TWOK_TEST_S3_URL")
	if prefix == "" {
		t.Skip("TWOK_TEST_S3_URL is not set")
	}
	run := writeid.New()
	arbitertest.Run(t, func(t *testing.T) arbiter.Arbiter {
		a, err := arbiter.Open(strings.TrimSuffix(prefix, "/") + "/twok-test-" + run + "/" + writeid.New())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("testing against %s", a)
		return a
	})
}
