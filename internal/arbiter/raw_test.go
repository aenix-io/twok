package arbiter_test

import (
	"net/url"
	"slices"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
)

func TestOpenRaw(t *testing.T) {
	var got string
	want := &fake{}
	name := scheme("test-raw")
	arbiter.RegisterRaw(name, func(raw string) (arbiter.Arbiter, error) {
		got = raw
		return want, nil
	})
	// A host list net/url rejects reaches the factory untouched.
	raw := name + "://e1:2379,e2:2379/twok/prod"
	a, err := arbiter.Open(raw)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if a != want || got != raw {
		t.Errorf("Open() = %v with %q, want the factory's arbiter with %q", a, got, raw)
	}
	if !slices.Contains(arbiter.Schemes(), name) {
		t.Errorf("Schemes() = %v, want %s in it", arbiter.Schemes(), name)
	}
}

func TestRegisterRawDuplicate(t *testing.T) {
	name := scheme("test-raw-dup")
	arbiter.Register(name, func(*url.URL) (arbiter.Arbiter, error) { return &fake{}, nil })
	defer func() {
		if recover() == nil {
			t.Error("RegisterRaw() of a scheme Register already took did not panic")
		}
	}()
	arbiter.RegisterRaw(name, func(string) (arbiter.Arbiter, error) { return &fake{}, nil })
}
