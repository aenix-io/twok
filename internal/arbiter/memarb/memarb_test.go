package memarb

import (
	"context"
	"errors"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/arbitertest"
)

func TestConformance(t *testing.T) {
	arbitertest.Run(t, func(t *testing.T) arbiter.Arbiter {
		// Subtest names are unique, so every subtest gets its own register.
		return New(t.Name())
	})
}

func TestOpenSharesTheRegister(t *testing.T) {
	ctx := context.Background()
	a, err := arbiter.Open("memory://shared/" + t.Name())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	b, err := arbiter.Open("memory://shared/" + t.Name())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	v, err := a.CompareAndSwap(ctx, arbiter.Zero, arbiter.Record{Generation: 1, Survivor: "a"})
	if err != nil {
		t.Fatalf("CompareAndSwap() error = %v", err)
	}
	got, gotV, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("Read() through the second arbiter error = %v", err)
	}
	if gotV != v || got.Survivor != "a" {
		t.Errorf("Read() = %+v, %q, want survivor a at %q", got, gotV, v)
	}
	if _, err := b.CompareAndSwap(ctx, arbiter.Zero, arbiter.Record{Generation: 1, Survivor: "b"}); !errors.Is(err, arbiter.ErrConflict) {
		t.Errorf("CompareAndSwap(Zero) through the second arbiter error = %v, want ErrConflict", err)
	}
	if got, want := a.String(), "memory://shared/"+t.Name(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestOpenWithoutName(t *testing.T) {
	if _, err := arbiter.Open("memory://"); err == nil {
		t.Fatal("Open(memory://) succeeded, want an error")
	}
}

func TestCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := New(t.Name())
	if _, _, err := a.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Read() error = %v, want context.Canceled", err)
	}
	if _, err := a.CompareAndSwap(ctx, arbiter.Zero, arbiter.Record{}); !errors.Is(err, context.Canceled) {
		t.Errorf("CompareAndSwap() error = %v, want context.Canceled", err)
	}
}
