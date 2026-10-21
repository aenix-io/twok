// Package memarb is an in-process arbiter, for tests.
//
// The URL is memory://<name>. Every arbiter opened with the same name in one
// process shares one register, so two supervisors in one test process can
// race for the same record. Nothing survives the process.
package memarb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Scheme is the URL scheme of the in-process arbiter.
const Scheme = "memory"

func init() {
	arbiter.Register(Scheme, func(u *url.URL) (arbiter.Arbiter, error) {
		name := u.Host + u.Path
		if name == "" {
			return nil, errors.New("memory arbiter: the URL needs a name, memory://<name>")
		}
		return New(name), nil
	})
}

// register is one record and the counter its versions come from. The
// counter never goes back, so a version is never reused, not even by a
// record with the same content.
type register struct {
	mu      sync.Mutex
	record  arbiter.Record
	version uint64
}

var (
	registersMu sync.Mutex
	registers   = make(map[string]*register)
)

func lookup(name string) *register {
	registersMu.Lock()
	defer registersMu.Unlock()
	r, ok := registers[name]
	if !ok {
		r = &register{}
		registers[name] = r
	}
	return r
}

// Arbiter is the register shared by every arbiter with its name.
type Arbiter struct {
	name string
	reg  *register
}

// New returns the arbiter for name, creating its register on first use.
func New(name string) *Arbiter {
	return &Arbiter{name: name, reg: lookup(name)}
}

func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	if err := ctx.Err(); err != nil {
		return arbiter.Record{}, arbiter.Zero, err
	}
	a.reg.mu.Lock()
	defer a.reg.mu.Unlock()
	if a.reg.version == 0 {
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	}
	return a.reg.record, render(a.reg.version), nil
}

func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	if err := ctx.Err(); err != nil {
		return arbiter.Zero, err
	}
	a.reg.mu.Lock()
	defer a.reg.mu.Unlock()
	current := arbiter.Zero
	if a.reg.version > 0 {
		current = render(a.reg.version)
	}
	if expected != current {
		return arbiter.Zero, arbiter.ErrConflict
	}
	a.reg.version++
	a.reg.record = record
	return render(a.reg.version), nil
}

func (a *Arbiter) Verify(ctx context.Context) error {
	return arbiter.VerifyProbe(ctx, New(a.name+arbiter.ProbeSuffix))
}

func (a *Arbiter) String() string {
	return fmt.Sprintf("%s://%s", Scheme, a.name)
}

func render(v uint64) arbiter.Version {
	return arbiter.Version(strconv.FormatUint(v, 10))
}
