// Package config holds the settings both members of a cluster share and
// reads them from a mounted ConfigMap, one file per key.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/aenix-io/twok/internal/engine"
)

// Mode is the agent's mode; see engine.Mode.
type Mode = engine.Mode

// The modes.
const (
	ModeOff     = engine.ModeOff
	ModeDryRun  = engine.ModeDryRun
	ModeEnforce = engine.ModeEnforce
)

// Config is the shared settings.
type Config struct {
	Mode               Mode
	PollInterval       time.Duration
	ClaimAfter         time.Duration
	ClaimDelay         time.Duration
	FollowerExtraDelay time.Duration
}

// Default is the configuration of an empty ConfigMap: twok is off.
func Default() Config {
	p := engine.DefaultParams()
	return Config{
		Mode:               ModeOff,
		PollInterval:       time.Second,
		ClaimAfter:         p.ClaimAfter,
		ClaimDelay:         p.ClaimDelay,
		FollowerExtraDelay: p.FollowerExtraDelay,
	}
}

type setting struct {
	name string
	set  func(c *Config, value string) error
}

// settings are in the order of the configuration reference in
// docs/design.md.
var settings = []setting{
	{"mode", func(c *Config, v string) error {
		switch m := Mode(v); m {
		case ModeOff, ModeDryRun, ModeEnforce:
			c.Mode = m
			return nil
		}
		return errors.New("must be one of off, dry-run, enforce")
	}},
	{"poll-interval", duration(func(c *Config) *time.Duration { return &c.PollInterval }, false)},
	{"claim-after", duration(func(c *Config) *time.Duration { return &c.ClaimAfter }, false)},
	{"claim-delay", duration(func(c *Config) *time.Duration { return &c.ClaimDelay }, true)},
	{"follower-extra-delay", duration(func(c *Config) *time.Duration { return &c.FollowerExtraDelay }, true)},
}

func duration(field func(*Config) *time.Duration, zeroAllowed bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			return err
		case d < 0:
			return errors.New("must not be negative")
		case d == 0 && !zeroAllowed:
			return errors.New("must be positive")
		}
		*field(c) = d
		return nil
	}
}

// Names returns the names of the shared settings, in the order of the
// configuration reference.
func Names() []string {
	names := make([]string, 0, len(settings))
	for _, s := range settings {
		names = append(names, s.name)
	}
	return names
}

// Set applies a setting by name.
func (c *Config) Set(name, value string) error {
	for _, s := range settings {
		if s.name == name {
			if err := s.set(c, value); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			return nil
		}
	}
	return fmt.Errorf("unknown key %q", name)
}

// FromDir reads the settings from a mounted ConfigMap, one file per key.
// Settings without a file keep their defaults, so an empty directory, or
// one that does not exist because the ConfigMap is missing, means twok is
// off. An unknown key or an invalid value is an error: the caller keeps
// the configuration it had.
func FromDir(dir fs.FS) (Config, error) {
	cfg := Default()
	entries, err := fs.ReadDir(dir, ".")
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return Config{}, err
	}
	for _, e := range entries {
		// kubelet keeps the ConfigMap's revisions in "..*" entries and
		// links every key to the current one.
		if strings.HasPrefix(e.Name(), "..") {
			continue
		}
		value, err := fs.ReadFile(dir, e.Name())
		if err != nil {
			return Config{}, err
		}
		if err := cfg.Set(e.Name(), strings.TrimSpace(string(value))); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}
