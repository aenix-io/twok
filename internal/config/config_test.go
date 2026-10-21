package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestFromDir(t *testing.T) {
	for _, tc := range []struct {
		name    string
		files   fstest.MapFS
		want    func(*Config)
		wantErr string
	}{
		{name: "empty is off", files: fstest.MapFS{}, want: func(*Config) {}},
		{
			name: "every key",
			files: fstest.MapFS{
				"mode":                 {Data: []byte("enforce\n")},
				"poll-interval":        {Data: []byte("500ms")},
				"claim-after":          {Data: []byte("30s")},
				"claim-delay":          {Data: []byte("0s")},
				"follower-extra-delay": {Data: []byte("10s")},
			},
			want: func(c *Config) {
				c.Mode = ModeEnforce
				c.PollInterval = 500 * time.Millisecond
				c.ClaimAfter = 30 * time.Second
				c.ClaimDelay = 0
				c.FollowerExtraDelay = 10 * time.Second
			},
		},
		{
			name: "kubelet revisions are skipped",
			files: fstest.MapFS{
				"..2026_10_21/mode": {Data: []byte("enforce")},
				"..data":            {Data: []byte("x")},
				"mode":              {Data: []byte("dry-run")},
			},
			want: func(c *Config) { c.Mode = ModeDryRun },
		},
		{name: "unknown mode", files: fstest.MapFS{"mode": {Data: []byte("on")}}, wantErr: "mode: must be one of"},
		{name: "unknown key", files: fstest.MapFS{"claim-afer": {Data: []byte("1s")}}, wantErr: `unknown key "claim-afer"`},
		{name: "zero claim-after", files: fstest.MapFS{"claim-after": {Data: []byte("0s")}}, wantErr: "claim-after: must be positive"},
		{name: "negative delay", files: fstest.MapFS{"claim-delay": {Data: []byte("-1s")}}, wantErr: "claim-delay: must not be negative"},
		{name: "bad duration", files: fstest.MapFS{"poll-interval": {Data: []byte("1")}}, wantErr: "poll-interval: time: missing unit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FromDir(tc.files)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("FromDir() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromDir() error = %v", err)
			}
			want := Default()
			tc.want(&want)
			if got != want {
				t.Errorf("FromDir() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestMissingDirectoryIsOff(t *testing.T) {
	got, err := FromDir(os.DirFS(filepath.Join(t.TempDir(), "absent")))
	if err != nil {
		t.Fatalf("FromDir() error = %v", err)
	}
	if got != Default() || got.Mode != ModeOff {
		t.Errorf("FromDir() = %+v, want the defaults with mode off", got)
	}
}

func TestDefaultsMatchTheDesign(t *testing.T) {
	want := Config{Mode: ModeOff, PollInterval: time.Second, ClaimAfter: 20 * time.Second, ClaimDelay: 3 * time.Second, FollowerExtraDelay: 5 * time.Second}
	if got := Default(); got != want {
		t.Errorf("Default() = %+v, want %+v", got, want)
	}
	if got := strings.Join(Names(), ","); got != "mode,poll-interval,claim-after,claim-delay,follower-extra-delay" {
		t.Errorf("Names() = %s", got)
	}
}
