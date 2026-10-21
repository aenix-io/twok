package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	for _, content := range []string{"first\n", "second\n"} {
		if err := Write(path, []byte(content), 0o600); err != nil {
			t.Fatalf("Write(%q) error = %v", content, err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Errorf("content = %q, want %q", got, content)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permissions = %o, want 600", perm)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("directory holds %d entries, want only the file: temporary files must not be left behind", len(entries))
	}
}

func TestWriteMissingDirectory(t *testing.T) {
	if err := Write(filepath.Join(t.TempDir(), "missing", "state.json"), []byte("x"), 0o600); err == nil {
		t.Fatal("Write() into a missing directory succeeded")
	}
}
