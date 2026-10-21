package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMissing(t *testing.T) {
	s, exists, err := Read(filepath.Join(t.TempDir(), "twok-state.json"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if exists {
		t.Error("Read() exists = true for a missing file")
	}
	if want := (State{Generation: 0, Role: RoleMember, SchemaVersion: SchemaVersion}); s != want {
		t.Errorf("Read() = %+v, want %+v", s, want)
	}
}

func TestWriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "twok-state.json")
	for _, want := range []State{
		{Generation: 3, Role: RoleMember},
		{Generation: 4, Role: RoleJoining},
		{Generation: 4, Role: RoleMember},
	} {
		if err := Write(path, want); err != nil {
			t.Fatalf("Write(%+v) error = %v", want, err)
		}
		got, exists, err := Read(path)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		want.SchemaVersion = SchemaVersion
		if !exists || got != want {
			t.Errorf("Read() = %+v, %v, want %+v, true", got, exists, want)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"generation":4,"role":"member","schemaVersion":1}`+"\n"; got != want {
		t.Errorf("file content = %q, want %q", got, want)
	}
}

func TestWriteRejectsInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "twok-state.json")
	for _, s := range []State{{Generation: 1}, {Generation: 1, Role: "leader"}} {
		if err := Write(path, s); err == nil {
			t.Errorf("Write(%+v) succeeded, want an error", s)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("an invalid state was written: Stat() error = %v", err)
	}
}

func TestReadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "empty file", content: "", wantErr: "EOF"},
		{name: "truncated", content: `{"generation": 3, "ro`, wantErr: "unexpected EOF"},
		{name: "not JSON", content: "generation=3", wantErr: "invalid character"},
		{name: "unknown role", content: `{"generation": 3, "role": "leader", "schemaVersion": 1}`, wantErr: `unknown role "leader"`},
		{name: "missing role", content: `{"generation": 3, "schemaVersion": 1}`, wantErr: "role is missing"},
		{name: "missing schema version", content: `{"generation": 3, "role": "member"}`, wantErr: "schemaVersion 0"},
		{name: "newer schema version", content: `{"generation": 3, "role": "member", "schemaVersion": 2}`, wantErr: "newer"},
		{name: "negative generation", content: `{"generation": -1, "role": "member", "schemaVersion": 1}`, wantErr: "cannot unmarshal"},
		{name: "two objects", content: `{"generation": 3, "role": "member", "schemaVersion": 1}{}`, wantErr: "after the JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "twok-state.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			s, exists, err := Read(path)
			if err == nil {
				t.Fatalf("Read() = %+v, want an error", s)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Read() error = %q, want it to contain %q", err, tt.wantErr)
			}
			// A corrupted file must never look like a fresh member.
			if !exists {
				t.Error("Read() exists = false for a file that exists")
			}
		})
	}
}

func TestReadIgnoresUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "twok-state.json")
	if err := os.WriteFile(path, []byte(`{"generation": 5, "role": "joining", "schemaVersion": 1, "note": "added later"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, _, err := Read(path)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if s.Generation != 5 || s.Role != RoleJoining {
		t.Errorf("Read() = %+v, want generation 5, joining", s)
	}
}
