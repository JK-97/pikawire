package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfigSnapshotBgsave(t *testing.T) {
	base := "source: { host: h, port: 9221 }\nsink: { file: { path: ev.jsonl } }\n"

	// default applies pre-validation so it is assertable regardless of
	// whether this test binary carries the cgo reader
	dec := mustDecode(t, base)
	if dec.SnapshotBgsave != "" {
		t.Fatal("raw default must be empty before applyDefaults")
	}
	dec.applyDefaults()
	if dec.SnapshotBgsave != "auto" {
		t.Fatalf("default snapshot_bgsave = %q, want auto", dec.SnapshotBgsave)
	}

	_, err := LoadConfig(writeCfg(t, base+"snapshot_bgsave: bogus\n"))
	if err == nil || !strings.Contains(err.Error(), "auto|force") {
		t.Fatalf("bad snapshot_bgsave accepted or misreported: %v", err)
	}
}

func mustDecode(t *testing.T, body string) *Config {
	t.Helper()
	c, err := parseConfig([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestLoadConfigStrictUnknownKeys locks the strict-YAML contract: renamed
// or nonexistent options must fail loudly, never silently no-op.
func TestLoadConfigStrictUnknownKeys(t *testing.T) {
	cases := []string{
		"scan_batch: 500",
		"pending_watermark: 100",
		"snapshot_engine: scan",
		"correct_workers: 4",
		"types: [string]",
	}
	for _, c := range cases {
		if _, err := LoadConfig(writeCfg(t, "source: { host: h, port: 1 }\nsink: { file: { path: x } }\n"+c+"\n")); err == nil {
			t.Fatalf("retired option %q accepted silently", c)
		}
	}
}
