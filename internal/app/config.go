// Package app wires source, dbsync snapshot engine and sinks into the
// pikawire runtime.
package app

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jk-97/pikawire/internal/dbsync"
	"github.com/jk-97/pikawire/internal/sqlsink"
	"gopkg.in/yaml.v3"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Config is the YAML-loaded runtime configuration.
type Config struct {
	Source SourceConfig `yaml:"source"`
	Sink   SinkConfig   `yaml:"sink"`

	Pipeline *PipelineConfig `yaml:"pipeline"`

	Checkpoint string `yaml:"checkpoint"`
	IncludeTTL bool   `yaml:"include_ttl"`

	AckEvery  Duration `yaml:"ack_every"`
	Heartbeat Duration `yaml:"heartbeat"`

	MetricsAddr string `yaml:"metrics_addr"` // e.g. ":9090"
	DumpRoot    string `yaml:"dump_root"`    // dbsync working directory
	// SnapshotBgsave: "auto" (default) reuses a still-valid master
	// checkpoint (the stream attaches at its anchor before the bulk dump
	// transfer); "force" always demands a fresh master BGSAVE first.
	SnapshotBgsave string `yaml:"snapshot_bgsave"`
	// Snapshot backlog (disk-backed segmented log, see replica.diskBuffer):
	// appends block above pending_bytes_high (default 8GiB), resuming below
	// half of it; reserve_free_bytes keeps headroom on the buffer filesystem
	// (default 4GiB); buffer_fsync none|durable (durable = fsync every 4MB;
	// none matches the crash-means-rescan recovery model).
	BufferDir        string `yaml:"buffer_dir"`
	PendingBytesHigh int64  `yaml:"pending_bytes_high"`
	ReserveFreeBytes int64  `yaml:"reserve_free_bytes"`
	BufferFsync      string `yaml:"buffer_fsync"`
}

// SourceConfig is the PikiwiDB master.
type SourceConfig struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Password  string `yaml:"password"`
	DB        string `yaml:"db"`
	LocalIP   string `yaml:"local_ip"`
	LocalPort int    `yaml:"local_port"`
}

// SinkConfig selects exactly one sink backend.
type SinkConfig struct {
	Kafka *KafkaSinkConfig `yaml:"kafka"`
	File  *FileSinkConfig  `yaml:"file"`
}

// KafkaSinkConfig targets a single topic (mode A raw command stream).
type KafkaSinkConfig struct {
	Brokers           []string `yaml:"brokers"`
	Topic             string   `yaml:"topic"`
	ClientID          string   `yaml:"client_id"`
	IncludeHeartbeats bool     `yaml:"include_heartbeats"`
	WorkersPerPart    int      `yaml:"workers_per_partition"`
	BatchSize         int      `yaml:"batch_size"`
	Compression       string   `yaml:"compression"` // none|gzip|snappy|lz4|zstd
}

// FileSinkConfig appends JSON lines (debug/CI).
type FileSinkConfig struct {
	Path              string `yaml:"path"`
	IncludeHeartbeats bool   `yaml:"include_heartbeats"`
}

// PipelineConfig selects the embedded row pipeline as the sink.
type PipelineConfig struct {
	Driver   string   `yaml:"driver"` // postgres | mysql | doris
	DSN      string   `yaml:"dsn"`    // sql drivers
	Fenodes  []string `yaml:"fenodes"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	// SequenceColumn adds a Doris MoW sequence column (recommended for
	// delete-heavy streams: guards delete->recreate ordering in the target).
	SequenceColumn string               `yaml:"sequence_column"`
	Rules          []sqlsink.RuleConfig `yaml:"rules"`
	MaxBatchRows   int                  `yaml:"max_batch_rows"`
	FlushEvery     Duration             `yaml:"flush_every"`
}

// Duration tolerates "5s"-style YAML strings.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var i int
	if err := n.Decode(&i); err != nil {
		return err
	}
	*d = Duration(time.Duration(i) * time.Millisecond)
	return nil
}

// LoadConfig reads and validates the YAML config file. Unknown keys are
// rejected so stale or misspelled options fail loudly instead of being
// silently ignored.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("app: read config: %w", err)
	}
	cfg, err := parseConfig(b)
	if err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseConfig decodes YAML with unknown-key rejection, no defaults applied.
func parseConfig(b []byte) (*Config, error) {
	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("app: parse config: %w", err)
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Source.DB == "" {
		c.Source.DB = "db0"
	}
	if c.Source.LocalIP == "" {
		c.Source.LocalIP = c.Source.Host
	}
	if c.Checkpoint == "" {
		c.Checkpoint = "pikawire.checkpoint.json"
	}
	if c.AckEvery == 0 {
		c.AckEvery = Duration(time.Second)
	}
	if c.BufferDir == "" {
		c.BufferDir = filepath.Join(filepath.Dir(c.Checkpoint), "pikawire-buffer")
	}
	switch c.BufferFsync {
	case "":
		c.BufferFsync = "none"
	case "none", "durable":
	default:
		// normalized here; validated below
	}
	if c.SnapshotBgsave == "" {
		c.SnapshotBgsave = "auto"
	}
}

func (c *Config) validate() error {
	if c.Source.Host == "" || c.Source.Port == 0 {
		return fmt.Errorf("app: source.host/port required")
	}
	switch c.SnapshotBgsave {
	case "auto", "force":
	default:
		return fmt.Errorf("app: snapshot_bgsave must be auto|force, got %q", c.SnapshotBgsave)
	}
	switch c.BufferFsync {
	case "none", "durable":
	default:
		return fmt.Errorf("app: buffer_fsync must be none|durable, got %q", c.BufferFsync)
	}
	if c.DumpRoot == "" {
		c.DumpRoot = "pikawire-dump"
	}
	if !dbsync.ReaderAvailable() {
		return fmt.Errorf("app: this build has no dbsync engine: build with -tags pikadump (see scripts/build-cgo.sh)")
	}
	nSinks := 0
	if c.Sink.Kafka != nil {
		nSinks++
	}
	if c.Sink.File != nil {
		nSinks++
	}
	if c.Pipeline != nil {
		nSinks++
	}
	if nSinks == 0 {
		return fmt.Errorf("app: one sink required (kafka|file|pipeline)")
	}
	if nSinks > 1 {
		return fmt.Errorf("app: choose exactly one sink (kafka|file|pipeline)")
	}
	if c.Pipeline != nil {
		if c.Pipeline.Driver == "" || len(c.Pipeline.Rules) == 0 {
			return fmt.Errorf("app: pipeline.driver/rules required")
		}
		if c.Pipeline.Driver == "doris" {
			if len(c.Pipeline.Fenodes) == 0 {
				return fmt.Errorf("app: pipeline.doris requires fenodes")
			}
		} else if c.Pipeline.DSN == "" {
			return fmt.Errorf("app: pipeline.dsn required for driver %s", c.Pipeline.Driver)
		}
	}
	if c.Sink.Kafka != nil && (len(c.Sink.Kafka.Brokers) == 0 || c.Sink.Kafka.Topic == "") {
		return fmt.Errorf("app: sink.kafka.brokers/topic required")
	}
	if c.Sink.File != nil && c.Sink.File.Path == "" {
		return fmt.Errorf("app: sink.file.path required")
	}
	return nil
}

// SourceID identifies this source in the envelope and checkpoint.
func (c *Config) SourceID() string {
	return fmt.Sprintf("%s:%d", c.Source.Host, c.Source.Port)
}
