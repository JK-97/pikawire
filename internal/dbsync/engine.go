package dbsync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jk-97/pikawire/internal/resp"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/replica"
)

// EngineConfig for the dump→events pass.
type EngineConfig struct {
	DBName     string
	SourceID   string
	IncludeTTL bool
	Logger     *slog.Logger
	NowFunc    func() time.Time
}

// Engine converts a fetched dump into snapshot-phase events. Anchors are the
// exact bgsave position, so downstream ordering rules are unchanged and the
// reconciliation is unnecessary: binlog beyond the anchor starts only after
// the dump pass completes.
type Engine struct {
	cfg EngineConfig
}

func NewEngine(cfg EngineConfig) *Engine {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.NowFunc == nil {
		cfg.NowFunc = time.Now
	}
	return &Engine{cfg: cfg}
}

// Emit streams records from it as snapshot-phase events anchored at pos
// (the exact bgsave position). emit is called from this goroutine only.
func (e *Engine) Emit(ctx context.Context, it Iterator, pos replica.Offset, emit func(*envelope.Event) error) (int64, error) {
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		rec, ok, err := it.Next()
		if err != nil {
			return n, fmt.Errorf("dbsync: read dump: %w", err)
		}
		if !ok {
			return n, nil
		}
		argv, err := resp.ParseArray(rec.RawRESP)
		if err != nil || len(argv) == 0 {
			return n, fmt.Errorf("dbsync: malformed RESP for %q: %v", rec.Key, err)
		}
		cmd := string(argv[0])
		ev := &envelope.Event{
			SchemaVersion: envelope.SchemaVersion,
			Phase:         envelope.PhaseSnapshot,
			Op:            opForCommand(cmd),
			DB:            e.cfg.DBName,
			Type:          rec.Type,
			Key:           rec.Key,
			Command:       cmd,
			Args:          argv,
			Source: envelope.Source{
				ID: e.cfg.SourceID, DB: e.cfg.DBName,
				Filenum: pos.Filenum, Offset: pos.Offset,
				// ordinal within the run: a key can emit several commands at
				// the same anchor (SET + EXPIRE, chunked populates); seq is
				// the per-record tiebreak. Iteration over a dump is
				// deterministic, so the ordinal is replay-stable.
				Seq:         uint64(n) + 1,
				ExecTimeSec: uint64(e.cfg.NowFunc().Unix()),
			},
		}
		if err := emit(ev); err != nil {
			return n, err
		}
		n++
		if n%1000000 == 0 {
			e.cfg.Logger.Info("dbsync: emit progress", "commands", n, "last_type", rec.Type)
		}
	}
}

func opForCommand(cmd string) envelope.Op {
	switch strings.ToUpper(cmd) {
	case "EXPIRE", "PEXPIRE":
		return envelope.OpUpdate
	default:
		return envelope.OpRead
	}
}
