package sqlsink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
)

// Config wires the SQL sink.
type Config struct {
	Driver string // postgres | mysql
	// Dialect overrides driver-based SQL rendering (tests / exotic engines).
	Dialect Dialect
	DSN     string
	Rules   []*RuleConfig

	MaxBatchRows int           // flush trigger (default 500)
	FlushEvery   time.Duration // flush trigger (default 200ms)
	Logger       *slog.Logger
}

// PendingRow pairs a mapped row with the completion channel of its Emit.
type PendingRow struct {
	Row  *Row
	Done chan error
}

// Sink turns events into upsert/delete rows and commits them in groups.
//
// Emit blocks until the row's group has committed (checkpoint honesty: the
// runner only advances `delivered` after durable SQL state), while the
// committer batches whatever accumulated into a single transaction.
type Sink struct {
	cfg    Config
	log    *slog.Logger
	db     *sql.DB
	dial   Dialect
	rules  []*RuleConfig
	cancel context.CancelFunc

	mu      sync.Mutex
	queue   []PendingRow
	wake    chan struct{}
	closed  bool
	stmtMu  sync.Mutex
	stmts   map[string]*sql.Stmt
	wg      sync.WaitGroup
	errOnce error
}

// New opens the database handle and starts the committer.
func New(cfg Config) (*Sink, error) {
	for _, r := range cfg.Rules {
		if err := r.Compile(); err != nil {
			return nil, err
		}
	}
	d := cfg.Dialect
	if d == nil {
		var err error
		d, err = NewDialect(cfg.Driver)
		if err != nil {
			return nil, err
		}
	}
	if cfg.MaxBatchRows <= 0 {
		cfg.MaxBatchRows = 500
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 200 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("sqlsink: open: %w", err)
	}
	db.SetMaxOpenConns(2) // one committer + admin reads
	s := &Sink{cfg: cfg, log: cfg.Logger, db: db, dial: d, rules: cfg.Rules, wake: make(chan struct{}, 1), stmts: map[string]*sql.Stmt{}}
	cctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.commitLoop(cctx)
	return s, nil
}

// Emit implements replica.Sink semantics for envelope events: unknown
// entities (no rule matched) are skipped, matching rows block until durable.
func (s *Sink) Emit(ctx context.Context, ev *envelope.Event) error {
	if ev.Phase == envelope.PhaseHeartbeat {
		return nil
	}
	row, err := RowForEvent(s.rules, ev)
	if err != nil {
		if errors.Is(err, ErrNoMatch) {
			return nil
		}
		// A structural mapping failure must not silently pass: surface it
		// so the operator fixes the rule; treat as permanent sink error.
		s.log.Error("sqlsink: mapping error", "err", err, "event", ev.String())
		return err
	}
	pr := PendingRow{Row: row, Done: make(chan error, 1)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("sqlsink: closed")
	}
	full := len(s.queue) >= s.cfg.MaxBatchRows
	s.queue = append(s.queue, pr)
	s.mu.Unlock()
	if full {
		s.signal()
	}
	select {
	case err := <-pr.Done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Sink) commitLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.flushBatch()
		case <-s.wake:
			s.flushBatch()
		}
	}
}

func (s *Sink) flushBatch() {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.mu.Unlock()
		return
	}
	batch := s.queue
	s.queue = nil
	s.mu.Unlock()

	batch = Coalesce(batch)
	err := s.commit(batch)
	for _, p := range batch {
		p.Done <- err
	}
}

// Coalesce collapses repeated mutations of the same row key within one batch,
// keeping the LAST operation (upsert with last-writer-wins columns merged, or
// a delete). This cuts write amplification for hot keys during catch-up.
func Coalesce(batch []PendingRow) []PendingRow {
	index := map[string]int{}
	out := make([]PendingRow, 0, len(batch))
	for _, p := range batch {
		k := p.Row.Table + "\x00" + strings.Join(pkKey(p.Row), "\x00")
		if prev, ok := index[k]; ok {
			old := out[prev].Row
			if p.Row.Del {
				out[prev] = p // delete wins, stays at original position
				continue
			}
			if !old.Del {
				merged := &Row{Table: p.Row.Table, PK: p.Row.PK, Cols: map[string]string{}, Source: p.Row.Source}
				for c, v := range old.Cols {
					merged.Cols[c] = v
				}
				for c, v := range p.Row.Cols {
					merged.Cols[c] = v
				}
				out[prev].Row = merged
				continue
			}
			out[prev] = p // upsert after delete: recreate in place
			continue
		}
		index[k] = len(out)
		out = append(out, p)
	}
	return out
}

func pkKey(r *Row) []string {
	// stable pk ordering independent of map iteration: callers pass rules
	// whose PrimaryKey order we do not retain on the Row; sort by column name.
	names := make([]string, 0, len(r.PK))
	for c := range r.PK {
		names = append(names, c)
	}
	sortStrings(names)
	vals := make([]string, 0, len(names))
	for _, n := range names {
		vals = append(vals, r.PK[n])
	}
	return vals
}

func (s *Sink) commit(batch []PendingRow) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("sqlsink: begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, p := range batch {
		if err = execRow(tx, s.dial, p.Row); err != nil {
			return fmt.Errorf("sqlsink: exec %s: %w", p.Row.Table, err)
		}
	}
	return tx.Commit()
}

func execRow(tx *sql.Tx, d Dialect, r *Row) error {
	pkCols := make([]string, 0, len(r.PK))
	for c := range r.PK {
		pkCols = append(pkCols, c)
	}
	sortStrings(pkCols)
	if r.Del {
		q, _ := d.Delete(r.Table, pkCols)
		_, err := tx.Exec(q, toArgs(pkCols, r.PK)...)
		return err
	}
	cols := r.ColNames()
	q, n := d.Upsert(r.Table, pkCols, cols)
	args := make([]any, 0, n)
	for _, c := range pkCols {
		args = append(args, r.PK[c])
	}
	for _, c := range cols {
		args = append(args, r.Cols[c])
	}
	_, err := tx.Exec(q, args...)
	return err
}

func toArgs(order []string, m map[string]string) []any {
	out := make([]any, len(order))
	for i, c := range order {
		out[i] = m[c]
	}
	return out
}

// Close drains pending rows and releases statements and the pool.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.flushBatch()
	s.cancel()
	s.wg.Wait()
	s.stmtMu.Lock()
	for _, st := range s.stmts {
		_ = st.Close()
	}
	s.stmtMu.Unlock()
	return s.db.Close()
}
