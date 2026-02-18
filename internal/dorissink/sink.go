// Package dorissink writes mapped Pikawire rows into Apache Doris via
// Stream Load. It reuses the sqlsink mapping rules and the same group-commit
// checkpoint discipline, so pikawire can run PikiwiDB -> Doris directly
// (no Kafka hop required).
package dorissink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/sqlsink"
)

// Config wires the Doris sink.
type Config struct {
	// Fenodes: FE http endpoints, e.g. ["doris-fe:8030"].
	Fenodes  []string
	User     string
	Password string

	Rules        []*sqlsink.RuleConfig
	MaxBatchRows int
	// MaxQueueRows bounds the buffered pre-flush queue (backpressure:
	// Emit blocks when full). Defaults to 4x MaxBatchRows.
	MaxQueueRows int
	FlushEvery   time.Duration
	HTTPTimeout  time.Duration
	// SequenceColumn, when set, is injected into every load row with the
	// event's (filenum, offset) encoded as a monotonic int64, and declared
	// via function_column.sequence_col. This fixes the Doris 2.1 unique-MoW
	// race where a delete-sign batch could swallow the following partial
	// upsert of the same key (row recreate lost without a version guard).
	SequenceColumn string
	Logger         *slog.Logger
}

// entry pairs a mapped row with the source position it came from.
type entry struct {
	pr  sqlsink.PendingRow
	pos envelope.Position
}

// Sink batches mapped rows and commits them per-table with one Stream Load
// per flush group (unique-key + partial_update + __DORIS_DELETE_SIGN__).
//
// It is an async sink (implements replica.AsyncSink): Emit returns as soon
// as a row is queued; the delivery notifier fires with the contiguous
// committed prefix once a whole batch survives its stream loads. A failed
// batch freezes advancement and surfaces through Err(); nothing after the
// failed point is acknowledged, so restart replays from the last committed
// batch boundary.
type Sink struct {
	cfg   Config
	log   *slog.Logger
	hc    *http.Client
	rules []*sqlsink.RuleConfig

	mu       sync.Mutex
	queue    []entry
	room     chan struct{}
	wake     chan struct{}
	notifier func(envelope.Position) error
	termErr  error
	closed   bool
	cctx     context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	labelSeq  atomicUint
	nextTable int // fenode round-robin
}

func New(cfg Config) (*Sink, error) {
	for _, r := range cfg.Rules {
		if err := r.Compile(); err != nil {
			return nil, err
		}
	}
	if len(cfg.Fenodes) == 0 {
		return nil, errors.New("dorissink: fenodes required")
	}
	if cfg.MaxBatchRows <= 0 {
		cfg.MaxBatchRows = 1000
	}
	if cfg.MaxQueueRows <= 0 {
		cfg.MaxQueueRows = 4 * cfg.MaxBatchRows
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 500 * time.Millisecond
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Sink{
		cfg: cfg,
		log: cfg.Logger,
		hc: &http.Client{
			Timeout: cfg.HTTPTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Doris FE answers _stream_load with a 307 to the chosen BE:
				// follow it once, preserving method+body (net/http would drop
				// PUT bodies otherwise on some paths).
				if len(via) > 2 {
					return http.ErrUseLastResponse
				}
				// All-in-one deployments register the BE under its loopback
				// address, producing a 307 target unreachable from outside
				// the container. Rewrite the host to the FE we reached (same
				// host, BE port) — correct for single-host topologies,
				// never triggered by distributed ones (non-loopback BE IPs).
				if len(via) > 0 && isLoopback(req.URL.Hostname()) && !isLoopback(via[0].URL.Hostname()) {
					req.URL.Host = via[0].URL.Hostname() + ":" + req.URL.Port()
				}
				return nil
			},
		},
		rules: cfg.Rules,
		wake:  make(chan struct{}, 1),
		room:  make(chan struct{}, cfg.MaxQueueRows),
	}
	cctx, cancel := context.WithCancel(context.Background())
	s.cctx = cctx
	s.cancel = cancel
	s.wg.Add(1)
	go s.commitLoop(cctx)
	return s, nil
}

// Emit implements replica.Sink.
func (s *Sink) Emit(ctx context.Context, ev *envelope.Event) error {
	if ev.Phase == envelope.PhaseHeartbeat {
		return nil
	}
	row, err := sqlsink.RowForEvent(s.rules, ev)
	if err != nil {
		if errors.Is(err, sqlsink.ErrNoMatch) {
			return nil
		}
		s.log.Error("dorissink: mapping error", "err", err, "event", ev.String())
		return err
	}
	pr := sqlsink.PendingRow{Row: row}
	// Backpressure: block while the queue is full so memory stays bounded
	// when Doris loads slower than the scan produces.
	select {
	case s.room <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	pos := envelope.Position{Filenum: ev.Source.Filenum, Offset: ev.Source.Offset}
	if s.cfg.SequenceColumn != "" {
		// filenum<<40 | offset: monotonic across binlog-file rotation and
		// restart (filenum is globally increasing); offsets stay far below
		// 2^40 (per-file rotation at ~100 MB).
		row := pr.Row
		if row != nil {
			row.Cols[s.cfg.SequenceColumn] = strconv.FormatInt(int64(pos.Filenum)<<40|int64(pos.Offset), 10)
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.room
		return errors.New("dorissink: closed")
	}
	if s.termErr != nil {
		err := s.termErr
		s.mu.Unlock()
		<-s.room
		return err
	}
	full := len(s.queue) >= s.cfg.MaxBatchRows
	s.queue = append(s.queue, entry{pr: pr, pos: pos})
	s.mu.Unlock()
	if full {
		s.signal()
	}
	return nil
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
			s.flush()
		case <-s.wake:
			s.flush()
		}
	}
}

func (s *Sink) flush() {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.mu.Unlock()
		return
	}
	batch := s.queue
	s.queue = nil
	s.mu.Unlock()
	for range batch {
		select {
		case <-s.room:
		default:
		}
	}
	prs := make([]sqlsink.PendingRow, len(batch))
	for i, e := range batch {
		prs[i] = e.pr
	}
	batched := sqlsink.Coalesce(prs)

	// Group rows by (table, exact column set): Doris stream load declares
	// columns per load, and a row missing a declared column is loaded as
	// NULL, which would clobber stored values despite partial_update. One
	// load per distinct shape keeps updates truly partial.
	groups := map[groupKey][]sqlsink.PendingRow{}
	for _, p := range batched {
		gk := groupKey{table: p.Row.Table, shape: rowShape(p.Row)}
		groups[gk] = append(groups[gk], p)
	}
	keys := make([]groupKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].table != keys[j].table {
			return keys[i].table < keys[j].table
		}
		return keys[i].shape < keys[j].shape
	})
	for _, k := range keys {
		if err := s.streamLoad(s.cctx, k.table, groups[k]); err != nil {
			s.mu.Lock()
			if s.termErr == nil {
				s.termErr = err
			}
			s.mu.Unlock()
			s.log.Error("dorissink: batch failed, checkpoint frozen", "table", k.table, "err", err)
			return
		}
	}
	// Whole batch committed: advance the contiguous prefix.
	mx := envelope.Position{}
	for _, e := range batch {
		if e.pos.After(mx) {
			mx = e.pos
		}
	}
	if !mx.IsZero() {
		s.mu.Lock()
		fn := s.notifier
		s.mu.Unlock()
		if fn != nil {
			if err := fn(mx); err != nil {
				s.mu.Lock()
				if s.termErr == nil {
					s.termErr = err
				}
				s.mu.Unlock()
			}
		}
	}
}

type groupKey struct {
	table string
	shape string
}

// rowShape is the canonical column signature of a row (sorted, deletes get
// their own shape incl. the delete sign column).
func rowShape(r *sqlsink.Row) string {
	cols := make([]string, 0, len(r.PK)+len(r.Cols)+1)
	for c := range r.PK {
		cols = append(cols, c)
	}
	for c := range r.Cols {
		cols = append(cols, c)
	}
	if r.Del {
		cols = append(cols, "__DORIS_DELETE_SIGN__")
	}
	sort.Strings(cols)
	return strings.Join(cols, ",")
}

// streamLoad writes one group of rows (single table, identical column shape)
// via _stream_load.
func (s *Sink) streamLoad(ctx context.Context, table string, rows []sqlsink.PendingRow) error {
	db, tbl := splitTable(table)
	fe := s.nextFe()
	label := fmt.Sprintf("pikawire-%s-%s-%d-%d", strings.ReplaceAll(db, ".", "_"), tbl, time.Now().Unix(), s.labelSeq.Add(1))

	// column set: union of present columns across group (doris partial
	// update accepts per-row missing fields by filling defaults per load
	// column list -> use the union + pks).
	present := map[string]bool{}
	pks := map[string]bool{}
	for _, p := range rows {
		for c := range p.Row.PK {
			pks[c] = true
		}
		for c := range p.Row.Cols {
			present[c] = true
		}
	}
	delete(present, "")
	cols := make([]string, 0, len(pks)+len(present)+1)
	for c := range pks {
		cols = append(cols, c)
	}
	for c := range present {
		if !pks[c] {
			cols = append(cols, c)
		}
	}
	hasDelete := false
	for _, p := range rows {
		if p.Row.Del {
			hasDelete = true
			break
		}
	}
	if hasDelete {
		cols = append(cols, "__DORIS_DELETE_SIGN__")
	}
	sort.Strings(cols)

	var body bytes.Buffer
	// Doris 2.1 json format parses the outer-array shape; JSON-lines with
	// strip_outer_array:false silently ingests only the first object.
	body.WriteByte('[')
	enc := json.NewEncoder(&body)
	for i, p := range rows {
		obj := make(map[string]any, len(cols))
		for c, v := range p.Row.PK {
			obj[c] = v
		}
		for c, v := range p.Row.Cols {
			obj[c] = v
		}
		if p.Row.Del {
			obj["__DORIS_DELETE_SIGN__"] = 1
		}
		if i > 0 {
			body.WriteByte(',')
		}
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("dorissink: encode: %w", err)
		}
	}
	body.WriteByte(']')

	url := fmt.Sprintf("http://%s/api/%s/%s/_stream_load", fe, db, tbl)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body.Bytes())), nil
	}
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", "json")
	req.Header.Set("strip_outer_array", "true")
	req.Header.Set("label", label)
	req.Header.Set("columns", strings.Join(cols, ","))
	req.Header.Set("partial_columns", "true") // Doris 2.x MoW partial column update
	if s.cfg.SequenceColumn != "" {
		req.Header.Set("function_column.sequence_col", s.cfg.SequenceColumn)
	}
	req.SetBasicAuth(s.cfg.User, s.cfg.Password)

	resp, err := s.hc.Do(req)
	if err != nil {
		return fmt.Errorf("dorissink: stream load %s: %w", label, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var out struct {
		Status   string `json:"Status"`
		Message  string `json:"Message"`
		Label    string `json:"Label"`
		ErrorURL string `json:"ErrorURL"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("dorissink: bad stream load response (%d): %s", resp.StatusCode, truncate(string(raw), 300))
	}
	switch {
	case out.Status == "Success":
		return nil
	case out.Status == "Label Already Exists":
		// previous attempt committed this exact batch; treat as success
		return nil
	default:
		return fmt.Errorf("dorissink: stream load %s status=%s message=%s errorURL=%s", label, out.Status, out.Message, out.ErrorURL)
	}
}

func (s *Sink) nextFe() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	fe := s.cfg.Fenodes[s.nextTable%len(s.cfg.Fenodes)]
	s.nextTable++
	return fe
}

func splitTable(t string) (db, tbl string) {
	if i := strings.LastIndexByte(t, '.'); i > 0 {
		return t[:i], t[i+1:]
	}
	// without explicit schema, doris uses the connection db (unset) -> error
	return "information_schema", t
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// SetDeliveryNotifier implements replica.AsyncSink.
func (s *Sink) SetDeliveryNotifier(fn func(envelope.Position) error) {
	s.mu.Lock()
	s.notifier = fn
	s.mu.Unlock()
}

// Err implements replica.ErrSink: terminal delivery error (a failed batch
// freezes checkpoint advancement; the runner stops on it).
func (s *Sink) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.termErr
}

// Close drains and stops.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	for {
		s.mu.Lock()
		n := len(s.queue)
		s.mu.Unlock()
		if n == 0 {
			break
		}
		s.flush()
	}
	s.cancel()
	s.wg.Wait()
	return s.Err()
}

func isLoopback(host string) bool {
	return host == "127.0.0.1" || host == "localhost" || host == "::1" || strings.HasPrefix(host, "127.")
}
