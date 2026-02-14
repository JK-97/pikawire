package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/binlog"
	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/event"
	"github.com/jk-97/pikawire/internal/info"
	"github.com/jk-97/pikawire/internal/metrics"
	"github.com/jk-97/pikawire/internal/pb/innermessage"
	"github.com/jk-97/pikawire/internal/pbnet"
	"github.com/jk-97/pikawire/internal/resp"
	"github.com/jk-97/pikawire/internal/store"
)

// ErrPurged means the requested position was already purged on the master.
var ErrPurged = errors.New("replica: sync point purged by master")

// ErrStalled means the master stopped feeding a session that is still behind
// its binlog tail (observed over repeated idle probes). The data is intact
// on the master; callers should tear down and reconnect from the checkpoint.
var ErrStalled = errors.New("replica: binlog sync stalled (master ahead while idle)")

// Sink receives events in strict per-run order. Emit must block until the
// event is durably accepted (kafka delivery report), and return an error to
// fail the run. Sinks may additionally implement AsyncSink and/or ErrSink;
// the runner detects them at Run start.
type Sink interface {
	Emit(ctx context.Context, ev *envelope.Event) error
}

// AsyncSink declares that Emit returns before durability: the sink owns
// checkpoint advancement through the notifier (invoked as the contiguous
// acked prefix advances). The runner stops when the notifier returns an
// error or the sink reports a terminal error via ErrSink.
type AsyncSink interface {
	SetDeliveryNotifier(func(envelope.Position) error)
}

// ErrSink exposes a terminal async delivery error.
type ErrSink interface {
	Err() error
}

// Config wires the runner.
type Config struct {
	MasterHost string
	MasterPort int
	Password   string
	DBName     string
	LocalIP    string
	LocalPort  int
	SourceID   string

	Store *store.Store
	Sink  Sink

	// StartAt overrides the checkpoint position when non-zero. It is an
	// authoritative entry-boundary position: establish() must not re-anchor
	// away from it (unlike a tip sampled from INFO, which can race the
	// master's producer status).
	StartAt Offset
	// RescanHint asks the caller (via callback) when the master purged our
	// position; Run returns ErrPurged in that case.
	AckEvery      time.Duration // period for BinlogAck to master
	Heartbeat     time.Duration // idle time before heartbeat event (0 = off)
	DBSyncTimeout time.Duration // read deadline while waiting for master bgsave
	// Snapshot-handoff backlog lives in a segmented on-disk log (see
	// diskBuffer): BufferDir (default: system temp), PendingBytesHigh blocks
	// consume above this many buffered bytes (default 8GiB) and resumes
	// below half of it, ReserveFreeBytes refuses to let the filesystem drop
	// below the mark (default 4GiB), and BufferDurable fsyncs every 4MB
	// instead of relying on page cache alone (default off: a process crash
	// re-runs the whole dump anyway).
	BufferDir        string
	PendingBytesHigh int64
	ReserveFreeBytes int64
	BufferDurable    bool
	Metrics          *Metrics // optional instrumentation
	Logger           *slog.Logger
}

// Metrics groups instrumentation hooks; any field may be nil.
type Metrics struct {
	EntriesReceived  *metrics.Counter
	EventsEmitted    *metrics.Counter
	DeliveredOffset  *metrics.Gauge
	SourceLagSeconds *metrics.Gauge
}

// Runner streams binlog from a PikiwiDB master through the gate state
// machine, delivering ordered events to Sink. Single receiver goroutine +
// single emitter goroutine keep per-key event order exact.
type Runner struct {
	cfg Config
	log *slog.Logger

	runDone chan struct{} // closed when Run returns (any path)

	mu           sync.Mutex
	gate         *GateCtl
	lastPos      Offset
	hasPos       bool
	buf          *diskBuffer // created lazily on first buffered entry
	bufInflight  int64       // appends decided but not yet completed
	emitCh       chan envelopeOrErr
	accepted     uint64 // entries consumed from master
	delivered    Offset
	hasDel       bool
	deliveredCnt uint64
	sinkErr      error
	lastEmitAt   time.Time
	sid          int32
	sinkIsAsync  bool
	sinkPos      envelope.Position // highest notifier-acked binlog position
	ready        chan struct{}     // closed once the first session is established
	readyOnce    sync.Once
	firstUnacked Offset // first entry not yet covered by an ack (zero = none)
}

// NewRunner creates a runner in closed state.
func NewRunner(cfg Config) *Runner {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.AckEvery <= 0 {
		cfg.AckEvery = time.Second
	}
	return &Runner{
		cfg:     cfg,
		log:     cfg.Logger,
		emitCh:  make(chan envelopeOrErr, 256),
		runDone: make(chan struct{}),
		ready:   make(chan struct{}),
	}
}

type envelopeOrErr struct {
	ev     *envelope.Event
	pos    Offset // binlog position to record as delivered after success
	isPing bool
}

// AttachGate connects a gate (created per run).
func (r *Runner) AttachGate(g *GateCtl) { r.mu.Lock(); r.gate = g; r.mu.Unlock() }

// Run executes one replication session. The caller owns retries: with a
// buffering gate a failure means "redo the dump" (idempotent); with an open
// gate the caller may simply re-Run from the checkpoint.
// ErrStopped signals that the replication session ended; callers producing
// via Inject must treat it as "abort".
var ErrStopped = errors.New("replica: runner stopped")

func (r *Runner) Run(ctx context.Context) (err error) {
	defer close(r.runDone)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start, err := r.startOffset(ctx)
	if err != nil {
		return err
	}
	r.log.Info("replica: starting session", "filenum", start.Filenum, "offset", start.Offset)

	// async sinks own checkpoint advancement; wire notifier + ack gating.
	if a, ok := r.cfg.Sink.(AsyncSink); ok {
		r.sinkIsAsync = true
		a.SetDeliveryNotifier(func(pos envelope.Position) error {
			r.mu.Lock()
			if !pos.IsZero() && (pos.Filenum > r.delivered.Filenum ||
				(pos.Filenum == r.delivered.Filenum && pos.Offset > r.delivered.Offset)) {
				r.delivered = pos
				r.hasDel = true
				r.cfg.Store.SetDelivered(pos.Filenum, pos.Offset)
				r.deliveredCnt++
			}
			flushN := r.deliveredCnt
			r.mu.Unlock()
			if flushN%200 == 0 {
				if err := r.cfg.Store.Flush(); err != nil {
					r.log.Warn("replica: checkpoint flush failed", "err", err)
				}
			}
			if e := ctx.Err(); e != nil {
				return e
			}
			return nil
		})
		defer r.cfg.Store.Flush()
	}

	conn, sessionID, start, err := r.establish(ctx, start)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.sid = sessionID
	r.lastEmitAt = time.Now()
	r.mu.Unlock()
	r.readyOnce.Do(func() { close(r.ready) })
	r.log.Info("replica: session established", "session_id", sessionID, "start_filenum", start.Filenum, "start_offset", start.Offset)

	// first ack anchors the master's delivery window
	if err := r.ack(conn, start, start, sessionID, true); err != nil {
		return err
	}
	emitDone := make(chan struct{})
	go r.emitLoop(ctx, emitDone)
	defer func() { <-emitDone }() // emitLoop exits on ctx cancel or channel close... see emitLoop

	tick := time.NewTicker(r.cfg.AckEvery)
	defer tick.Stop()
	var stallTicks, aheadStreak int

	for {
		select {
		case <-ctx.Done():
			conn.Close() // unblock Recv
			<-emitDone
			return ctx.Err()
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(recvTimeout))
		var respMsg innermessage.InnerResponse
		err := conn.Recv(&respMsg)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				r.onIdle()
				if err := r.onTick(conn, start); err != nil {
					return err
				}
				stallTicks++
				if stallTicks >= 10 {
					stallTicks = 0
					ahead, perr := r.masterAhead(ctx)
					if perr != nil {
						aheadStreak = 0
					} else if ahead {
						aheadStreak++
						if aheadStreak >= 2 {
							r.log.Warn("replica: binlog sync stalled; reconnecting",
								"master_behind_by_bytes", ">4k", "after_idle_s", aheadStreak*10)
							return ErrStalled
						}
					} else {
						aheadStreak = 0
					}
				}
				continue
			}
			conn.Close()
			return err
		}
		stallTicks = 0
		aheadStreak = 0
		if serr := r.Err(); serr != nil {
			conn.Close()
			return serr
		}
		if es, ok := r.cfg.Sink.(ErrSink); ok {
			if serr := es.Err(); serr != nil {
				conn.Close()
				return fmt.Errorf("replica: sink failure: %w", serr)
			}
		}
		if respMsg.GetType() != innermessage.Type_kBinlogSync {
			r.log.Debug("replica: non-binlog msg", "type", respMsg.GetType().String(), "code", respMsg.GetCode().String())
			continue
		}
		for _, res := range respMsg.GetBinlogSync() {
			if res.GetSessionId() != sessionID {
				continue
			}
			if err := r.consume(ctx, res); err != nil {
				conn.Close()
				return err
			}
		}
		if err := r.maybeAckRange(conn); err != nil {
			conn.Close()
			return err
		}
	}
}

// consume processes one raw binlog entry according to the gate.
func (r *Runner) consume(ctx context.Context, res *innermessage.InnerResponse_BinlogSync) error {
	if len(res.GetBinlog()) == 0 {
		return nil // master keepalive packet
	}
	item, err := binlog.DecodeTypeFirst(res.GetBinlog())
	if err != nil {
		r.log.Warn("replica: undecodable binlog entry skipped", "err", err)
		return nil
	}
	argv, err := resp.ParseArray(item.Content)
	if err != nil || len(argv) == 0 {
		r.log.Warn("replica: unparseable binlog content skipped", "err", err)
		return nil
	}
	cmd := string(argv[0])
	dt := event.CommandDataType(cmd)
	key := ""
	if len(argv) > 1 {
		key = string(argv[1])
	}
	// Two offset spaces (verified against pika 3.5.6 / 4.0.2):
	//   item header (filenum,offset) = START of the entry  -> event identity
	//   PB-level binlog_offset       = END of the entry    -> sync-window key
	// They are contiguous: next.start == prev.end. Gate/ack/lastPos all use
	// the PB-end space; H (the dbsync bgsave anchor) is a PB-end, and the
	// first incremental event after it has pos (item start) == H exactly.
	pbOff := res.GetBinlogOffset()
	itemPos := Offset{Filenum: item.Filenum, Offset: item.Offset}
	pos := Offset{Filenum: pbOff.GetFilenum(), Offset: pbOff.GetOffset()}
	if pos.IsZero() {
		pos = itemPos // defensive: some builds may omit the PB field
	}

	r.mu.Lock()
	g := r.gate
	r.lastPos = pos
	r.hasPos = true
	r.accepted++
	if r.firstUnacked.IsZero() {
		r.firstUnacked = pos
	}
	if m := r.cfg.Metrics; m != nil {
		if m.EntriesReceived != nil {
			m.EntriesReceived.Add(1)
		}
		if m.SourceLagSeconds != nil && item.ExecTime > 0 {
			lag := time.Now().Unix() - int64(item.ExecTime)
			if lag < 0 {
				lag = 0
			}
			m.SourceLagSeconds.Set(lag)
		}
	}
	var action int // 0 drop (at/below dump anchor), 1 emit now, 2 buffer
	if g != nil {
		switch {
		case g.Drop(pos):
			action = 0
		case g.ShouldEmitNow(pos):
			action = 1
		default: // stateBuffering && beyond anchor
			action = 2
		}
	} else {
		action = 1 // no gate attached: pure incremental mode
	}
	if action == 2 {
		// Claim the slot under mu so Release's "buffer empty" flip can never
		// strand an in-flight append, but perform the (possibly blocking,
		// watermark-driven) disk write WITHOUT holding mu.
		if r.buf == nil {
			if err := r.newBufferLocked(); err != nil {
				r.mu.Unlock()
				return err
			}
		}
		r.bufInflight++
		buf := r.buf
		r.mu.Unlock()
		err := buf.append(ctx, bufferRecord{
			pbEnd: pos, itemPos: itemPos, execTime: item.ExecTime,
			raw: res.GetBinlog(),
		})
		r.mu.Lock()
		r.bufInflight--
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
	if action == 0 {
		return nil
	}
	ev := r.entryEvent(itemPos, uint64(item.ExecTime), cmd, dt, key, argv)
	select {
	case r.emitCh <- envelopeOrErr{ev: ev, pos: pos}:
		return nil
	case <-r.runDone:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// newBufferLocked creates the backlog buffer; caller holds r.mu.
func (r *Runner) newBufferLocked() error {
	dir := r.cfg.BufferDir
	if dir == "" {
		dir = filepath.Join(os.TempDir(), fmt.Sprintf("pikawire-buffer-%d", os.Getpid()))
	}
	high := r.cfg.PendingBytesHigh
	if high <= 0 {
		high = 8 << 30
	}
	reserve := r.cfg.ReserveFreeBytes
	if reserve <= 0 {
		reserve = 4 << 30
	}
	// durable mode also retains drained segments (until Release completes):
	// the whole backlog must stay replayable for crash-resume.
	b, err := newDiskBuffer(dir, 64<<20, high, reserve, r.cfg.BufferDurable, r.cfg.BufferDurable)
	if err != nil {
		return fmt.Errorf("replica: backlog buffer: %w", err)
	}
	r.buf = b
	return nil
}

// PrepareBacklog adopts an existing durable backlog (crash-resume) and
// reports the pb-end of its last fsync'd record (zero when empty). Must be
// called before Run so the replication stream attaches exactly there.
func (r *Runner) PrepareBacklog() (Offset, int64, error) {
	r.mu.Lock()
	if r.buf == nil {
		if err := r.newBufferLocked(); err != nil {
			r.mu.Unlock()
			return Offset{}, 0, err
		}
	}
	b := r.buf
	r.mu.Unlock()
	return b.tail()
}

// BacklogTail reports the tail position after PrepareBacklog without
// rescanning (mainly for tests/diagnostics).

// Shutdown releases runner-held resources of a session that never ran
// (resume probe helper).
func (r *Runner) Shutdown() {
	if r == nil {
		return
	}
	r.mu.Lock()
	b := r.buf
	r.buf = nil
	r.mu.Unlock()
	if b != nil {
		b.close()
	}
}

// WipeBacklog discards a durable backlog dir before a full re-scan.
func WipeBacklog(dir string) error {
	if dir == "" {
		return nil
	}
	return os.RemoveAll(dir)
}

// entryEvent rebuilds an incremental event from decoded entry parts; shared
// by the direct path and the disk-backlog drain.
func (r *Runner) entryEvent(itemPos Offset, execSec uint64, cmd, dt, key string, argv [][]byte) *envelope.Event {
	return &envelope.Event{
		SchemaVersion: envelope.SchemaVersion,
		Phase:         envelope.PhaseIncremental,
		Op:            envelope.Classify(cmd),
		DB:            r.cfg.DBName,
		Type:          dt,
		Key:           key,
		Command:       cmd,
		Args:          argv,
		Source: envelope.Source{
			ID: r.cfg.SourceID, DB: r.cfg.DBName,
			Filenum: itemPos.Filenum, Offset: itemPos.Offset,
			ExecTimeSec: execSec,
		},
	}
}

// onIdle injects a heartbeat when configured and idle long enough (open only).
// masterAhead reports whether the master's binlog tail is ahead of the last
// received position by more than a small residue (pika withholds ~one entry
// from live sessions, so a small gap is steady-state). Probe runs on the
// recv-idle path, so a genuinely stalled session is detected and the caller
// reconnects — data is never lost, only delayed.
func (r *Runner) masterAhead(ctx context.Context) (bool, error) {
	ri, err := info.FetchReplicationInfo(ctx, r.cfg.MasterHost, r.cfg.MasterPort, r.cfg.Password, 5*time.Second)
	if err != nil {
		return false, err
	}
	if !ri.HasBinlogOffset {
		return false, nil
	}
	r.mu.Lock()
	cur := r.lastPos
	has := r.hasPos
	r.mu.Unlock()
	if !has {
		return false, nil
	}
	const stallEpsilon = 4096 // pika keeps ~1 entry unflushed to live sessions
	if ri.Filenum > cur.Filenum {
		return true, nil
	}
	if ri.Filenum == cur.Filenum && ri.Offset > cur.Offset+stallEpsilon {
		return true, nil
	}
	return false, nil
}

func (r *Runner) onIdle() {
	r.mu.Lock()
	beat := r.cfg.Heartbeat > 0 && r.gate != nil && r.gate.State() == stateOpen
	if time.Since(r.lastEmitAt) < r.cfg.Heartbeat {
		beat = false
	}
	pos := r.lastPos
	r.mu.Unlock()
	_ = pos
	if !beat {
		return
	}
	if time.Since(r.lastEmitAt) < r.cfg.Heartbeat {
		return
	}
	ev := &envelope.Event{
		SchemaVersion: envelope.SchemaVersion,
		Phase:         envelope.PhaseHeartbeat,
		Op:            envelope.OpHeartbeat,
		DB:            r.cfg.DBName,
		Type:          "none",
		Source:        envelope.Source{ID: r.cfg.SourceID, DB: r.cfg.DBName, Filenum: pos.Filenum, Offset: pos.Offset},
	}
	select {
	case r.emitCh <- envelopeOrErr{ev: ev, isPing: true}:
	default:
	}
	_ = r.runDone
}

// emitLoop is the single goroutine that hands events to the sink. Per-key
// order equals enqueue order into emitCh, and all sources enqueue on paths
// serialized by the gate state machine.
func (r *Runner) emitLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-r.emitCh:
			if !ok {
				// queue drained on shutdown; flush store below
				_ = r.cfg.Store.Flush()
				return
			}
			if err := r.cfg.Sink.Emit(ctx, item.ev); err != nil {
				r.log.Error("replica: sink emit failed", "err", err, "event", item.ev.String())
				// fail the run on delivery error
				r.markSinkError(err)
				return
			}
			r.markEmitted(item)
		}
	}
}

var errSinkFailed = errors.New("replica: sink emit failed (see logs)")

func (r *Runner) markSinkError(err error) {
	r.mu.Lock()
	if r.sinkErr == nil {
		r.sinkErr = fmt.Errorf("%w: %v", errSinkFailed, err)
	}
	r.mu.Unlock()
}

// DBName exposes the configured logical db name.
func (r *Runner) DBName() string { return r.cfg.DBName }

// DBSyncSetup performs the MetaSync+DBSync handshake for a full dump:
// returns a live connection (rsync may then fetch the dump) and the session
// id. start is advertised as the DBSync position: the master reuses an
// existing checkpoint only while it is within its gap window of start, so a
// caller with a live replication stream must pass the stream start here.
func (r *Runner) DBSyncSetup(ctx context.Context, start Offset) (*pbnet.Conn, int32, error) {
	conn, err := r.connect()
	if err != nil {
		return nil, 0, err
	}
	if err := r.metaSync(conn); err != nil {
		conn.Close()
		return nil, 0, err
	}
	if r.cfg.DBSyncTimeout <= 0 {
		r.cfg.DBSyncTimeout = 600 * time.Second
	}
	sid, err := r.dbSync(conn, start)
	if err != nil {
		conn.Close()
		return nil, 0, err
	}
	return conn, sid, nil
}

// RequestOpenAt sets the dump anchor (dbsync mode: H is the dump's bgsave
// position, not the runner's last position). Entries already buffered are
// kept for Release.
func (r *Runner) RequestOpenAt(h Offset) {
	r.mu.Lock()
	if r.gate != nil {
		r.gate.RequestOpenAt(h)
	}
	r.mu.Unlock()
}

// markEmitted records delivery progress and flushes the checkpoint periodically.
func (r *Runner) markEmitted(item envelopeOrErr) {
	r.mu.Lock()
	r.lastEmitAt = time.Now()
	if !item.pos.IsZero() && !r.sinkIsAsync {
		r.delivered = item.pos
		r.hasDel = true
		r.cfg.Store.SetDelivered(item.pos.Filenum, item.pos.Offset)
	}
	if m := r.cfg.Metrics; m != nil {
		if m.EventsEmitted != nil {
			m.EventsEmitted.Add(1)
		}
		if m.DeliveredOffset != nil && !item.pos.IsZero() {
			m.DeliveredOffset.Set(int64(item.pos.Offset))
		}
	}
	maybe := r.deliveredCnt
	r.deliveredCnt++
	r.mu.Unlock()
	if !item.isPing && maybe%200 == 0 {
		if err := r.cfg.Store.Flush(); err != nil {
			r.log.Warn("replica: checkpoint flush failed", "err", err)
		}
	}
}

// Inject hands one snapshot-phase event (a full image from the dbsync dump)
// to the ordered emit stream. Must be called between RequestOpenAt and
// Release. It never panics: once the session has ended it reports ErrStopped.
func (r *Runner) Inject(ctx context.Context, ev *envelope.Event) error {
	select {
	case r.emitCh <- envelopeOrErr{ev: ev}:
		return nil
	case <-r.runDone:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release drains the buffered backlog and opens the gate. Entries buffered
// before the dump anchor was known — at or below it — are dropped during the
// drain (their effects live in the dump images). While entries remain to be
// flushed the gate stays in buffering state, so concurrently arriving entries
// keep appending to the buffer: the flusher stays the ONE producer pushing
// into emitCh, preserving per-key binlog order. Flipping to open before the
// flush would let the receive loop emit live entries around the backlog and
// corrupt per-key order.
func (r *Runner) Release() error {
	var dropped int
	for {
		r.mu.Lock()
		g := r.gate
		buf := r.buf
		var cnt int64
		if buf != nil {
			cnt, _ = buf.pending()
		}
		if cnt <= 0 && r.bufInflight == 0 { // <= guards baseline skew
			if g != nil {
				g.Release()
			}
			r.mu.Unlock()
			if buf != nil {
				buf.discard() // fully applied: retained segments go away too
				r.mu.Lock()
				r.buf = nil
				r.mu.Unlock()
			}
			if dropped > 0 {
				r.log.Info("replica: release dropped anchored entries", "count", dropped)
			}
			return nil
		}
		r.mu.Unlock()
		if buf == nil {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		recs, err := buf.next(256)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		for _, rec := range recs {
			if g != nil && g.Drop(rec.pbEnd) {
				dropped++
				continue
			}
			item, err := binlog.DecodeTypeFirst(rec.raw)
			if err != nil {
				r.log.Warn("replica: buffered entry undecodable at drain", "err", err)
				continue
			}
			argv, err := resp.ParseArray(item.Content)
			if err != nil || len(argv) == 0 {
				r.log.Warn("replica: buffered entry unparseable at drain", "err", err)
				continue
			}
			cmd := string(argv[0])
			dt := event.CommandDataType(cmd)
			key := ""
			if len(argv) > 1 {
				key = string(argv[1])
			}
			ev := r.entryEvent(rec.itemPos, uint64(rec.execTime), cmd, dt, key, argv)
			select {
			case r.emitCh <- envelopeOrErr{ev: ev, pos: rec.pbEnd}:
			case <-r.runDone:
				return ErrStopped
			case <-time.After(60 * time.Second):
				return errors.New("replica: emit queue stuck on release")
			}
		}
	}
}

// Err reports a sink failure recorded asynchronously by the emit loop.
func (r *Runner) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sinkErr
}

// startOffset resolves the session start: explicit > checkpoint > master tip
// (read from INFO replication; the master rejects synthetic "max" offsets).
func (r *Runner) startOffset(ctx context.Context) (Offset, error) {
	if r.cfg.StartAt != (Offset{}) {
		return r.cfg.StartAt, nil
	}
	st := r.cfg.Store.State()
	if st.HasPos {
		return Offset{Filenum: st.Filenum, Offset: st.Offset}, nil
	}
	ri, err := info.FetchReplicationInfo(ctx, r.cfg.MasterHost, r.cfg.MasterPort, r.cfg.Password, 5*time.Second)
	if err != nil {
		return Offset{}, fmt.Errorf("replica: resolve master tip: %w", err)
	}
	return Offset{Filenum: ri.Filenum, Offset: ri.Offset}, nil
}

// onTick is called on every recv timeout (~1s): it keeps the session alive
// with a re-TrySync (the master's keepalive probe accepts repeated TrySync)
// and flushes a range ack when delivery progressed.
func (r *Runner) onTick(conn *pbnet.Conn, start Offset) error {
	if err := r.pingTrySync(conn, start); err != nil {
		return err
	}
	return r.maybeAckRange(conn)
}

// maybeAckRange acks [firstUnacked, lastReceived]. The master validates both
// ends against its sync window, so ends must be real entry positions and a
// fully-acked range must never be resent. Acking on receive (not delivery)
// keeps the window rotating during long snapshots; crash recovery is still
// safe because a new session re-TrySyncs from the delivered checkpoint.
func (r *Runner) maybeAckRange(conn *pbnet.Conn) error {
	r.mu.Lock()
	from := r.firstUnacked
	to := r.lastPos
	r.mu.Unlock()
	if from.IsZero() {
		// Liveness: the master evicts slaves without recent BinlogSync
		// traffic (SetLastRecvTime happens only in the ack handler). An
		// all-zero range is a ping that the master never feeds to Update.
		return r.ack(conn, Offset{}, Offset{}, r.sid, false)
	}
	if err := r.ack(conn, from, to, r.sid, false); err != nil {
		return err
	}
	r.log.Info("replica: ack sent", "from_filenum", from.Filenum, "from_offset", from.Offset, "to_filenum", to.Filenum, "to_offset", to.Offset, "session", r.sid)
	r.mu.Lock()
	r.firstUnacked = Offset{} // re-armed by the next received entry
	r.mu.Unlock()
	return nil
}

// establish opens the PB session. A request position that equals or exceeds
// the master's producer status (typical when anchoring "from now" via INFO)
// can leave the slave reader parked at the file tail without an activation
// push; the master reports its real producer position in every TrySync
// response, so when there is no checkpoint we re-anchor once on that
// authoritative offset to guarantee a live session.
func (r *Runner) establish(ctx context.Context, start Offset) (*pbnet.Conn, int32, Offset, error) {
	anchorFromCheckpoint := r.cfg.StartAt != Offset{} // explicit caller position
	if !anchorFromCheckpoint {
		if st := r.cfg.Store.State(); st.HasPos {
			anchorFromCheckpoint = true
		}
	}
	for attempt := 0; ; attempt++ {
		conn, err := r.connect()
		if err != nil {
			return nil, 0, start, fmt.Errorf("replica: connect repl port: %w", err)
		}
		if err := r.metaSync(conn); err != nil {
			conn.Close()
			return nil, 0, start, err
		}
		sessionID, code, tip, err := r.trySync(conn, start)
		if err != nil {
			conn.Close()
			return nil, 0, start, err
		}
		switch code {
		case innermessage.InnerResponse_TrySync_kOk, innermessage.InnerResponse_TrySync_kSyncPointLarger:
		case innermessage.InnerResponse_TrySync_kSyncPointBePurged:
			conn.Close()
			return nil, 0, start, ErrPurged
		default:
			conn.Close()
			return nil, 0, start, fmt.Errorf("replica: trysync rejected with code %v", code)
		}
		if attempt == 0 && !anchorFromCheckpoint && !tip.IsZero() &&
			(code == innermessage.InnerResponse_TrySync_kSyncPointLarger || tip != start) {
			r.log.Info("replica: re-anchor at master producer position", "from_filenum", start.Filenum, "from_offset", start.Offset, "to_filenum", tip.Filenum, "to_offset", tip.Offset)
			conn.Close()
			start = tip
			continue
		}
		return conn, sessionID, start, nil
	}
}

// WaitReady blocks until the first replication session is established (so
// snapshot anchors can read a meaningful position) or ctx/run ends.
func (r *Runner) WaitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-r.runDone:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CurrentPos returns the last binlog position consumed from the master.
func (r *Runner) CurrentPos() Offset {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastPos
}
