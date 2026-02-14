package replica

// GateCtl is the snapshot/incremental handoff state machine driving the
// runner.
//
//	buffering : the dump phase. Binlog entries are buffered (never emitted);
//	            once the dump anchor is known, entries at or below it are
//	            dropped (contained in the dump images). The anchor may be set
//	            late — the session can run from before the master's bgsave,
//	            capturing everything while the dump is produced and
//	            transferred, which pins the master's purge window via acks
//	            and removes any dependence on binlog files surviving the
//	            dump fetch.
//	open      : normal incremental mode.
//
// The runner owns the transition: Release drains the buffer keeping the
// flusher the single emitCh producer (per-key binlog order) before opening.
type gateState int

const (
	stateBuffering gateState = iota
	stateOpen
)

// GateCtl wraps the state machine plus the dump anchor.
type GateCtl struct {
	state gateState
	h     Offset // dump anchor (bgsave position); entries <= h are folded in
	hSet  bool   // anchor known (a session may buffer before the dump exists)
}

// NewGateCtl builds a buffering gate. The anchor may be set later via
// RequestOpenAt — until then every entry is buffered, which lets the
// replication session run while the master prepares its bgsave dump.
func NewGateCtl() *GateCtl { return &GateCtl{state: stateBuffering} }

func (g *GateCtl) State() gateState { return g.state }

// ShouldEmitNow reports whether an entry at pos can be emitted immediately
// (open state, beyond the anchor). In buffering state the runner queues.
func (g *GateCtl) ShouldEmitNow(pos Offset) bool {
	return g.state == stateOpen && offsetAfter(pos, g.h)
}

// Drop reports whether an entry is at or below the dump anchor: its effects
// are contained in the dump image and must never be emitted. Before the
// anchor is known nothing can be judged as folded, so Drop reports false and
// every entry stays buffered.
func (g *GateCtl) Drop(pos Offset) bool {
	return g.hSet && !offsetAfter(pos, g.h)
}

// RequestOpenAt sets the dump anchor (valid before Release). Entries buffered
// at or below the anchor are dropped when Release drains the buffer; entries
// beyond it are emitted in binlog order.
func (g *GateCtl) RequestOpenAt(h Offset) {
	g.h = h
	g.hSet = true
}

// Release moves to open (H stays for correctness of ShouldEmitNow: entries
// at exactly h are never emitted). The runner calls it only after the
// pending buffer is fully drained through emitCh, so per-key binlog order
// survives the buffering-to-open transition.
func (g *GateCtl) Release() {
	g.state = stateOpen
}

// H returns the frozen dump anchor.
func (g *GateCtl) H() Offset { return g.h }
