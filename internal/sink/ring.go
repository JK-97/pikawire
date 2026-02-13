package sink

import (
	"sync"

	"github.com/jk-97/pikawire/internal/envelope"
)

// seqRing tracks contiguous completion of globally-ordered emissions so
// `delivered` only advances over a durable prefix (crash never skips an
// un-acked event). Emitters claim ordinals with Next(), mark results with
// Done(), and the consumer reads the contiguous prefix via Advance().
type seqRing struct {
	mu       sync.Mutex
	next     uint64
	base     uint64 // first ordinal still in the window
	consumed uint64
	items    map[uint64]ringItem // ordinal -> result
	closed   bool
}

type ringItem struct {
	pos  envelope.Position // delivered position to record on success (zero = don't advance)
	err  error
	done bool
}

// AdvanceConsumed returns how many ordinals have been consumed from the
// prefix (test/monitoring helper).
func (r *seqRing) AdvanceConsumed() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.consumed
}

func (r *seqRing) Next() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.next
	r.next++
	return n
}

// Done records the outcome of ordinal n. Safe after Close.
func (r *seqRing) Done(n uint64, pos envelope.Position, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.items == nil {
		r.items = map[uint64]ringItem{}
	}
	r.items[n] = ringItem{pos: pos, err: err, done: true}
}

// Advance returns the largest contiguous completed prefix (strictly greater
// than every previously returned position — callers may feed it straight to
// a monotonic checkpoint store) and whether any error was observed in the
// consumed prefix. Callers should stop emitting after an error report.
func (r *seqRing) Advance() (envelope.Position, error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var advanced bool
	var lastPos envelope.Position
	var firstErr error
	for {
		it, ok := r.items[r.base]
		if !ok {
			break
		}
		if it.err != nil {
			firstErr = it.err
			break // leave the failed item for idempotent re-report
		}
		delete(r.items, r.base)
		r.base++
		r.consumed++
		advanced = true
		if !it.pos.IsZero() {
			lastPos = it.pos
		}
	}
	if lastPos.IsZero() {
		return envelope.Position{}, firstErr, advanced
	}
	return lastPos, firstErr, advanced
}

func (r *seqRing) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}
