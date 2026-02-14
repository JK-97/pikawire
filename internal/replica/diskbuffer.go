package replica

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// diskBuffer is a segmented on-disk log of binlog entries buffered while the
// gate is in buffering state (dump phase). Records are
//
//	[4B payload length][payload]
//
// where payload is a compact binary envelope (positions, exec time, raw
// binlog item). Segments rotate at segBytes; a segment is deleted as soon as
// the drain cursor passes it. Default fsync=none: the buffer survives
// process crashes (page cache) but not power loss — consistent with the
// recovery model, where any crash before snapshot completion re-runs the
// whole dump. buffer_fsync=durable adds an fsync every fsyncInterval bytes
// for future crash-replay modes without a full rescan.
//
// Backpressure: appending while buffered bytes are above the high watermark,
// or while the filesystem has less than reserve free, BLOCKS the caller
// (consume) instead of failing the run.
type diskBuffer struct {
	dir      string
	segBytes int64
	high     int64 // buffered-by bytes at which appends block
	low      int64 // appends resume below this
	reserve  int64 // refuse to fall below this much free disk
	durable  bool
	keep     bool // retain drained segments until Close (durable resume needs them)
	adopted  bool // handle started from pre-existing (crash) segments

	mu       sync.Mutex
	segs     []*seg // oldest first; the last is open for writes
	written  int64  // record bytes handed to the OS (incl. 4B length)
	drainedB int64  // record bytes consumed by the reader
	appended int64
	drainedC int64
	rs       int   // reader segment index
	ro       int64 // reader offset within segs[rs]
	fsAccum  int64
	created  int64 // monotonic segment ordinal (names survive deletion)

	closed  bool
	blocked bool  // high watermark engaged (hysteresis until low)
	err     error // sticky I/O failure
}

type seg struct {
	path string
	f    *os.File
	size int64 // bytes written; readers may touch [0,size)
}

const fsyncInterval = 4 << 20

// bufferRecord is one decoded pending entry: gate position (pb-end), event
// identity (item start), exec time, and the raw binlog payload to rebuild
// the event from at drain time.
type bufferRecord struct {
	pbEnd    Offset
	itemPos  Offset
	execTime uint32
	raw      []byte
}

func (r *bufferRecord) payloadLen() int { return 28 + len(r.raw) }

func (r *bufferRecord) encode(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, r.pbEnd.Filenum)
	dst = binary.LittleEndian.AppendUint64(dst, r.pbEnd.Offset)
	dst = binary.LittleEndian.AppendUint32(dst, r.itemPos.Filenum)
	dst = binary.LittleEndian.AppendUint64(dst, r.itemPos.Offset)
	dst = binary.LittleEndian.AppendUint32(dst, r.execTime)
	return append(dst, r.raw...)
}

func decodeRecord(buf []byte) (bufferRecord, error) {
	if len(buf) < 28 {
		return bufferRecord{}, fmt.Errorf("diskBuffer: short record %d", len(buf))
	}
	var r bufferRecord
	r.pbEnd.Filenum = binary.LittleEndian.Uint32(buf[0:4])
	r.pbEnd.Offset = binary.LittleEndian.Uint64(buf[4:12])
	r.itemPos.Filenum = binary.LittleEndian.Uint32(buf[12:16])
	r.itemPos.Offset = binary.LittleEndian.Uint64(buf[16:24])
	r.execTime = binary.LittleEndian.Uint32(buf[24:28])
	r.raw = append([]byte(nil), buf[28:]...)
	return r, nil
}

func newDiskBuffer(dir string, segBytes, high, reserve int64, durable, keep bool) (*diskBuffer, error) {
	if segBytes <= 0 {
		segBytes = 64 << 20
	}
	if high <= 0 {
		high = 8 << 30
	}
	if reserve < 0 {
		reserve = 0
	}
	if !keep {
		if err := os.RemoveAll(dir); err != nil { // stale buffers from a crashed run
			return nil, err
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	b := &diskBuffer{
		dir: dir, segBytes: segBytes,
		high: high, low: high / 2, reserve: reserve, durable: durable, keep: keep,
	}
	if keep {
		if err := b.adopt(); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// adopt rebuilds the segment list from an existing buffer directory
// (durable resume): files are ordered by their creation index; the last one
// is the open segment (appends continue at its EOF).
func (b *diskBuffer) adopt() error {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}
	var found []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "pending-") && strings.HasSuffix(e.Name(), ".log") {
			found = append(found, filepath.Join(b.dir, e.Name()))
		}
	}
	sort.Strings(found)
	for _, path := range found {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		b.segs = append(b.segs, &seg{path: path, f: f, size: st.Size()})
		b.written += st.Size()
		b.created++
		b.adopted = true
	}
	if len(b.segs) == 0 {
		return nil // rotateLocked creates on demand
	}
	// continue appending in the last segment even if it filled up
	if last := b.segs[len(b.segs)-1]; last.size >= b.segBytes {
		return nil // writeLocked rotates first: size>0 && overflow => new file
	}
	return nil
}

// tail scans every fsync'd record and reports the pb-end of the last one
// (zero when the backlog holds no complete record), then rewinds the cursor
// for the real drain. Torn trailing frames are ignored (not yet durable).
func (b *diskBuffer) tail() (Offset, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return Offset{}, 0, b.err
	}
	var last Offset
	var count int64
	for {
		recs, err := b.nextLocked(512)
		if err != nil {
			return last, count, err
		}
		if len(recs) == 0 {
			break
		}
		last = recs[len(recs)-1].pbEnd
		count += int64(len(recs))
	}
	// records from an ADOPTED (crash) backlog never passed through append():
	// credit them once so pending() (appended-drained) reaches exact zero.
	if b.adopted {
		b.appended += count
		b.adopted = false
	}
	b.rs, b.ro, b.drainedB, b.drainedC = 0, 0, 0, 0
	return last, count, nil
}

// appendErr reports a sticky buffer failure (consumed as "run failed").
var errBufferClosed = errors.New("diskBuffer: closed")

func (b *diskBuffer) append(ctx context.Context, rec bufferRecord) error {
	frame := make([]byte, 0, 4+rec.payloadLen())
	frame = binary.LittleEndian.AppendUint32(frame, uint32(rec.payloadLen()))
	frame = rec.encode(frame)

	for {
		b.mu.Lock()
		if b.err != nil {
			err := b.err
			b.mu.Unlock()
			return err
		}
		if b.closed {
			b.mu.Unlock()
			return errBufferClosed
		}
		buffered := b.written - b.drainedB
		if buffered >= b.high || (b.blocked && buffered >= b.low) {
			b.blocked = true // hysteresis: resume only below low mark
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		b.blocked = false
		if !b.fsHasRoomLocked() {
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond): // disk room re-check is pricier
			}
			continue
		}
		err := b.writeLocked(frame)
		b.mu.Unlock()
		return err
	}
}

func (b *diskBuffer) fsHasRoomLocked() bool {
	free, ok := fsFreeBytes(b.dir)
	return !ok || free >= b.reserve
}

func (b *diskBuffer) writeLocked(frame []byte) error {
	if len(b.segs) == 0 || (b.segs[len(b.segs)-1].size > 0 && b.segs[len(b.segs)-1].size+int64(len(frame)) > b.segBytes) {
		if err := b.rotateLocked(); err != nil {
			return err
		}
	}
	s := b.segs[len(b.segs)-1]
	if _, err := s.f.WriteAt(frame, s.size); err != nil {
		b.err = fmt.Errorf("diskBuffer: write: %w", err)
		return b.err
	}
	s.size += int64(len(frame))
	b.written += int64(len(frame))
	b.appended++
	b.fsAccum += int64(len(frame))
	if b.durable && b.fsAccum >= fsyncInterval {
		b.fsAccum = 0
		if err := s.f.Sync(); err != nil {
			b.err = fmt.Errorf("diskBuffer: fsync: %w", err)
			return b.err
		}
	}
	return nil
}

func (b *diskBuffer) rotateLocked() error {
	name := filepath.Join(b.dir, fmt.Sprintf("pending-%08d.log", b.created))
	b.created++
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("diskBuffer: create segment: %w", err)
	}
	b.segs = append(b.segs, &seg{path: name, f: f})
	return nil
}

func (b *diskBuffer) next(n int) ([]bufferRecord, error) {
	if n <= 0 {
		n = 256
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nextLocked(n)
}

// nextLocked pops up to n records from the drain cursor, advancing the
// cursor and (unless retention is on) deleting segments once fully read.
func (b *diskBuffer) nextLocked(n int) ([]bufferRecord, error) {
	if b.err != nil {
		return nil, b.err
	}
	var out []bufferRecord
	hdr := make([]byte, 4)
	for len(out) < n {
		if b.rs >= len(b.segs) {
			break // drained everything currently visible
		}
		s := b.segs[b.rs]
		if b.ro >= s.size {
			// segment fully consumed: unlink and advance (delete-per-drain)
			if b.rs != len(b.segs)-1 || s.size == 0 {
				_ = s.f.Close()
				if !b.keep {
					_ = os.Remove(s.path)
				}
				b.segs = append(b.segs[:0:0], b.segs[1:]...)
				b.rs = 0
				b.ro = 0
				continue
			}
			break // still writing the open segment
		}
		if _, err := s.f.ReadAt(hdr, b.ro); err != nil {
			b.err = fmt.Errorf("diskBuffer: read len: %w", err)
			return out, b.err
		}
		plen := int(binary.LittleEndian.Uint32(hdr))
		if plen <= 0 || int64(plen)+b.ro > s.size {
			b.err = errors.New("diskBuffer: corrupt record length")
			return out, b.err
		}
		payload := make([]byte, plen)
		if _, err := s.f.ReadAt(payload, b.ro+4); err != nil {
			b.err = fmt.Errorf("diskBuffer: read payload: %w", err)
			return out, b.err
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			b.err = err
			return out, err
		}
		out = append(out, rec)
		b.ro += int64(4 + plen)
		b.drainedB += int64(4 + plen)
		b.drainedC++
	}
	return out, nil
}

// pending reports whether records may remain (the reader must also check
// that no append is in flight).
func (b *diskBuffer) pending() (count int64, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appended - b.drainedC, b.written - b.drainedB
}

func (b *diskBuffer) bufferedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written - b.drainedB
}

func (b *diskBuffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, s := range b.segs {
		_ = s.f.Close()
	}
	b.segs = nil
	if !b.keep {
		_ = os.RemoveAll(b.dir) // non-retained buffers die with the handle
	}
}

// discard closes the buffer AND removes its directory unconditionally. Only
// called once the backlog is fully applied (Release) or explicitly wiped.
func (b *diskBuffer) discard() {
	b.close()
	_ = os.RemoveAll(b.dir)
}
