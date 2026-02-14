package replica

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestBuf(t *testing.T, segBytes, high, reserve int64, durable, keep bool) *diskBuffer {
	t.Helper()
	b, err := newDiskBuffer(filepath.Join(t.TempDir(), "buf"), segBytes, high, reserve, durable, keep)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.close)
	return b
}

func mkRec(start uint64, raw string) bufferRecord {
	return bufferRecord{
		pbEnd:    Offset{Filenum: 1, Offset: start + 50},
		itemPos:  Offset{Filenum: 1, Offset: start},
		execTime: 1234,
		raw:      []byte(raw),
	}
}

func TestDiskBufferRoundTripOrder(t *testing.T) {
	b := newTestBuf(t, 64<<20, 1<<30, 0, false, false)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := b.append(ctx, mkRec(uint64(i*100), "x")); err != nil {
			t.Fatal(err)
		}
	}
	if n, by := b.pending(); n != 100 || by <= 0 {
		t.Fatalf("pending = %d/%d", n, by)
	}
	var seq []uint64
	for len(seq) < 100 {
		recs, err := b.next(7)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			seq = append(seq, r.itemPos.Offset)
		}
	}
	for i, v := range seq {
		if v != uint64(i*100) {
			t.Fatalf("order broken at %d: %d", i, v)
		}
	}
	if n, _ := b.pending(); n != 0 {
		t.Fatalf("pending after drain = %d", n)
	}
}

func TestDiskBufferRotationDeletesDrained(t *testing.T) {
	b := newTestBuf(t, 64, 1<<30, 0, false, false) // tiny segments force rotation
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if err := b.append(ctx, mkRec(uint64(i), "abcdefghij0123456789stuvwxyzabcdefghij01")); err != nil {
			t.Fatal(err)
		}
	}
	ents, _ := os.ReadDir(b.dir)
	if len(ents) < 3 {
		t.Fatalf("expected multiple segments, got %d", len(ents))
	}
	for i := 0; i < 3; i++ { // drain half -> at least one consumed segment deleted
		if _, err := b.next(1); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	var after []os.DirEntry
	for time.Now().Before(deadline) {
		after, _ = os.ReadDir(b.dir)
		if len(after) < len(ents) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(after) >= len(ents) {
		t.Fatalf("drained segments not deleted: before=%d after=%d", len(ents), len(after))
	}
}

func TestDiskBufferWatermarkBlocksNotFails(t *testing.T) {
	b := newTestBuf(t, 1<<20, 150, 0, false, false) // high watermark: 150 bytes
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// first record trips the high watermark on its own (152 > 150)
	raw := make([]byte, 120)
	if err := b.append(ctx, mkRec(1, string(raw))); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.append(ctx, mkRec(2, "0123456789")) }()
	select {
	case err := <-done:
		t.Fatalf("append returned without draining: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := b.next(1); err != nil { // drain -> frees space -> unblocks
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append still blocked after drain")
	}
}

func TestDiskBufferTailAndAdopt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "buf")
	b, err := newDiskBuffer(dir, 64<<20, 1<<30, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := b.append(ctx, mkRec(uint64(1000+i*100), "payload")); err != nil {
			t.Fatal(err)
		}
	}
	// tail scans fsync'd records and rewinds for the real drain
	last, n, err := b.tail()
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 || last.Offset != 1000+9*100+50 {
		t.Fatalf("tail=%v n=%d", last, n)
	}
	// drained segments must be RETAINED in keep mode
	if _, err := b.next(10); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) == 0 {
		t.Fatal("keep mode must retain segments")
	}
	b.discard() // Release-equivalent: fully applied => dir removed

	// adopt: reopen a fresh buffer over the same (surviving) dir contents
	// note: close() wiped the dir; emulate a crashed-run backlog by writing
	// again then reopening with keep
	b2, err := newDiskBuffer(dir, 64<<20, 1<<30, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := b2.append(ctx, mkRec(uint64(500+i*100), "x")); err != nil {
			t.Fatal(err)
		}
	}
	// simulate a crash: b2 is never closed (close would wipe the dir)
	b3, err := newDiskBuffer(dir, 64<<20, 1<<30, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer b3.close()
	last, n, err = b3.tail()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || last.Offset != 750 { // pbEnd = 700 + 50B frame header (mkRec)
		t.Fatalf("adopted tail=%v n=%d", last, n)
	}
	recs, err := b3.next(10)
	if err != nil || len(recs) != 3 || recs[0].itemPos.Offset != 500 {
		t.Fatalf("adopted drain recs=%d err=%v", len(recs), err)
	}
}

func TestDiskBufferAdoptPendingReachesZero(t *testing.T) {
	// regression: after adopt+tail+append, pending() must reach
	// exactly zero, or the Release gate never flips.
	dir := filepath.Join(t.TempDir(), "buf")
	ctx := context.Background()
	b, err := newDiskBuffer(dir, 64<<20, 1<<30, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if err := b.append(ctx, mkRec(uint64(i*100), "x")); err != nil {
			t.Fatal(err)
		}
	}
	_, n, err := b.tail()
	if err != nil || n != 7 {
		t.Fatalf("tail n=%d err=%v", n, err)
	}
	b.discard() // clean slate, then redo the adopt sequence
	b, err = newDiskBuffer(dir, 64<<20, 1<<30, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if err := b.append(ctx, mkRec(uint64(i*100), "x")); err != nil {
			t.Fatal(err)
		}
	}
	tail, n, err := b.tail()
	if err != nil || n != 7 || tail.Offset == 0 {
		t.Fatalf("tail=%v n=%d err=%v", tail, n, err)
	}
	for i := 7; i < 12; i++ { // live appends after the scan (receive loop)
		if err := b.append(ctx, mkRec(uint64(i*100), "y")); err != nil {
			t.Fatal(err)
		}
	}
	for {
		recs, err := b.next(256)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			break
		}
	}
	if cnt, by := b.pending(); cnt != 0 || by != 0 {
		t.Fatalf("pending after full adopt-drain: count=%d bytes=%d (want 0/0)", cnt, by)
	}
	b.close()
}
func TestDiskBufferDurableFsync(t *testing.T) {
	b := newTestBuf(t, 1<<20, 1<<30, 0, true, false) // durable: fsync every 4MB accum
	ctx := context.Background()
	payload := "x" // each append small; push >4MB to cross the interval
	rec := mkRec(1, payload)
	rec.raw = make([]byte, 64<<10)
	for i := 0; i < 80; i++ { // ~5MB
		if err := b.append(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := b.pending(); n != 80 {
		t.Fatalf("pending=%d", n)
	}
}
