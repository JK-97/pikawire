package replica

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/pb/innermessage"
	"github.com/jk-97/pikawire/internal/store"
	"google.golang.org/protobuf/proto"
)

// encodeEntry builds one raw binlog payload (type-first layout) with the
// given entry-START position and a RESP-array command body.
func encodeEntry(t *testing.T, filenum uint32, startOff uint64, argv ...string) []byte {
	t.Helper()
	var body []byte
	body = append(body, fmt.Sprintf("*%d\r\n", len(argv))...)
	for _, a := range argv {
		body = append(body, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
	}
	hdr := make([]byte, 34)
	binary.LittleEndian.PutUint16(hdr[0:2], 1)
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(time.Now().Unix()))
	binary.LittleEndian.PutUint32(hdr[18:22], filenum)
	binary.LittleEndian.PutUint64(hdr[22:30], startOff)
	binary.LittleEndian.PutUint32(hdr[30:34], uint32(len(body)))
	return append(hdr, body...)
}

// TestReleaseDropsPreAnchorBuffered covers the dbsync session-first design:
// the replication stream runs (buffering everything) before the dump anchor
// exists — entries captured during bgsave/transfer. When the anchor arrives
// late, Release's drain must drop everything at or below it and emit the
// rest in binlog order.
func TestReleaseDropsPreAnchorBuffered(t *testing.T) {
	st, err := store.Open(t.TempDir()+"/ck.json", "test")
	if err != nil {
		t.Fatal(err)
	}
	sink := &recSink{started: make(chan struct{})}
	r := NewRunner(Config{Store: st, Sink: sink, DBName: "db0", SourceID: "test"})
	g := NewGateCtl()
	r.AttachGate(g)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.emitLoop(ctx, done)

	// Four entries consumed with NO anchor yet (dump still being fetched).
	// item start = pb end - 50; anchor will be set at pb-end 1:250.
	for _, end := range []uint64{100, 150, 200, 250} {
		if err := r.consume(ctx, resFor(encodeEntry(t, 1, end-50, "SET", "k", "v"), 1, end)); err != nil {
			t.Fatal(err)
		}
	}
	if g.State() != stateBuffering {
		t.Fatal("gate must stay buffering before Release")
	}
	if r.buf == nil {
		t.Fatal("buffer not created by consumed entries")
	}
	if n, _ := r.buf.pending(); n != 4 {
		t.Fatalf("all four entries must be buffered pre-anchor, got %d", n)
	}

	r.RequestOpenAt(Offset{Filenum: 1, Offset: 200}) // entries ≤200 are folded into the dump
	if err := r.consume(ctx, resFor(encodeEntry(t, 1, 250, "SET", "k", "post"), 1, 300)); err != nil {
		t.Fatal(err)
	}
	if err := r.Release(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(sink.seen()) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("want 2 emitted (1:250, 1:300 item starts), got %v", sink.seen())
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := sink.seen()
	want := []envelope.Position{{Filenum: 1, Offset: 200}, {Filenum: 1, Offset: 250}}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d = %v, want %v", i, got[i], want[i])
		}
	}
	if g.State() != stateOpen {
		t.Fatalf("gate state = %v, want open", g.State())
	}
	cancel()
	<-done
}

// TestBackpressureBlocksUntilDrained proves the disk backlog applies
// pressure instead of failing the run: with a tiny byte watermark, consume
// BLOCKS while the buffer is over the mark and completes (in order) once
// Release drains it.
func TestBackpressureBlocksUntilDrained(t *testing.T) {
	st, err := store.Open(t.TempDir()+"/ck.json", "test")
	if err != nil {
		t.Fatal(err)
	}
	sink := &recSink{gate: make(chan struct{}), started: make(chan struct{})}
	r := NewRunner(Config{Store: st, Sink: sink, DBName: "db0", SourceID: "test",
		BufferDir: t.TempDir() + "/buf", PendingBytesHigh: 100})
	g := NewGateCtl()
	r.AttachGate(g)
	r.RequestOpenAt(Offset{Filenum: 1, Offset: 100})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.emitLoop(ctx, done)

	// frame per record ~4+28+~190 raw bytes: the first write lands, the
	// second sees the buffer over the 100B watermark and must block (not fail).
	pad := strings.Repeat("v", 130)
	if err := r.consume(ctx, resFor(encodeEntry(t, 1, 150, "RPUSH", "l", pad), 1, 200)); err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	go func() {
		second <- r.consume(ctx, resFor(encodeEntry(t, 1, 250, "RPUSH", "l", pad), 1, 300))
	}()
	select {
	case err := <-second:
		t.Fatalf("backpressured consume returned early: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	relErr := make(chan error, 1)
	go func() { relErr <- r.Release() }()
	<-sink.started
	close(sink.gate) // let the drain proceed; buffer frees -> unblocks

	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consume never resumed after drain freed the buffer")
	}
	if err := <-relErr; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(sink.seen()) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("want 2 events, got %v", sink.seen())
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := sink.seen()
	if got[0] != (envelope.Position{Filenum: 1, Offset: 150}) || got[1] != (envelope.Position{Filenum: 1, Offset: 250}) {
		t.Fatalf("backpressure reordered the stream: %v", got)
	}
	cancel()
	<-done
}

func resFor(payload []byte, filenum uint32, pbEnd uint64) *innermessage.InnerResponse_BinlogSync {
	return &innermessage.InnerResponse_BinlogSync{
		Binlog:       payload,
		BinlogOffset: &innermessage.BinlogOffset{Filenum: proto.Uint32(filenum), Offset: proto.Uint64(pbEnd)},
	}
}

type recSink struct {
	mu      sync.Mutex
	gate    chan struct{}
	orders  []envelope.Position
	started chan struct{}
	once    sync.Once
}

func (s *recSink) Emit(ctx context.Context, ev *envelope.Event) error {
	s.once.Do(func() { close(s.started) })
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders = append(s.orders, envelope.Position{Filenum: ev.Source.Filenum, Offset: ev.Source.Offset})
	return nil
}

func (s *recSink) seen() []envelope.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]envelope.Position(nil), s.orders...)
}

// TestReleaseKeepsBufferingUntilDrained reproduces the ordering defect a
// live experiment (1M-key dump + 35k ops/s write storm) exposed: Release
// flipped the gate to open BEFORE the backlog was flushed, so the receive
// loop emitted live entries around still-buffered backlog events, breaking
// per-key binlog order (observed at scale as mis-ordered keys and diverged
// after replay).
//
// With a backlog larger than the emitCh capacity, the old code opened the
// gate while its flusher was still blocked; the fix must keep it in
// buffering state (live entries keep appending behind the backlog) until
// everything queued has been emitted, in binlog order.
func TestReleaseKeepsBufferingUntilDrained(t *testing.T) {
	st, err := store.Open(t.TempDir()+"/ck.json", "test")
	if err != nil {
		t.Fatal(err)
	}
	sink := &recSink{gate: make(chan struct{}), started: make(chan struct{})}
	r := NewRunner(Config{Store: st, Sink: sink, DBName: "db0", SourceID: "test"})
	g := NewGateCtl()
	r.AttachGate(g)
	anchor := Offset{Filenum: 1, Offset: 100}
	r.RequestOpenAt(anchor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.emitLoop(ctx, done)

	// An entry at or below the dump anchor is folded into the dump and
	// must never be emitted.
	if err := r.consume(ctx, resFor(encodeEntry(t, 1, 60, "SET", "a", "1"), 1, 100)); err != nil {
		t.Fatal(err)
	}

	// Backlog: 400 entries beyond the anchor (emitCh capacity is 256).
	const backlog = 400
	for i := 0; i < backlog; i++ {
		start, end := uint64(1000+i*100), uint64(1050+i*100)
		if err := r.consume(ctx, resFor(encodeEntry(t, 1, start, "RPUSH", "list", fmt.Sprint(i)), 1, end)); err != nil {
			t.Fatal(err)
		}
	}

	relErr := make(chan error, 1)
	go func() { relErr <- r.Release() }()
	<-sink.started                     // sink is blocked inside Emit for the first backlog event
	time.Sleep(100 * time.Millisecond) // let the flusher fill emitCh and block

	// While the flush is stuck the gate must still be BUFFERING...
	if s := g.State(); s != stateBuffering {
		t.Fatalf("gate state during in-flight flush = %v, want buffering (old bug flipped to open before draining)", s)
	}
	// ...so a live entry must go to the buffer, NOT emit around it.
	liveStart, liveEnd := uint64(1000+backlog*100), uint64(1050+backlog*100)
	if err := r.consume(ctx, resFor(encodeEntry(t, 1, liveStart, "RPUSH", "list", "live"), 1, liveEnd)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	r.mu.Lock()
	bufed := r.bufInflight + func() int64 {
		n, _ := r.buf.pending()
		return n
	}()
	r.mu.Unlock()
	if bufed == 0 {
		t.Fatal("live entry was emitted around the unflushed backlog (per-key order violated)")
	}

	close(sink.gate) // let the sink proceed
	if err := <-relErr; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if len(sink.seen()) == backlog+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d events, want %d", len(sink.seen()), backlog+1)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := sink.seen()
	if got[len(got)-1] != (envelope.Position{Filenum: 1, Offset: liveStart}) {
		t.Fatalf("live entry not last: got %v want %v:%d", got[len(got)-1], 1, liveStart)
	}
	for i := 1; i < len(got); i++ {
		if !got[i].After(got[i-1]) {
			t.Fatalf("stream not binlog-ordered at %d: %v then %v", i, got[i-1], got[i])
		}
	}
	if g.State() != stateOpen {
		t.Fatalf("gate state = %v after drain, want open", g.State())
	}
	cancel()
	<-done
}

// TestStartAtHonoredForFileZero pins two reposition-critical behaviors:
// filenum 0 is a valid position on fresh pika masters (3.5.6 counts from 0),
// and an explicit StartAt must beat checkpoint/INFO resolution.
func TestStartAtHonoredForFileZero(t *testing.T) {
	st, err := store.Open(t.TempDir()+"/ck.json", "test")
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(Config{Store: st, Sink: &recSink{}, DBName: "db0", StartAt: Offset{Filenum: 0, Offset: 47300487}})
	got, err := r.startOffset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != (Offset{Filenum: 0, Offset: 47300487}) {
		t.Fatalf("startOffset = %v, want explicit filenum-0 StartAt", got)
	}
}
