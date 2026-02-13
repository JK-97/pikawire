package sink

import (
	"errors"
	"sync"
	"testing"

	"github.com/jk-97/pikawire/internal/envelope"
)

func TestRingPrefixOrdering(t *testing.T) {
	r := &seqRing{}
	o0, o1, o2 := r.Next(), r.Next(), r.Next()
	// complete out of order: 1,2 must not advance past 0
	r.Done(o1, envelope.Position{Offset: 2}, nil)
	r.Done(o2, envelope.Position{Offset: 3}, nil)
	pos, err, adv := r.Advance()
	if adv || !pos.IsZero() || err != nil {
		t.Fatalf("premature advance pos=%v err=%v", pos, err)
	}
	r.Done(o0, envelope.Position{Offset: 1}, nil)
	pos, err, adv = r.Advance()
	if !adv || err != nil || pos.Offset != 3 {
		t.Fatalf("prefix failed pos=%v err=%v", pos, err)
	}
}

func TestRingErrorStopsAtGap(t *testing.T) {
	r := &seqRing{}
	a, b := r.Next(), r.Next()
	boom := errors.New("boom")
	r.Done(b, envelope.Position{Offset: 9}, boom)
	if _, err, adv := r.Advance(); adv || err != nil {
		t.Fatal("should not advance at head")
	}
	r.Done(a, envelope.Position{Offset: 5}, nil)
	// a advances; the failed head b is reported alongside the prefix
	pos, err, adv := r.Advance()
	if !adv || pos.Offset != 5 {
		t.Fatalf("expected prefix to 5, got %v err=%v", pos, err)
	}
	if err == nil {
		t.Fatal("expected b error report")
	}
	// idempotent error re-report while failed item remains at head;
	// the prefix already consumed is not re-reported.
	_, err2, _ := r.Advance()
	if err2 == nil {
		t.Fatal("expected persistent error report")
	}
}

func TestRingZeroPosEventsDoNotAdvanceOffset(t *testing.T) {
	r := &seqRing{}
	o := r.Next()
	r.Done(o, envelope.Position{}, nil)
	pos, err, adv := r.Advance()
	if !adv || err != nil || !pos.IsZero() {
		t.Fatalf("zero-pos: adv=%v pos=%v err=%v", adv, pos, err)
	}
}

func TestRingConcurrent(t *testing.T) {
	r := &seqRing{}
	var wg sync.WaitGroup
	const n = 1000
	ords := make([]uint64, n)
	for i := range ords {
		ords[i] = r.Next()
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.Done(ords[i], envelope.Position{Offset: uint64(i + 1)}, nil)
		}(i)
	}
	wg.Wait()
	var last envelope.Position
	var err error
	var adv bool
	for c := 0; c < 4 && !adv; c++ {
		last, err, adv = r.Advance()
	}
	if err != nil || last.Offset != n {
		t.Fatalf("final prefix pos=%v err=%v", last, err)
	}
}
