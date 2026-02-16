package dbsync

import (
	"context"
	"errors"
	"testing"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/replica"
	"github.com/jk-97/pikawire/internal/resp"
)

type fakeIter struct {
	recs []Record
	i    int
}

func (f *fakeIter) Next() (*Record, bool, error) {
	if f.i >= len(f.recs) {
		return nil, false, nil
	}
	r := &f.recs[f.i]
	f.i++
	return r, true, nil
}
func (f *fakeIter) Close() error { return nil }

func TestEmitConvertsRESP(t *testing.T) {
	it := &fakeIter{recs: []Record{
		{Type: "string", Key: "k1", RawRESP: resp.SerializeArray([][]byte{[]byte("SET"), []byte("k1"), []byte("v1")})},
		{Type: "string", Key: "k1", RawRESP: resp.SerializeArray([][]byte{[]byte("EXPIRE"), []byte("k1"), []byte("60")})},
		{Type: "hash", Key: "h1", RawRESP: resp.SerializeArray([][]byte{[]byte("HMSET"), []byte("h1"), []byte("f"), []byte("v")})},
	}}
	e := NewEngine(EngineConfig{DBName: "db0", SourceID: "s"})
	var got []*envelope.Event
	n, err := e.Emit(context.Background(), it, replica.Offset{Filenum: 3, Offset: 999}, func(ev *envelope.Event) error {
		got = append(got, ev)
		return nil
	})
	if err == nil && n > 0 {
		for i, ev := range got {
			if want := uint64(i + 1); ev.Source.Seq != want {
				t.Errorf("snapshot event %d seq = %d, want %d (distinct per record sharing an anchor)", i, ev.Source.Seq, want)
			}
		}
		if got[0].Source.Filenum == got[1].Source.Filenum && got[0].Source.Offset == got[1].Source.Offset &&
			got[0].EventID() == got[1].EventID() {
			t.Error("SET and EXPIRE on one key must have distinct event_ids")
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || len(got) != 3 {
		t.Fatalf("n=%d got=%d", n, len(got))
	}
	if got[0].Phase != envelope.PhaseSnapshot || got[0].Command != "SET" || got[0].Op != envelope.OpRead {
		t.Fatalf("bad first: %+v", got[0])
	}
	if got[1].Op != envelope.OpUpdate { // EXPIRE is an update-style companion
		t.Fatal("expire op")
	}
	for _, ev := range got {
		if ev.Source.Filenum != 3 || ev.Source.Offset != 999 {
			t.Fatalf("anchor lost: %+v", ev.Source)
		}
	}
}

func TestEmitPropagatesError(t *testing.T) {
	it := &fakeIter{recs: []Record{{Type: "string", Key: "k", RawRESP: []byte("garbage")}}}
	e := NewEngine(EngineConfig{DBName: "db0"})
	_, err := e.Emit(context.Background(), it, replica.Offset{}, func(*envelope.Event) error { return nil })
	if err == nil {
		t.Fatal("want malformed RESP error")
	}
	it2 := &fakeIter{recs: []Record{{Type: "bogus", Key: "k", RawRESP: resp.SerializeArray([][]byte{[]byte("SET"), []byte("k"), []byte("v")})}}}
	_ = it2
}

func TestReaderUnavailableMessage(t *testing.T) {
	if ReaderAvailable() {
		t.Skip("built with cgo reader")
	}
	_, err := OpenIterator("/x", "db0", ReaderResume{})
	if !errors.Is(err, ErrNoReader) {
		t.Fatalf("want ErrNoReader, got %v", err)
	}
}
