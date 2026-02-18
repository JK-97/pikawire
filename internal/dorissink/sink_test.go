package dorissink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/sqlsink"
)

type captured struct {
	path, label, cols, partial, format, strip string
	seqCol                                    string
	auth                                      string
	body                                      string
}

func fakeFE(t *testing.T, status string, rec *sync.Map) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.Store(r.Header.Get("Label"), captured{
			path: r.URL.Path, label: r.Header.Get("Label"), cols: r.Header.Get("columns"),
			partial: r.Header.Get("partial_columns"), format: r.Header.Get("format"),
			strip:  r.Header.Get("strip_outer_array"),
			seqCol: r.Header.Get("function_column.sequence_col"),
			auth:   r.Header.Get("Authorization"),
			body:   string(body),
		})
		w.Write([]byte(`{"Status":"` + status + `","Message":"ok","Label":"` + r.Header.Get("Label") + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newSink(t *testing.T, url string) *Sink {
	host := strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://")
	rules := []*sqlsink.RuleConfig{{DataTypes: []string{"hash"}, KeyGlob: "user:{uid}",
		Table: "db1.ods_user", PrimaryKey: []string{"uid"}, FieldColumns: map[string]string{"name": "name"}}}
	s, err := New(Config{Fenodes: []string{host}, User: "root", Rules: rules,
		FlushEvery: 40 * time.Millisecond, MaxBatchRows: 500})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func capturedCount(rec *sync.Map) int {
	n := 0
	rec.Range(func(_, _ any) bool { n++; return true })
	return n
}

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for async sink")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStreamLoadShape(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Success", &rec)
	s := newSink(t, srv.URL)
	ev := &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: "user:7", Command: "HSET",
		Args:   [][]byte{[]byte("HSET"), []byte("user:7"), []byte("name"), []byte("tom")},
		Source: envelope.Source{Offset: 5}}
	if err := s.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return capturedCount(&rec) >= 1 })
	var got captured
	rec.Range(func(_, v any) bool { got = v.(captured); return false })
	if !strings.HasPrefix(got.path, "/api/db1/ods_user/_stream_load") {
		t.Fatal(got.path)
	}
	if got.format != "json" || got.partial != "true" {
		t.Fatalf("headers: %+v", got)
	}
	// columns = sorted union incl. pk
	want := map[string]bool{"uid": true, "name": true}
	for _, c := range strings.Split(got.cols, ",") {
		delete(want, c)
	}
	if len(want) != 0 {
		t.Fatalf("cols=%s", got.cols)
	}
	// body is an outer JSON array (doris strip_outer_array=true)
	var objs []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(got.body)), &objs); err != nil || len(objs) != 1 {
		t.Fatalf("body not outer-array json: %v / %s", err, got.body)
	}
	obj := objs[0]
	if obj["uid"] != "7" || obj["name"] != "tom" {
		t.Fatalf("payload %+v", obj)
	}
	if !strings.HasPrefix(got.label, "pikawire-db1-ods_user-") {
		t.Fatal(got.label)
	}
}

func TestMultiRowUsesOuterArray(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Success", &rec)
	s := newSink(t, srv.URL)
	for i, name := range []string{"a", "b", "c"} {
		ev := &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: "user:" + name, Command: "HSET",
			Args:   [][]byte{[]byte("HSET"), []byte("user:" + name), []byte("name"), []byte(name)},
			Source: envelope.Source{Offset: uint64(i)}}
		if err := s.Emit(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		var strip string
		rec.Range(func(_, v any) bool {
			c := v.(captured)
			var objs []map[string]any
			if err := json.Unmarshal([]byte(c.body), &objs); err != nil {
				t.Fatalf("body not outer-array: %v / %s", err, c.body)
			}
			total += len(objs)
			strip = c.strip
			return true
		})
		if total >= 3 {
			if strip != "true" {
				t.Fatalf("strip=%s", strip)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("rows never flushed")
}

func TestDeleteSign(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Success", &rec)
	s := newSink(t, srv.URL)
	ev := &envelope.Event{Phase: envelope.PhaseIncremental, Type: "unknown", Key: "user:7", Command: "DEL",
		Op: envelope.OpDelete, Args: [][]byte{[]byte("DEL"), []byte("user:7")}}
	if err := s.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return capturedCount(&rec) >= 1 })
	var got captured
	rec.Range(func(_, v any) bool { got = v.(captured); return false })
	if !strings.Contains(got.cols, "__DORIS_DELETE_SIGN__") {
		t.Fatal(got.cols)
	}
	var objs []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(got.body)), &objs); err != nil || len(objs) != 1 {
		t.Fatalf("body not outer-array json: %v / %s", err, got.body)
	}
	obj := objs[0]
	if v, ok := obj["__DORIS_DELETE_SIGN__"]; !ok || v.(float64) != 1 {
		t.Fatalf("delete sign missing: %v", obj)
	}
}

func TestLabelAlreadyExistsIsSuccess(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Label Already Exists", &rec)
	s := newSink(t, srv.URL)
	err := s.Emit(context.Background(), &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash",
		Key: "user:1", Args: [][]byte{[]byte("HSET"), []byte("user:1"), []byte("name"), []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailureSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"Status":"Fail","Message":"too many filtered rows"}`))
	}))
	s := newSink(t, srv.URL)
	if err := s.Emit(context.Background(), &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash",
		Key: "user:1", Args: [][]byte{[]byte("HSET"), []byte("user:1"), []byte("name"), []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.Err() != nil })
	if !strings.Contains(s.Err().Error(), "too many filtered") {
		t.Fatal(s.Err())
	}
}

func TestAsyncCheckpointPrefix(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Success", &rec)
	s := newSink(t, srv.URL)
	var mu sync.Mutex
	var seen []envelope.Position
	s.SetDeliveryNotifier(func(pos envelope.Position) error {
		mu.Lock()
		seen = append(seen, pos)
		mu.Unlock()
		return nil
	})
	mk := func(k string, fn uint32, off uint64) *envelope.Event {
		return &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: k, Command: "HSET",
			Args:   [][]byte{[]byte("HSET"), []byte(k), []byte("name"), []byte("v")},
			Source: envelope.Source{Filenum: fn, Offset: off}}
	}
	for i, k := range []string{"user:a", "user:b", "user:c"} {
		if err := s.Emit(context.Background(), mk(k, 2, uint64(10*(i+1)))); err != nil {
			t.Fatal(err)
		}
	}
	// snapshot event with zero pos must not advance anything alone
	if err := s.Emit(context.Background(), mk("user:d", 0, 0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 1
	})
	mu.Lock()
	last := seen[len(seen)-1]
	for _, p := range seen {
		if p.IsZero() {
			t.Fatalf("zero position notified: %v", seen)
		}
	}
	mu.Unlock()
	if last.Filenum != 2 || last.Offset != 30 {
		t.Fatalf("want prefix 2:30, got %v", last)
	}
	// second batch keeps monotonic prefix
	if err := s.Emit(context.Background(), mk("user:e", 2, 40)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 2
	})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamLoadRedirectLoopbackRewrite(t *testing.T) {
	s, err := New(Config{Fenodes: []string{"doris:8030"}, User: "root"})
	if err != nil {
		t.Fatal(err)
	}
	cr := s.hc.CheckRedirect
	via := []*http.Request{{URL: mustURL(t, "http://doris:8030/api/db/tbl/_stream_load")}}
	req, _ := http.NewRequest(http.MethodPut, "http://127.0.0.1:8040/api/db/tbl/_stream_load", nil)
	if err := cr(req, via); err != nil {
		t.Fatal(err)
	}
	if req.URL.Host != "doris:8040" {
		t.Fatalf("want doris:8040, got %s", req.URL.Host)
	}
	// distributed case: non-loopback BE must be left alone
	req2, _ := http.NewRequest(http.MethodPut, "http://10.0.0.9:8040/api/db/tbl/_stream_load", nil)
	if err := cr(req2, via); err != nil {
		t.Fatal(err)
	}
	if req2.URL.Host != "10.0.0.9:8040" {
		t.Fatalf("want 10.0.0.9:8040, got %s", req2.URL.Host)
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestSequenceColumn(t *testing.T) {
	var rec sync.Map
	srv := fakeFE(t, "Success", &rec)
	host := strings.TrimPrefix(strings.TrimPrefix(srv.URL, "http://"), "https://")
	rules := []*sqlsink.RuleConfig{{DataTypes: []string{"hash"}, KeyGlob: "user:{uid}",
		Table: "db1.ods_user", PrimaryKey: []string{"uid"}, FieldColumns: map[string]string{"name": "name"}}}
	s, err := New(Config{Fenodes: []string{host}, User: "root", Rules: rules,
		FlushEvery: 40 * time.Millisecond, MaxBatchRows: 500, SequenceColumn: "seqv"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ev := &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: "user:7", Command: "HSET",
		Args:   [][]byte{[]byte("HSET"), []byte("user:7"), []byte("name"), []byte("tom")},
		Source: envelope.Source{Filenum: 3, Offset: 99}}
	if err := s.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	ev2 := &envelope.Event{Phase: envelope.PhaseIncremental, Op: envelope.OpDelete, Type: "unknown", Key: "user:7", Command: "DEL",
		Args:   [][]byte{[]byte("DEL"), []byte("user:7")},
		Source: envelope.Source{Filenum: 3, Offset: 120}}
	if err := s.Emit(context.Background(), ev2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return capturedCount(&rec) >= 1 })
	// one flush: HSET + DEL of the SAME key coalesce to the final delete row
	rec.Range(func(_, v any) bool {
		c := v.(captured)
		if c.seqCol != "seqv" {
			t.Fatalf("sequence header missing: %+v", c)
		}
		if !strings.Contains(c.cols, "seqv") {
			t.Fatalf("seqv not declared in cols: %s", c.cols)
		}
		want := fmt.Sprint(int64(3)<<40 | int64(120)) // winner position
		if !strings.Contains(c.body, want) {
			t.Fatalf("body lacks coalesced winner seq: %s", c.body)
		}
		return false
	})
}
