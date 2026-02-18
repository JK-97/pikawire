package sqlsink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
)

// ---- minimal recording fake driver ----

var (
	recMu     sync.Mutex
	recExecs  []string
	recOpenN  int
	recFailTx bool
)

type fakeDrv struct{}
type fakeConn struct{ inTx bool }
type fakeStmt struct{ q string }
type fakeTx struct{ c *fakeConn }

func (fakeDrv) Open(string) (driver.Conn, error) {
	recMu.Lock()
	recOpenN++
	recMu.Unlock()
	return &fakeConn{}, nil
}
func (c *fakeConn) Prepare(q string) (driver.Stmt, error) { return &fakeStmt{q}, nil }
func (c *fakeConn) Close() error                          { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	if recFailTx {
		return nil, errors.New("fake begin fail")
	}
	c.inTx = true
	return &fakeTx{c}, nil
}
func (t *fakeTx) Commit() error   { t.c.inTx = false; return nil }
func (t *fakeTx) Rollback() error { t.c.inTx = false; return nil }
func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	recMu.Lock()
	recExecs = append(recExecs, s.q)
	recMu.Unlock()
	return driver.RowsAffected(1), nil
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) { return nil, errors.New("n/a") }
func (s *fakeStmt) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	recMu.Lock()
	recExecs = append(recExecs, q)
	recMu.Unlock()
	return driver.RowsAffected(1), nil
}

func init() {
	sql.Register("sqlsinkfake", fakeDrv{})
}

func resetRec() {
	recMu.Lock()
	recExecs = nil
	recMu.Unlock()
}

func evHash(key string, pairs ...string) *envelope.Event {
	argv := [][]byte{[]byte("HSET"), []byte(key)}
	for _, p := range pairs {
		argv = append(argv, []byte(p))
	}
	return &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: key, Command: "HSET", Args: argv,
		Source: envelope.Source{ID: "s", DB: "db0", Filenum: 0, Offset: 100}}
}

func newTestSink(t *testing.T) *Sink {
	rules := []*RuleConfig{{DataTypes: []string{"hash"}, KeyGlob: "user:{uid}", Table: "ods_user",
		PrimaryKey: []string{"uid"}, FieldColumns: map[string]string{"name": "name"}}}
	s, err := New(Config{Driver: "sqlsinkfake", Dialect: pgDialect{}, DSN: "x", Rules: rules,
		MaxBatchRows: 1000, FlushEvery: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestEmitCommitsDurable(t *testing.T) {
	s := newTestSink(t)
	resetRec()
	if err := s.Emit(context.Background(), evHash("user:1", "name", "a")); err != nil {
		t.Fatal(err)
	}
	recMu.Lock()
	n := len(recExecs)
	recMu.Unlock()
	if n == 0 {
		t.Fatal("Emit returned before commit visible")
	}
}

func TestCoalesceUnit(t *testing.T) {
	mk := func(key, val string, del bool) PendingRow {
		r := &Row{Table: "t", PK: map[string]string{"k": key}, Cols: map[string]string{}}
		if val != "" {
			r.Cols["c"] = val
		}
		if del {
			r.Del = true
		}
		return PendingRow{Row: r, Done: make(chan error, 1)}
	}
	// same-pk upserts merge to one, last write wins
	b := Coalesce([]PendingRow{mk("a", "1", false), mk("a", "2", false), mk("b", "x", false)})
	if len(b) != 2 || b[0].Row.Cols["c"] != "2" {
		t.Fatalf("merge failed: %d rows", len(b))
	}
	// delete after upsert collapses to delete
	b = Coalesce([]PendingRow{mk("a", "1", false), mk("a", "", true)})
	if len(b) != 1 || !b[0].Row.Del {
		t.Fatal("delete collapse failed")
	}
	// upsert after delete recreates
	b = Coalesce([]PendingRow{mk("a", "", true), mk("a", "9", false)})
	if len(b) != 1 || b[0].Row.Del || b[0].Row.Cols["c"] != "9" {
		t.Fatal("recreate collapse failed")
	}
	// interleaved keys collapse in place
	b = Coalesce([]PendingRow{mk("a", "1", false), mk("b", "2", false), mk("a", "3", false)})
	if len(b) != 2 || b[0].Row.Cols["c"] != "3" {
		t.Fatal("interleave failed")
	}
}

func TestNoMatchSkipped(t *testing.T) {
	s := newTestSink(t)
	resetRec()
	ev := evHash("other:1", "name", "x")
	ev.Type = "zset"
	if err := s.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	recMu.Lock()
	n := len(recExecs)
	recMu.Unlock()
	if n != 0 {
		t.Fatalf("unmatched event executed SQL: %v", recExecs)
	}
}

func TestHeartbeatSkipped(t *testing.T) {
	s := newTestSink(t)
	resetRec()
	_ = s.Emit(context.Background(), &envelope.Event{Phase: envelope.PhaseHeartbeat})
	time.Sleep(80 * time.Millisecond)
	recMu.Lock()
	defer recMu.Unlock()
	if len(recExecs) != 0 {
		t.Fatal("heartbeat executed SQL")
	}
}
