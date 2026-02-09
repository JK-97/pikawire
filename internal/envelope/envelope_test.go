package envelope

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMarshalRoundtrip(t *testing.T) {
	e := &Event{
		Phase: PhaseIncremental, Op: OpDelete, DB: "db0", Type: "hash",
		Key: "user:1", Command: "DEL",
		Args:   [][]byte{[]byte("DEL"), []byte("user:1")},
		Source: Source{ID: "127.0.0.1:9221", DB: "db0", Filenum: 7, Offset: 1234, Seq: 99, ExecTimeSec: 1756000000},
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	for _, want := range []string{`"op":"d"`, `"command":"DEL"`, `"args":["DEL","user:1"]`, `"args_encoding":"utf8"`, `"filenum":7`, `"event_id":"inc:127.0.0.1:9221:7:1234:99"`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
}

func TestBase64Fallback(t *testing.T) {
	raw := []byte{0xff, 0xfe, 0x01}
	e := &Event{Phase: PhaseSnapshot, Op: OpRead, DB: "db0", Type: "string", Key: "bin", Command: "SET", Args: [][]byte{[]byte("SET"), raw}}
	b, _ := json.Marshal(e)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["args_encoding"] != "base64" {
		t.Fatalf("want base64, got %v", m["args_encoding"])
	}
	arg0 := m["args"].([]any)[1].(string)
	dec, err := DecodeArg("base64", []byte(arg0))
	if err != nil || string(dec) != string(raw) {
		t.Fatalf("decode mismatch: %v %q", err, dec)
	}
}

func TestKafkaKeyStable(t *testing.T) {
	e := &Event{DB: "db0", Type: "hash", Key: "h:1"}
	if string(e.KafkaKey()) != "db0:h:1" {
		t.Fatal(e.KafkaKey())
	}
	// regression: DEL ("unknown") and a later re-creation ("hash") of the
	// same key MUST co-partition, or per-key ordering is void in Kafka
	del := &Event{DB: "db0", Type: "unknown", Key: "h:1"}
	if string(del.KafkaKey()) != string(e.KafkaKey()) {
		t.Fatalf("DEL routed away from its entity's stream: %s vs %s", del.KafkaKey(), e.KafkaKey())
	}
	// distinct keys must not collide on the db separator
	a := (&Event{DB: "db0", Key: "a:b"}).KafkaKey()
	b := (&Event{DB: "db0", Key: "a\x00:b"}).KafkaKey()
	if string(a) == string(b) {
		t.Fatal("key collision")
	}
}

func TestEventIDStableAcrossRestart(t *testing.T) {
	mk := func(seq uint64) *Event {
		return &Event{Phase: PhaseSnapshot, DB: "db0", Type: "string", Key: "k", Command: "SET", Source: Source{ID: "s", Seq: seq}}
	}
	if mk(1).EventID() != mk(2).EventID() {
		t.Fatal("snapshot event id must not depend on seq")
	}
}

func TestClassify(t *testing.T) {
	if Classify("del") != OpDelete || Classify("HMSET") != OpUpdate || Classify("expire") != OpUpdate {
		t.Fatal("classify wrong")
	}
}
