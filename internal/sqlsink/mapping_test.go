package sqlsink

import (
	"testing"

	"github.com/jk-97/pikawire/internal/envelope"
)

func ruleHash(t *testing.T) *RuleConfig {
	r := &RuleConfig{
		DataTypes:    []string{"hash"},
		KeyGlob:      "user:{uid}",
		Table:        "public.ods_user",
		PrimaryKey:   []string{"uid"},
		FieldColumns: map[string]string{"name": "name", "age": "age"},
	}
	if err := r.Compile(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHashMapping(t *testing.T) {
	ev := &envelope.Event{Phase: envelope.PhaseIncremental, Type: "hash", Key: "user:42",
		Args: [][]byte{[]byte("HSET"), []byte("user:42"), []byte("name"), []byte("tom")}}
	row, err := RowForEvent([]*RuleConfig{ruleHash(t)}, ev)
	if err != nil {
		t.Fatal(err)
	}
	if row.PK["uid"] != "42" || row.Cols["name"] != "tom" || row.Del {
		t.Fatalf("bad row %+v", row)
	}
	// unmatched key/type passes through
	if _, err := RowForEvent([]*RuleConfig{ruleHash(t)}, &envelope.Event{Type: "string", Key: "k"}); err != ErrNoMatch {
		t.Fatalf("want ErrNoMatch got %v", err)
	}
}

func TestMultiCaptureKey(t *testing.T) {
	r := &RuleConfig{DataTypes: []string{"hash"}, KeyGlob: "org:{org}:user:{uid}", Table: "t",
		PrimaryKey: []string{"org", "uid"}}
	if err := r.Compile(); err != nil {
		t.Fatal(err)
	}
	row, err := RowForEvent([]*RuleConfig{r}, &envelope.Event{Type: "hash", Key: "org:acme:user:7",
		Args: [][]byte{[]byte("HMSET"), []byte("k"), []byte("f"), []byte("v")}})
	if err != nil {
		t.Fatal(err)
	}
	if row.PK["org"] != "acme" || row.PK["uid"] != "7" {
		t.Fatalf("captures %+v", row.PK)
	}
}

func TestAdjacentCaptureRejected(t *testing.T) {
	r := &RuleConfig{Table: "t", PrimaryKey: []string{"a"}, KeyGlob: "x:{a}{b}"}
	if err := r.Compile(); err == nil {
		t.Fatal("want adjacent-capture compile error")
	}
}

func TestStringJSONValue(t *testing.T) {
	r := &RuleConfig{DataTypes: []string{"string"}, KeyGlob: "acct:{id}", Table: "t",
		PrimaryKey: []string{"id"}, ValueColumn: "raw", ValueJSON: true,
		JSONColumns: []string{"balance", "name"}}
	if err := r.Compile(); err != nil {
		t.Fatal(err)
	}
	ev := &envelope.Event{Type: "string", Key: "acct:9", Command: "SET", Args: [][]byte{
		[]byte("SET"), []byte("acct:9"), []byte(`{"balance":12.5,"name":"bob","secret":"s"}`)}}
	row, err := RowForEvent([]*RuleConfig{r}, ev)
	if err != nil {
		t.Fatal(err)
	}
	if row.PK["id"] != "9" || row.Cols["name"] != "bob" || row.Cols["balance"] != "12.5" {
		t.Fatalf("%+v", row)
	}
	if _, leaked := row.Cols["secret"]; leaked {
		t.Fatal("JSONColumns whitelist violated")
	}
}

func TestDeleteRow(t *testing.T) {
	// binlog DEL carries no data type: must still map via key pattern
	ev := &envelope.Event{Type: "unknown", Key: "user:3", Command: "DEL", Op: envelope.OpDelete,
		Args: [][]byte{[]byte("DEL"), []byte("user:3")}}
	row, err := RowForEvent([]*RuleConfig{ruleHash(t)}, ev)
	if err != nil {
		t.Fatal(err)
	}
	if !row.Del || row.PK["uid"] != "3" {
		t.Fatalf("%+v", row)
	}
}
