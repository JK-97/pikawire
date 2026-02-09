// Package envelope defines the canonical CDC event contract (Mode A).
//
// An event is a faithful, lossless representation of one PikiwiDB command
// (incremental binlog entry, or a full-image command synthesized from the
// dbsync dump snapshot). No business semantics are interpreted: key parsing,
// column modeling and flattening are left to downstream consumers.
//
// Events are encoded as JSON objects; Kafka message keys are stable strings
// derived from the logical entity so that all events for one key land in the
// same partition and keep their relative order.
package envelope

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// SchemaVersion is the on-the-wire contract version. Breaking changes bump it.
const SchemaVersion = 1

// Phase tells where an event came from.
type Phase string

const (
	PhaseSnapshot    Phase = "snapshot"    // full image from the dbsync dump
	PhaseIncremental Phase = "incremental" // raw binlog entry
	PhaseHeartbeat   Phase = "heartbeat"   // position marker, no payload
)

// Op classifies the command for naive consumers.
type Op string

const (
	OpRead      Op = "r" // snapshot full image
	OpCreate    Op = "c"
	OpUpdate    Op = "u"
	OpDelete    Op = "d"
	OpOther     Op = "x" // commands we cannot classify (RENAMENX, LPUSHX, ...)
	OpHeartbeat Op = "h"
)

// Source identifies the origin position in the source database.
type Source struct {
	ID      string `json:"id"`      // source address, e.g. "10.0.0.1:9221"
	DB      string `json:"db"`      // db name, e.g. "db0"
	Filenum uint32 `json:"filenum"` // binlog file number; 0 for snapshot events
	Offset  uint64 `json:"offset"`  // binlog offset; 0 for snapshot events
	// Seq orders events that share a source position: a monotonic record
	// ordinal on snapshot events (a key can emit several commands at the
	// dump anchor); 0 on incremental events, whose binlog position is
	// already unique (and keeping 0 makes event_id stable across restarts).
	Seq uint64 `json:"seq"`
	// ExecTimeSec is the source-side command execution time (unix seconds,
	// from binlog). Snapshot events report the read time instead.
	ExecTimeSec uint64 `json:"exec_time_s,omitempty"`
}

// Event is the public contract shared by all sinks.
//
// The raw command is split for convenience:
//
//	Command  command name, upper case ("HSET", "DEL", ...)
//	Key      the primary key (argv[1] for standard commands)
//	Type     data type of the key: string|hash|list|set|zset|stream|unknown
//	Args     full command argv including the command name itself
//
// Args preserves the exact bytes received from the source. JSON strings must
// be valid UTF-8, so ArgsEncoding records how Args were serialized:
// "utf8" when every element is valid UTF-8, otherwise "base64".
type Event struct {
	SchemaVersion int      `json:"schema_version"`
	Phase         Phase    `json:"phase"`
	Op            Op       `json:"op"`
	DB            string   `json:"db"`
	Type          string   `json:"data_type"`
	Key           string   `json:"key"`
	Command       string   `json:"command"`
	Args          [][]byte `json:"-"`
	ArgsEncoding  string   `json:"args_encoding"`
	Heartbeat     bool     `json:"-"`
	Source        Source   `json:"source"`
}

// KafkaKey is the partition key for entity-level ordering: db + ":" + key,
// deliberately WITHOUT data_type. Type is unstable per entity in the stream
// (DEL carries "unknown"; a key can flip string->hash mid-stream; snapshot vs
// incremental derive it differently), and Kafka only orders within a
// partition — embedding type would scatter one entity's events across
// partitions and void the per-key ordering contract. Consumers needing type
// still find it in the event payload.
func (e *Event) KafkaKey() []byte {
	return []byte(e.DB + ":" + e.Key)
}

// EventID is a unique, replay-stable identifier for idempotent consumers.
// Full-image snapshot events are re-generated on restart, so their id
// intentionally excludes seq: identity is (phase, db, type, key, command).
func (e *Event) EventID() string {
	if e.Phase == PhaseIncremental {
		return fmt.Sprintf("inc:%s:%d:%d:%d", e.Source.ID, e.Source.Filenum, e.Source.Offset, e.Source.Seq)
	}
	return string(e.Phase) + ":" + e.Source.ID + ":" + e.DB + ":" + e.Type + ":" + e.Key + ":" + e.Command
}

// MarshalJSON encodes the envelope. Args are emitted as an encoded string
// array following ArgsEncoding.
func (e *Event) MarshalJSON() ([]byte, error) {
	argsEnc := "utf8"
	if !allValidUTF8(e.Args) {
		argsEnc = "base64"
	}
	buf := bytes.NewBuffer(make([]byte, 0, 256+16*len(e.Args)))
	buf.WriteByte('{')
	buf.WriteString(`"schema_version":`)
	buf.WriteString(strconv.Itoa(SchemaVersion))
	writeJSONStr(buf, "phase", string(e.Phase))
	writeJSONStr(buf, "op", string(e.Op))
	writeJSONStr(buf, "db", e.DB)
	writeJSONStr(buf, "data_type", e.Type)
	writeJSONStr(buf, "key", e.Key)
	writeJSONStr(buf, "command", e.Command)
	buf.WriteString(`,"args":[`)
	for i, a := range e.Args {
		if i > 0 {
			buf.WriteByte(',')
		}
		if argsEnc == "base64" {
			buf.WriteByte('"')
			buf.WriteString(base64.StdEncoding.EncodeToString(a))
			buf.WriteByte('"')
		} else {
			writeRawJSONStr(buf, a)
		}
	}
	buf.WriteByte(']')
	writeJSONStr(buf, "args_encoding", argsEnc)
	writeJSONStr(buf, "event_id", e.EventID())
	buf.WriteString(`,"source":{"id":`)
	writeRawJSONStr(buf, []byte(e.Source.ID))
	buf.WriteString(`,"db":`)
	writeRawJSONStr(buf, []byte(e.Source.DB))
	buf.WriteString(`,"filenum":`)
	buf.WriteString(strconv.FormatUint(uint64(e.Source.Filenum), 10))
	buf.WriteString(`,"offset":`)
	buf.WriteString(strconv.FormatUint(e.Source.Offset, 10))
	buf.WriteString(`,"seq":`)
	buf.WriteString(strconv.FormatUint(e.Source.Seq, 10))
	if e.Source.ExecTimeSec > 0 {
		buf.WriteString(`,"exec_time_s":`)
		buf.WriteString(strconv.FormatUint(e.Source.ExecTimeSec, 10))
	}
	buf.WriteString("}}")
	return buf.Bytes(), nil
}

func allValidUTF8(args [][]byte) bool {
	for _, a := range args {
		if !utf8.Valid(a) {
			return false
		}
	}
	return true
}

func writeJSONStr(buf *bytes.Buffer, key, val string) {
	buf.WriteByte(',')
	buf.WriteByte('"')
	buf.WriteString(key)
	buf.WriteString(`":"`)
	buf.WriteString(jsonEscape(val))
	buf.WriteByte('"')
}

func writeRawJSONStr(buf *bytes.Buffer, val []byte) {
	buf.WriteByte('"')
	buf.WriteString(jsonEscape(string(val)))
	buf.WriteByte('"')
}

// jsonEscape escapes a string for JSON output (minimal, allocation-light).
// Control bytes below 0x20 that lack short escapes are emitted as \u00XX.
func jsonEscape(s string) string {
	var out []byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		var esc string
		switch c {
		case '"':
			esc = `\"`
		case '\\':
			esc = `\\`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\t':
			esc = `\t`
		default:
			if c < 0x20 {
				esc = `\u00` + hex(c)
			} else {
				continue
			}
		}
		out = append(out, s[start:i]...)
		out = append(out, esc...)
		start = i + 1
	}
	if out == nil {
		return s
	}
	return string(append(out, s[start:]...))
}

func hex(c byte) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[c>>4], digits[c&0xf]})
}
