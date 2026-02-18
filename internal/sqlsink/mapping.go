// Package sqlsink implements the embedded Phase-2 pipeline: declarative
// key/field mapping from Pikawire mode-A events onto relational rows, plus
// upsert/delete execution against Postgres and MySQL.
//
// Design boundaries (docs/pipeline.md):
//   - stateless, per-event transforms only (no joins/aggregation)
//   - target tables are pre-created; no automatic DDL
//   - at-least-once + idempotent upsert: replays converge
package sqlsink

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jk-97/pikawire/internal/envelope"
)

// ErrNoMatch is returned when no rule matches an event; the sink skips the
// event (it may legitimately belong to another pipeline or be consumed by a
// parallel kafka sink).
var ErrNoMatch = errors.New("sqlsink: no rule matches event")

// RuleConfig maps one logical entity family onto one SQL table.
type RuleConfig struct {
	// Match restricts the rule: comma-separated data types; keys use
	// glob patterns where '*' matches within a key segment, e.g. "hash,
	// string" with pattern "user:{uid}:profile".
	DataTypes  []string `yaml:"types"`
	KeyGlob    string   `yaml:"key_pattern,omitempty"` // optional full-key glob
	Table      string   `yaml:"table"`
	PrimaryKey []string `yaml:"pk"`

	// KeyColumns extracts column values from the key itself.
	// "user:{uid}:profile" -> column uid = "42" for key "user:42:profile".
	// Declared as a pattern with {col} placeholders; positions must be
	// disjoint (non-adjacent placeholders allowed, e.g. "a:{x}:b:{y}").
	KeyColumns []KeyCol `yaml:"-"`

	// FieldColumns maps hash fields to columns: "field=column".
	FieldColumns map[string]string `yaml:"fields"`
	// ValueColumn: for string-type values, the column that receives the raw
	// value (JSON expansion applies when ValueJSON is true).
	ValueColumn string `yaml:"value_column"`
	// ValueJSON expands a JSON string value onto columns (top-level fields
	// matched by name against ValueColumns whitelist when non-empty).
	ValueJSON   bool     `yaml:"value_json"`
	JSONColumns []string `yaml:"json_columns"`

	// Delete controls DEL handling: "row" (default) issues DELETE; "ignore"
	// drops deletes (log-style consumers).
	Delete string `yaml:"delete"`

	patternParts []patternSeg // compiled KeyGlob/KeyColumns matcher
}

type patternSeg struct {
	lit   string
	field string // column name captured, "" for literal
}

// KeyCol is a parsed {name} capture from the key pattern.
type KeyCol struct {
	Name string
	Pos  int // segment index in key split by separator strategy below
}

// Compile validates and pre-compiles a rule.
func (r *RuleConfig) Compile() error {
	if r.Table == "" || len(r.PrimaryKey) == 0 {
		return fmt.Errorf("sqlsink: rule %q: table and pk required", r.Table)
	}
	if len(r.DataTypes) == 0 {
		r.DataTypes = []string{"hash", "string"}
	}
	if r.Delete == "" {
		r.Delete = "row"
	}
	if r.KeyGlob != "" {
		if err := r.compileKey(r.KeyGlob); err != nil {
			return err
		}
	}
	return nil
}

// compileKey parses "user:{uid}:profile" into literal/capture segments and
// derives KeyColumns (each {name} becomes a string-valued key column).
func (r *RuleConfig) compileKey(pat string) error {
	var segs []patternSeg
	cur := strings.Builder{}
	for i := 0; i < len(pat); i++ {
		c := pat[i]
		if c == '{' {
			if cur.Len() > 0 {
				segs = append(segs, patternSeg{lit: cur.String()})
				cur.Reset()
			}
			j := strings.IndexByte(pat[i:], '}')
			if j < 0 {
				return fmt.Errorf("sqlsink: unbalanced { in key pattern %q", pat)
			}
			name := pat[i+1 : i+j]
			if name == "" {
				return fmt.Errorf("sqlsink: empty capture in %q", pat)
			}
			segs = append(segs, patternSeg{field: name})
			i += j
			continue
		}
		cur.WriteByte(c)
	}
	if cur.Len() > 0 {
		segs = append(segs, patternSeg{lit: cur.String()})
	}
	// A pattern with captures must separate adjacent captures by literals
	// (otherwise the split point is ambiguous).
	for i := 1; i < len(segs); i++ {
		if segs[i-1].field != "" && segs[i].field != "" {
			return fmt.Errorf("sqlsink: adjacent captures without literal between in %q", pat)
		}
	}
	r.patternParts = segs
	for _, sg := range segs {
		if sg.field != "" {
			r.KeyColumns = append(r.KeyColumns, KeyCol{Name: sg.field})
		}
	}
	return nil
}

// extractKey matches key against the pattern and returns captured columns.
func (r *RuleConfig) extractKey(key string) (map[string]string, bool) {
	if len(r.patternParts) == 0 {
		return nil, true // no pattern: rule matches any key
	}
	out := map[string]string{}
	s := key
	parts := r.patternParts
	for pi := 0; pi < len(parts); pi++ {
		p := parts[pi]
		if p.lit != "" {
			if !strings.HasPrefix(s, p.lit) {
				return nil, false
			}
			s = s[len(p.lit):]
			continue
		}
		// capture: ends at the next literal (or consumes the remainder)
		if pi+1 >= len(parts) {
			out[p.field] = s
			s = ""
			continue
		}
		next := parts[pi+1].lit
		if next == "" { // capture followed by capture: impossible post-compile
			return nil, false
		}
		idx := strings.Index(s, next)
		if idx < 0 {
			return nil, false
		}
		out[p.field] = s[:idx]
		s = s[idx:]
	}
	if s != "" {
		return nil, false
	}
	return out, true
}

// Row is one mapped mutation for the target table.
type Row struct {
	Table  string
	PK     map[string]string // primary key column -> value
	Cols   map[string]string // non-PK columns -> value (nil for deletes)
	Del    bool
	Source envelope.Source // provenance for metrics/debug
}

// PKValues returns pk values ordered by the rule's PrimaryKey list.
func (r *Row) PKValues(order []string) []string {
	out := make([]string, len(order))
	for i, c := range order {
		out[i] = r.PK[c]
	}
	return out
}

// ColNames returns deterministic (sorted) non-PK column names.
func (r *Row) ColNames() []string {
	names := make([]string, 0, len(r.Cols))
	for c := range r.Cols {
		names = append(names, c)
	}
	sortStrings(names)
	return names
}

// RowForEvent maps an envelope event to a Row. Returns ErrNoMatch when the
// event belongs to another entity family.
func RowForEvent(rules []*RuleConfig, ev *envelope.Event) (*Row, error) {
	// DEL/UNDEL entry headers carry no data type: deletes must match by key
	// pattern against every rule (idempotent on non-owning tables).
	isDelete := ev.Op == envelope.OpDelete
	for _, r := range rules {
		if !typeMatches(r, ev.Type) && !isDelete {
			continue
		}
		cap, ok := r.extractKey(ev.Key)
		if !ok {
			continue
		}
		row := &Row{Table: r.Table, PK: map[string]string{}, Cols: map[string]string{}, Source: ev.Source}
		for _, pkc := range r.PrimaryKey {
			v, has := cap[pkc]
			if !has {
				// pk may also come from hash fields (composite natural key)
				if fv, hf := hashField(ev, pkc); hf {
					v, has = fv, true
				}
			}
			if !has {
				return nil, fmt.Errorf("sqlsink: rule %s: cannot fill pk column %q from key %q", r.Table, pkc, ev.Key)
			}
			row.PK[pkc] = v
		}
		switch {
		case isDelete:
			if r.Delete == "ignore" {
				return nil, ErrNoMatch
			}
			row.Del = true
			return row, nil
		case ev.Type == "hash":
			// HMSET/HSET chunks: pairs from args[2:]
			for i := 2; i+1 < len(ev.Args); i += 2 {
				f := string(ev.Args[i])
				v := string(ev.Args[i+1])
				if col, want := r.FieldColumns[f]; want {
					row.Cols[col] = v
				} else if len(r.FieldColumns) == 0 {
					row.Cols[f] = v // default: same-name columns
				}
				// unmapped fields with a whitelist are dropped
			}
			// HDEL: args[2:] are fields; v1 policy: map to NULL via Cols
			if ev.Command == "HDEL" {
				for i := 2; i < len(ev.Args); i++ {
					f := string(ev.Args[i])
					if col, want := r.FieldColumns[f]; want {
						row.Cols[col] = "" // NOTE: NULL requires sentinel encoding (v2)
					}
				}
			}
			if len(row.Cols) == 0 {
				return nil, ErrNoMatch // nothing mapped
			}
			return row, nil
		case ev.Type == "string":
			col := r.ValueColumn
			if col == "" {
				col = "value"
			}
			if ev.Command == "DEL" {
				continue
			}
			if len(ev.Args) < 3 {
				return nil, ErrNoMatch
			}
			val := string(ev.Args[2])
			if r.ValueJSON {
				var m map[string]json.RawMessage
				if err := json.Unmarshal([]byte(val), &m); err != nil {
					return nil, fmt.Errorf("sqlsink: rule %s: value not JSON for %q: %w", r.Table, ev.Key, err)
				}
				for k, raw := range m {
					if !colAllowed(r.JSONColumns, k) {
						continue
					}
					var s string
					if err := json.Unmarshal(raw, &s); err != nil {
						row.Cols[k] = string(raw) // numeric/bool/object stay textual
						continue
					}
					row.Cols[k] = s
				}
				// scalar top-level "id"-style pk from JSON when key has no pattern
				for _, pkc := range r.PrimaryKey {
					if _, has := row.PK[pkc]; has && row.PK[pkc] != "" {
						continue
					}
					if v, ok := row.Cols[pkc]; ok {
						row.PK[pkc] = v
						delete(row.Cols, pkc)
					}
				}
				return row, nil
			}
			row.Cols[col] = val
			return row, nil
		}
	}
	return nil, ErrNoMatch
}

func colAllowed(list []string, name string) bool {
	if len(list) == 0 {
		return true
	}
	for _, c := range list {
		if c == name {
			return true
		}
	}
	return false
}

func hashField(ev *envelope.Event, field string) (string, bool) {
	if ev.Type != "hash" {
		return "", false
	}
	for i := 2; i+1 < len(ev.Args); i += 2 {
		if string(ev.Args[i]) == field {
			return string(ev.Args[i+1]), true
		}
	}
	return "", false
}

func typeMatches(r *RuleConfig, typ string) bool {
	for _, t := range r.DataTypes {
		if t == typ {
			return true
		}
	}
	return false
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
