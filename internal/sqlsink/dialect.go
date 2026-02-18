package sqlsink

import (
	"fmt"
	"strings"
)

// Dialect renders single-statement SQL for a Row under a placeholder style.
type Dialect interface {
	// Upsert renders an insert-of-cols ON-CONFLICT-update-of-cols statement.
	// Argument order: cols..., then pkValues... (the WHERE/CONFLICT keys).
	Upsert(table string, pk, cols []string) (sql string, nArgs int)
	Delete(table string, pk []string) (sql string, nArgs int)
}

// NewDialect maps a driver name to a Dialect.
func NewDialect(driver string) (Dialect, error) {
	switch driver {
	case "postgres", "pgx":
		return pgDialect{}, nil
	case "mysql":
		return myDialect{}, nil
	default:
		return nil, fmt.Errorf("sqlsink: unsupported driver %q (postgres|mysql)", driver)
	}
}

func quote(id string) string { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }

type pgDialect struct{}

func (pgDialect) Upsert(table string, pk, cols []string) (string, int) {
	cols = dedupe(cols)
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(joinQuoted(append(append([]string{}, pk...), cols...)))
	b.WriteString(") VALUES (")
	n := len(pk) + len(cols)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	b.WriteString(") ON CONFLICT (")
	b.WriteString(joinQuoted(pk))
	b.WriteString(") DO UPDATE SET ")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quote(c))
		b.WriteString(" = EXCLUDED.")
		b.WriteString(quote(c))
	}
	return b.String(), n
}

func (pgDialect) Delete(table string, pk []string) (string, int) {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(table)
	b.WriteString(" WHERE ")
	for i, c := range pk {
		if i > 0 {
			b.WriteString(" AND ")
		}
		fmt.Fprintf(&b, "%s = $%d", quote(c), i+1)
	}
	return b.String(), len(pk)
}

type myDialect struct{}

func (myDialect) Upsert(table string, pk, cols []string) (string, int) {
	cols = dedupe(cols)
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(joinBackticked(append(append([]string{}, pk...), cols...)))
	b.WriteString(") VALUES (")
	n := len(pk) + len(cols)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	b.WriteString(") ON DUPLICATE KEY UPDATE ")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("`")
		b.WriteString(c)
		b.WriteString("` = VALUES(`")
		b.WriteString(c)
		b.WriteString("`)")
	}
	return b.String(), n
}

func (myDialect) Delete(table string, pk []string) (string, int) {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(table)
	b.WriteString(" WHERE ")
	for i, c := range pk {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("`")
		b.WriteString(c)
		b.WriteString("` = ?")
	}
	return b.String(), len(pk)
}

func joinQuoted(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quote(c)
	}
	return strings.Join(out, ", ")
}

func joinBackticked(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = "`" + c + "`"
	}
	return strings.Join(out, ", ")
}

func dedupe(s []string) []string {
	seen := map[string]bool{}
	out := s[:0]
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
