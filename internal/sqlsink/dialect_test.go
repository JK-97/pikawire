package sqlsink

import (
	"strings"
	"testing"
)

func TestPGUpsert(t *testing.T) {
	d := pgDialect{}
	sql, n := d.Upsert("public.ods_user", []string{"uid"}, []string{"name", "age"})
	if n != 3 {
		t.Fatalf("nargs=%d", n)
	}
	for _, want := range []string{`INSERT INTO public.ods_user ("uid", "name", "age") VALUES ($1, $2, $3)`, `ON CONFLICT ("uid") DO UPDATE SET "name" = EXCLUDED."name", "age" = EXCLUDED."age"`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %q in %s", want, sql)
		}
	}
}

func TestMyUpsert(t *testing.T) {
	d := myDialect{}
	sql, n := d.Upsert("ods_user", []string{"uid"}, []string{"name"})
	if n != 2 || !strings.Contains(sql, "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)") {
		t.Fatalf("%s %d", sql, n)
	}
	d2 := pgDialect{}
	// full-row upsert must not reference pk columns in SET
	sql2, _ := d2.Upsert("t", []string{"a", "b"}, []string{"c", "c"})
	if strings.Count(sql2, "$") != 3 {
		t.Fatalf("dedupe failed: %s", sql2)
	}
}

func TestDeleteShape(t *testing.T) {
	sql, n := myDialect{}.Delete("t", []string{"a", "b"})
	if n != 2 || !strings.Contains(sql, "`a` = ? AND `b` = ?") {
		t.Fatal(sql)
	}
}
