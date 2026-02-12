package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	r := New("test")
	c := r.Counter("pikawire_test_total", "docs")
	c.Add(3)
	g := r.Gauge("pikawire_test_gauge", "docs")
	g.Set(7)
	l := r.Labeled("pikawire_test_events", "by phase")
	l.With("snapshot").Add(5)
	rr := httptest.NewRecorder()
	r.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{"pikawire_test_total 3", "pikawire_test_gauge 7", `pikawire_test_events{label="snapshot"} 5`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
