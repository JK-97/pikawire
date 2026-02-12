// Package metrics is a dependency-free Prometheus text endpoint: counters
// and gauges exposed at /metrics, plus /healthz and /debug/pprof.
package metrics

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Counter is a monotonically increasing uint64 metric.
type Counter struct{ v syncCounter }
type syncCounter struct {
	mu sync.Mutex
	n  uint64
}

func (c *Counter) Add(n uint64) {
	c.v.mu.Lock()
	c.v.n += n
	c.v.mu.Unlock()
}
func (c *Counter) Value() uint64 {
	c.v.mu.Lock()
	defer c.v.mu.Unlock()
	return c.v.n
}

// Gauge holds an arbitrary int64 value.
type Gauge struct {
	mu sync.Mutex
	v  int64
}

func (g *Gauge) Set(v int64) {
	g.mu.Lock()
	g.v = v
	g.mu.Unlock()
}
func (g *Gauge) Value() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

// Labeled is a fixed-set of labeled counters (phase, kind...).
type Labeled struct {
	name string
	help string
	mu   sync.Mutex
	vals map[string]*Counter
}

func NewLabeled(name, help string) *Labeled {
	return &Labeled{name: name, help: help, vals: map[string]*Counter{}}
}

func (l *Labeled) With(label string) *Counter {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.vals[label]
	if !ok {
		c = &Counter{}
		l.vals[label] = c
	}
	return c
}

// Registry renders and serves metrics.
type Registry struct {
	mu         sync.Mutex
	counters   []named
	gauges     []namedG
	labeled    []*Labeled
	started    time.Time
	versionStr string
}

type named struct {
	name string
	help string
	c    *Counter
}
type namedG struct {
	name string
	help string
	g    *Gauge
}

func New(version string) *Registry {
	return &Registry{started: time.Now(), versionStr: version}
}

func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.counters {
		if n.name == name {
			return n.c
		}
	}
	c := &Counter{}
	r.counters = append(r.counters, named{name, help, c})
	return c
}

func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.gauges {
		if n.name == name {
			return n.g
		}
	}
	g := &Gauge{}
	r.gauges = append(r.gauges, namedG{name, help, g})
	return g
}

func (r *Registry) Labeled(name, help string) *Labeled {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := NewLabeled(name, help)
	r.labeled = append(r.labeled, l)
	return l
}

// Handler returns the http mux for the admin server.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", r.serveMetrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	return mux
}

func (r *Registry) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	var b strings.Builder
	b.WriteString("# HELP pikawire_uptime_seconds process uptime\n")
	b.WriteString("# TYPE pikawire_uptime_seconds gauge\n")
	b.WriteString("pikawire_uptime_seconds " + strconv.FormatInt(int64(time.Since(r.started).Seconds()), 10) + "\n")
	b.WriteString("# HELP pikawire_build_info build metadata\n# TYPE pikawire_build_info gauge\n")
	b.WriteString(`pikawire_build_info{version="` + r.versionStr + `",go="` + runtime.Version() + `"} 1` + "\n")
	r.mu.Lock()
	names := make([]named, len(r.counters))
	copy(names, r.counters)
	gs := make([]namedG, len(r.gauges))
	copy(gs, r.gauges)
	lbs := make([]*Labeled, len(r.labeled))
	copy(lbs, r.labeled)
	r.mu.Unlock()
	for _, c := range names {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", c.name, c.help, c.name, c.name, c.c.Value())
	}
	for _, g := range gs {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", g.name, g.help, g.name, g.name, g.g.Value())
	}
	for _, l := range lbs {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", l.name, l.help, l.name)
		l.mu.Lock()
		keys := make([]string, 0, len(l.vals))
		for k := range l.vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s{label=%q} %d\n", l.name, k, l.vals[k].Value())
		}
		l.mu.Unlock()
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
