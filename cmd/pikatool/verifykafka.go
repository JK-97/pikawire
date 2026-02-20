package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/redisclient"
	"github.com/segmentio/kafka-go"
)

type tally struct {
	n   int64
	min int64
	max int64
}

type watchSpec struct {
	prefix   string
	lo, hi   int64 // inclusive key index range
	skip     int64 // >0: skip indices divisible by skip; <0: only indices divisible by |skip|
	minTotal int   // minimum events expected per key
}

// opModel replays the events of one watched key into its final state.
type opModel struct {
	typ    string // none|string|hash|list|set|zset
	s      []byte
	h      map[string][]byte
	l      [][]byte
	set    map[string]struct{}
	z      map[string][]byte
	events int
}

func newModel() *opModel {
	return &opModel{typ: "none"}
}

// apply replays one event. Returns false when the command cannot be modeled
// exactly (those keys fall back to event-count checks only).
func (m *opModel) apply(phase envelope.Phase, cmd string, argv []string) bool {
	if phase == envelope.PhaseHeartbeat {
		return true
	}
	m.events++
	c := strings.ToUpper(cmd)
	switch c {
	case "SET", "SETEX", "PSETEX", "GETSET":
		if len(argv) < 3 {
			return false
		}
		m.typ = "string"
		m.s = []byte(argv[2])
		m.h, m.l, m.set, m.z = nil, nil, nil, nil
	case "DEL", "UNLINK":
		m.typ = "none"
		m.s, m.h, m.l, m.set, m.z = nil, nil, nil, nil, nil
	case "HSET":
		if len(argv) < 4 {
			return false
		}
		if m.typ == "none" {
			m.typ = "hash"
			m.h = map[string][]byte{}
		}
		if m.typ != "hash" {
			return false
		}
		m.h[argv[2]] = []byte(argv[3])
	case "HMSET":
		if m.typ == "none" {
			m.typ = "hash"
			m.h = map[string][]byte{}
		}
		if m.typ != "hash" || len(argv) < 4 || (len(argv)-2)%2 != 0 {
			return false
		}
		for i := 2; i+1 < len(argv); i += 2 {
			m.h[argv[i]] = []byte(argv[i+1])
		}
	case "HDEL":
		if m.typ != "hash" || len(argv) < 3 {
			return false
		}
		for _, f := range argv[2:] {
			delete(m.h, f)
		}
		if len(m.h) == 0 {
			m.typ = "none"
			m.h = nil
		}
	case "RPUSH":
		if m.typ == "none" {
			m.typ = "list"
			m.l = nil
		}
		if m.typ != "list" || len(argv) < 3 {
			return false
		}
		for _, e := range argv[2:] {
			m.l = append(m.l, []byte(e))
		}
	case "LPOP", "RPOP":
		if m.typ != "list" {
			return false
		}
		if len(m.l) > 0 {
			if c == "LPOP" {
				m.l = m.l[1:]
			} else {
				m.l = m.l[:len(m.l)-1]
			}
		}
		if len(m.l) == 0 {
			m.typ = "none"
			m.l = nil
		}
	case "SADD":
		if m.typ == "none" {
			m.typ = "set"
			m.set = map[string]struct{}{}
		}
		if m.typ != "set" || len(argv) < 3 {
			return false
		}
		for _, mem := range argv[2:] {
			m.set[mem] = struct{}{}
		}
	case "SREM":
		if m.typ != "set" || len(argv) < 3 {
			return false
		}
		for _, mem := range argv[2:] {
			delete(m.set, mem)
		}
		if len(m.set) == 0 {
			m.typ = "none"
			m.set = nil
		}
	case "ZADD":
		if m.typ == "none" {
			m.typ = "zset"
			m.z = map[string][]byte{}
		}
		if m.typ != "zset" || len(argv) < 4 || (len(argv)-2)%2 != 0 {
			return false
		}
		for i := 2; i+1 < len(argv); i += 2 {
			m.z[argv[i+1]] = []byte(argv[i])
		}
	case "ZREM":
		if m.typ != "zset" || len(argv) < 3 {
			return false
		}
		for _, mem := range argv[2:] {
			delete(m.z, mem)
		}
		if len(m.z) == 0 {
			m.typ = "none"
			m.z = nil
		}
	default:
		// presence-preserving (TTL) or unknown commands: state unchanged
	}
	return true
}

// canonical renders the model deterministically for comparison.
func (m *opModel) canonical() string {
	switch m.typ {
	case "string":
		return "string:" + string(m.s)
	case "hash":
		fs := make([]string, 0, len(m.h))
		for f, v := range m.h {
			fs = append(fs, f+"="+string(v))
		}
		sort.Strings(fs)
		return "hash{" + strings.Join(fs, ",") + "}"
	case "list":
		es := make([]string, len(m.l))
		for i, e := range m.l {
			es[i] = string(e)
		}
		return "list[" + strings.Join(es, ",") + "]"
	case "set":
		ms := make([]string, 0, len(m.set))
		for x := range m.set {
			ms = append(ms, x)
		}
		sort.Strings(ms)
		return "set{" + strings.Join(ms, ",") + "}"
	case "zset":
		zs := make([]string, 0, len(m.z))
		for x, sc := range m.z {
			zs = append(zs, x+"="+normScore(string(sc)))
		}
		sort.Strings(zs)
		return "zset{" + strings.Join(zs, ",") + "}"
	default:
		return "none"
	}
}

// normScore canonicalizes a zset score so snapshot-emitted "%.6f" text and
// pika's shortest-form replies compare equal (they denote the same double).
func normScore(s string) string {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return s
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// pikaCanonical renders a pika reply (TYPE + read value) deterministically.
func pikaCanonical(typ string, v redisclient.Value) string {
	switch typ {
	case "none":
		return "none"
	case "string":
		if v.Kind != redisclient.KindBytes {
			return "none"
		}
		return "string:" + string(v.Str)
	case "hash":
		fs := []string{}
		for i := 0; i+1 < len(v.Arr); i += 2 {
			fs = append(fs, string(v.Arr[i].Str)+"="+string(v.Arr[i+1].Str))
		}
		sort.Strings(fs)
		return "hash{" + strings.Join(fs, ",") + "}"
	case "zset":
		zs := []string{}
		for i := 0; i+1 < len(v.Arr); i += 2 {
			zs = append(zs, string(v.Arr[i].Str)+"="+normScore(string(v.Arr[i+1].Str)))
		}
		sort.Strings(zs)
		return "zset{" + strings.Join(zs, ",") + "}"
	case "list":
		es := []string{}
		for _, e := range v.Arr {
			es = append(es, string(e.Str))
		}
		return "list[" + strings.Join(es, ",") + "]"
	case "set":
		ms := []string{}
		for _, e := range v.Arr {
			ms = append(ms, string(e.Str))
		}
		sort.Strings(ms)
		return "set{" + strings.Join(ms, ",") + "}"
	default:
		return "none"
	}
}

// verifyKafka consumes a topic from offset 0, replays every watched key into
// a local model, then compares the model against pika's current state.
// Watched keys are given as repeated -watch "prefix:lo-hi[:skip]:min".
func verifyKafka(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verifykafka", flag.ExitOnError)
	broker := fs.String("broker", "127.0.0.1:9092", "kafka broker")
	topic := fs.String("topic", "", "topic to verify")
	pikaAddr := fs.String("pika", "", "pika host:port for the final-state cross-check")
	sampleEvery := fs.Int64("sample", 1000, "sample one ordinary key per N events")
	var watchArgs strList
	fs.Var(&watchArgs, "watch", `repeatable "prefix:lo-hi[:skip]:min" specs, e.g. "sc10m:0-99999:50:2"`)
	var tallyArgs strList
	fs.Var(&tallyArgs, "tally", `repeatable prefix: count events and min/max numeric key suffix`)
	_ = fs.Parse(args)
	if *topic == "" {
		return fmt.Errorf("verifykafka: -topic required")
	}
	var watches []watchSpec
	for _, spec := range watchArgs {
		parts := strings.Split(strings.TrimSpace(spec), ":")
		if len(parts) != 3 && len(parts) != 4 {
			return fmt.Errorf("verifykafka: bad watch %q", spec)
		}
		rg := strings.SplitN(parts[1], "-", 2)
		if len(rg) != 2 {
			return fmt.Errorf("verifykafka: bad range in %q", spec)
		}
		lo, err1 := strconv.ParseInt(rg[0], 10, 64)
		hi, err2 := strconv.ParseInt(rg[1], 10, 64)
		var skip int64
		if len(parts) == 4 {
			skip, err1 = strconv.ParseInt(parts[2], 10, 64)
		}
		min, err3 := strconv.Atoi(parts[len(parts)-1])
		if err1 != nil || err2 != nil || err3 != nil {
			return fmt.Errorf("verifykafka: bad spec %q", spec)
		}
		prefix := parts[0]
		if !strings.HasSuffix(prefix, ":") {
			prefix += ":"
		}
		watches = append(watches, watchSpec{prefix: prefix, lo: lo, hi: hi, skip: skip, minTotal: min})
	}
	if len(watches) == 0 && len(tallyArgs) == 0 {
		return fmt.Errorf("verifykafka: at least one -watch or -tally required")
	}
	tallies := make(map[string]*tally, len(tallyArgs))
	for _, p := range tallyArgs {
		if !strings.HasSuffix(p, ":") {
			p += ":"
		}
		tallies[p] = &tally{}
	}

	conn, err := kafka.Dial("tcp", *broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions(*topic)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return fmt.Errorf("verifykafka: topic %s has no partitions", *topic)
	}

	var mu sync.Mutex
	var totalSnap, totalIncr int64
	models := make(map[string]*opModel, 4096)
	pending := make(map[string][]pendingEvent, 4096)
	sampleStats := make(map[string]int, 64)
	var dupSnap []string

	var wg sync.WaitGroup
	for _, p := range parts {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc, err := kafka.DialLeader(ctx, "tcp", *broker, *topic, p.ID)
			if err != nil {
				return
			}
			last, err := pc.ReadLastOffset()
			pc.Close()
			if err != nil {
				return
			}
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:     []string{*broker},
				Topic:       *topic,
				Partition:   p.ID,
				StartOffset: 0,
				MinBytes:    1,
				MaxBytes:    8 << 20,
			})
			defer r.Close()
			var seen int64
			for seen < last {
				m, err := r.FetchMessage(ctx)
				if err != nil {
					return
				}
				seen = m.Offset + 1
				var raw struct {
					Phase        envelope.Phase `json:"phase"`
					Command      string         `json:"command"`
					Key          string         `json:"key"`
					Args         []string       `json:"args"`
					ArgsEncoding string         `json:"args_encoding"`
					Source       struct {
						Filenum uint32 `json:"filenum"`
						Offset  uint64 `json:"offset"`
						Seq     uint64 `json:"seq"`
					} `json:"source"`
				}
				if err := json.Unmarshal(m.Value, &raw); err != nil {
					continue
				}
				if raw.ArgsEncoding == "base64" {
					for i := range raw.Args {
						if b, err := base64.StdEncoding.DecodeString(raw.Args[i]); err == nil {
							raw.Args[i] = string(b)
						}
					}
				}
				mu.Lock()
				switch raw.Phase {
				case envelope.PhaseIncremental:
					totalIncr++
				default:
					totalSnap++
				}
				if _, ok := matchWatch(raw.Key, watches); ok {
					// DEFERRED replay: events for one logical key can live on
					// different partitions (e.g. DEL is keyed db:unknown:key),
					// so per-partition consumption order is NOT the per-key
					// order. Collect and apply sorted by (phase, source pos,
					// seq) after all partitions are drained.
					rank := 0
					if raw.Phase == envelope.PhaseIncremental {
						rank = 1
					}
					pending[raw.Key] = append(pending[raw.Key], pendingEvent{
						rank: rank, f: raw.Source.Filenum, o: raw.Source.Offset,
						seq: raw.Source.Seq, phase: raw.Phase,
						cmd: raw.Command, args: raw.Args,
					})
				} else {
					h := fnv.New64a()
					h.Write([]byte(raw.Key))
					if h.Sum64()%uint64(*sampleEvery) == 0 {
						if raw.Phase != envelope.PhaseIncremental {
							sampleStats[raw.Key]++
							if sampleStats[raw.Key] > 1 {
								dupSnap = append(dupSnap, raw.Key)
							}
						}
					}
				}
				for p, t := range tallies {
					if strings.HasPrefix(raw.Key, p) {
						t.n++
						if v, err := strconv.ParseInt(strings.TrimPrefix(raw.Key, p), 10, 64); err == nil {
							if t.n == 1 || v < t.min {
								t.min = v
							}
							if v > t.max {
								t.max = v
							}
						}
						break
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// deterministic per-key replay in source order
	pendKeys := make([]string, 0, len(pending))
	for k := range pending {
		pendKeys = append(pendKeys, k)
	}
	sort.Strings(pendKeys)
	for _, k := range pendKeys {
		evs := pending[k]
		sort.SliceStable(evs, func(i, j int) bool {
			a, b := evs[i], evs[j]
			if a.rank != b.rank {
				return a.rank < b.rank
			}
			if a.f != b.f {
				return a.f < b.f
			}
			if a.o != b.o {
				return a.o < b.o
			}
			return a.seq < b.seq
		})
		md := newModel()
		models[k] = md
		for _, e := range evs {
			md.apply(e.phase, e.cmd, e.args)
		}
	}

	fmt.Printf("topic=%s events total=%d (snapshot=%d incremental=%d)\n",
		*topic, totalSnap+totalIncr, totalSnap, totalIncr)
	for p, t := range tallies {
		fmt.Printf("tally prefix %q: events=%d min=%d max=%d\n", p, t.n, t.min, t.max)
	}

	mu.Lock()
	defer mu.Unlock()
	// 1) per-key minimum event accounting
	bad := 0
	for _, w := range watches {
		for i := w.lo; i <= w.hi; i++ {
			if w.skip > 0 && i%w.skip == 0 {
				continue
			}
			if w.skip < 0 && i%(-w.skip) != 0 {
				continue
			}
			k := w.prefix + strconv.FormatInt(i, 10)
			md, ok := models[k]
			ev := 0
			if ok {
				ev = md.events
			}
			if ev < w.minTotal {
				bad++
				if bad <= 20 {
					fmt.Printf("MISSING %s events=%d min=%d\n", k, ev, w.minTotal)
				}
			}
		}
	}
	fmt.Printf("watched keys with fewer events than min: %d\n", bad)

	// 2) final-state comparison against pika
	if *pikaAddr != "" {
		pikaHost, pikaPort, err := splitHostPort(*pikaAddr)
		if err != nil {
			return fmt.Errorf("verifykafka: bad -pika: %w", err)
		}
		c, err := redisclient.Dial(ctx, fmt.Sprintf("%s:%d", pikaHost, pikaPort), "", 5*time.Second)
		if err != nil {
			return fmt.Errorf("verifykafka: pika dial: %w", err)
		}
		defer c.Close()

		keys := make([]string, 0, 16384)
		for _, w := range watches {
			for i := w.lo; i <= w.hi; i++ {
				if w.skip > 0 && i%w.skip == 0 {
					continue
				}
				if w.skip < 0 && i%(-w.skip) != 0 {
					continue
				}
				keys = append(keys, w.prefix+strconv.FormatInt(i, 10))
			}
		}
		mism := 0
		checked := 0
		for off := 0; off < len(keys); off += 512 {
			page := keys[off:]
			if len(page) > 512 {
				page = page[:512]
			}
			cmds := make([][][]byte, 0, len(page))
			for _, k := range page {
				cmds = append(cmds, [][]byte{[]byte("TYPE"), []byte(k)})
			}
			types, err := c.Do(cmds)
			if err != nil {
				return fmt.Errorf("verifykafka: pika TYPE: %w", err)
			}
			reads := make([][][]byte, 0, len(page))
			readIdx := make([]int, 0, len(page))
			for i, k := range page {
				t := types[i].Text()
				switch t {
				case "string":
					reads = append(reads, [][]byte{[]byte("GET"), []byte(k)})
				case "hash":
					reads = append(reads, [][]byte{[]byte("HGETALL"), []byte(k)})
				case "list":
					reads = append(reads, [][]byte{[]byte("LRANGE"), []byte(k), []byte("0"), []byte("-1")})
				case "set":
					reads = append(reads, [][]byte{[]byte("SMEMBERS"), []byte(k)})
				case "zset":
					reads = append(reads, [][]byte{[]byte("ZRANGE"), []byte(k), []byte("0"), []byte("-1"), []byte("WITHSCORES")})
				default:
					readIdx = append(readIdx, i)
				}
			}
			replies, err := c.Do(reads)
			if err != nil {
				return fmt.Errorf("verifykafka: pika read: %w", err)
			}
			ri := 0
			for i, k := range page {
				md := models[k]
				if md == nil {
					md = newModel()
				}
				typ := types[i].Text()
				var canon string
				if typ != "none" && typ != "unknown" {
					canon = pikaCanonical(typ, replies[ri])
					ri++
				} else {
					canon = "none"
				}
				checked++
				want := md.canonical()
				if typ == "unknown" {
					// unknown type (stream etc.): cannot model; skip
					continue
				}
				// tolerate pika "none" when the model has no data and no events
				if canon == "none" && want == "none" {
					continue
				}
				if canon != want {
					mism++
					if mism <= 20 {
						fmt.Printf("MISMATCH %s pika=%s model=%s\n", k, canon, want)
					}
				}
			}
		}
		fmt.Printf("final-state compared: %d keys, mismatches: %d\n", checked, mism)
		if mism > 0 {
			return fmt.Errorf("verifykafka: final-state mismatches: %d", mism)
		}
	}
	if bad > 0 || len(dupSnap) > 0 {
		return fmt.Errorf("verifykafka: consistency violations: missing=%d dup-snap=%d", bad, len(dupSnap))
	}
	fmt.Println("VERIFY-OK")
	return nil
}

func splitHostPort(s string) (string, int, error) {
	i := strings.LastIndex(s, ":")
	if i <= 0 {
		return "", 0, fmt.Errorf("want host:port")
	}
	p, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return "", 0, err
	}
	return s[:i], p, nil
}

func matchWatch(k string, ws []watchSpec) (watchSpec, bool) {
	for _, w := range ws {
		if strings.HasPrefix(k, w.prefix) {
			idx, err := strconv.ParseInt(strings.TrimPrefix(k, w.prefix), 10, 64)
			if err == nil && idx >= w.lo && idx <= w.hi && (w.skip == 0 || (w.skip > 0 && idx%w.skip != 0) || (w.skip < 0 && idx%(-w.skip) == 0)) {
				return w, true
			}
		}
	}
	return watchSpec{}, false
}

// pendingEvent is one collected event awaiting deterministic per-key replay.
type pendingEvent struct {
	rank  int
	f     uint32
	o     uint64
	seq   uint64
	phase envelope.Phase
	cmd   string
	args  []string
}

type strList []string

func (s *strList) String() string { return strings.Join(*s, ",") }

func (s *strList) Set(v string) error { *s = append(*s, v); return nil }
