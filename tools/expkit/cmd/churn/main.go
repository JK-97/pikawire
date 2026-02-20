// Command churn runs a randomized all-operations workload against the
// source, overlapping the snapshot scan window. It exists purely as load:
// correctness verdicts come from the audit + compare tools.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

type stats struct {
	Ops    int64            `json:"ops"`
	Errs   int64            `json:"errs"`
	ByOp   map[string]int64 `json:"by_op"`
	Start  time.Time        `json:"-"`
	ByOpMu *sync.Mutex      `json:"-"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	workers := flag.Int("workers", 8, "concurrent writers")
	perType := flag.Int("per-type", 200000, "universe size per type (matches loader)")
	batch := flag.Int("batch", 50, "commands per pipeline batch")
	sleep := flag.Duration("sleep", 3*time.Millisecond, "sleep between batches per worker")
	secs := flag.Int("secs", 0, "stop after N seconds (0=until stop file)")
	stopFile := flag.String("stop", "", "stop when this file appears")
	statsFile := flag.String("stats", "churn-stats.json", "final stats file")
	risky := flag.Bool("risky", true, "include mset/rename/setex/expire/type-flip ops")
	flag.Parse()

	var ops, errs atomic.Int64
	var byOpMu sync.Mutex
	byOp := map[string]int64{}
	seed := time.Now().UnixNano()
	deadline := time.Time{}
	if *secs > 0 {
		deadline = time.Now().Add(time.Duration(*secs) * time.Second)
	}

	stopNow := func() bool {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return true
		}
		if *stopFile != "" {
			if _, err := os.Stat(*stopFile); err == nil {
				return true
			}
		}
		return false
	}

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := exp.Dial(*addr, exp.DIAL)
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial:", err)
				return
			}
			defer c.Close()
			r := rand.New(rand.NewSource(seed + int64(w)))
			var newCnt int
			for !stopNow() {
				cmds := make([][]string, 0, *batch)
				names := make([]string, 0, *batch)
				for i := 0; i < *batch; i++ {
					cmd, name := genOp(r, *perType, w, &newCnt, *risky)
					cmds = append(cmds, cmd)
					names = append(names, name)
				}
				reps, err := c.Pipeline(cmds)
				if err != nil {
					// connection died mid-batch: outcomes of this batch are
					// unknown but all-or-applied on pika side are irrelevant
					// to verdicts (compare is live). Reconnect.
					fmt.Fprintln(os.Stderr, "pipeline err, redial:", err)
					c.Close()
					for attempt := 0; attempt < 30; attempt++ {
						time.Sleep(200 * time.Millisecond)
						if c, err = exp.Dial(*addr, exp.DIAL); err == nil {
							break
						}
					}
					if err != nil {
						return
					}
					continue
				}
				localOps, localErrs := int64(0), int64(0)
				localBy := map[string]int64{}
				for i, rp := range reps {
					if rp.T == '-' {
						localErrs++
						continue
					}
					localOps++
					localBy[names[i]]++
				}
				ops.Add(localOps)
				errs.Add(localErrs)
				byOpMu.Lock()
				for k, v := range localBy {
					byOp[k] += v
				}
				byOpMu.Unlock()
				time.Sleep(*sleep)
			}
		}(w)
	}
	tick := time.NewTicker(10 * time.Second)
	done := make(chan struct{})
	go func() {
		defer tick.Stop()
		last := ops.Load()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				now := ops.Load()
				fmt.Printf("churn ops=%d errs=%d rate=%d/s\n", now, errs.Load(), (now-last)/10)
				last = now
			}
		}
	}()
	wg.Wait()
	close(done)
	out, _ := json.MarshalIndent(struct {
		Ops  int64            `json:"ops_ok"`
		Errs int64            `json:"errs"`
		ByOp map[string]int64 `json:"by_op"`
	}{ops.Load(), errs.Load(), byOp}, "", " ")
	_ = os.WriteFile(*statsFile, out, 0o644)
	fmt.Printf("churn done ops=%d errs=%d\n", ops.Load(), errs.Load())
}

func genOp(r *rand.Rand, perType, worker int, newCnt *int, risky bool) ([]string, string) {
	i := r.Intn(perType)
	s := strconv.Itoa(i)
	pick := r.Float64()
	switch r.Intn(5) {
	case 0: // string universe + type-flip keys
		k := "str:" + s
		switch {
		case pick < 0.30:
			return []string{"SET", k, "v" + strconv.FormatInt(r.Int63(), 36)}, "set"
		case pick < 0.44:
			return []string{"INCRBY", k, strconv.Itoa(r.Intn(100) - 50)}, "incrby"
		case pick < 0.52:
			return []string{"APPEND", k, "a" + strconv.Itoa(r.Intn(9))}, "append"
		case pick < 0.62:
			return []string{"DEL", k}, "del"
		case pick < 0.67 && risky:
			return []string{"GETSET", k, "gs" + s}, "getset"
		case pick < 0.72 && risky:
			return []string{"SETEX", k, "7200", "sx" + strconv.FormatInt(r.Int63(), 36)}, "setex"
		case pick < 0.76 && risky:
			return []string{"EXPIRE", k, "86400"}, "expire"
		case pick < 0.84:
			return []string{"DEL", "flp:" + s}, "flip-del"
		case pick < 0.92 && risky:
			return []string{"HSET", "flp:" + s, "fx", "v" + strconv.Itoa(r.Intn(1000))}, "flip-hset"
		default:
			return []string{"SET", "flp:" + s, "fv" + strconv.Itoa(r.Intn(1000))}, "flip-set"
		}
	case 1: // hash
		k := "hsh:" + s
		switch {
		case pick < 0.40:
			return []string{"HSET", k, "f" + strconv.Itoa(r.Intn(6)), "hv" + strconv.FormatInt(r.Int63(), 36)}, "hset"
		case pick < 0.55:
			return []string{"HMSET", k, "g0", "a", "g1", "b"}, "hmset"
		case pick < 0.70:
			return []string{"HDEL", k, "f" + strconv.Itoa(r.Intn(6))}, "hdel"
		case pick < 0.85:
			return []string{"DEL", k}, "del"
		default:
			return []string{"HSET", k, "f0", "re" + strconv.Itoa(r.Intn(1000))}, "hset"
		}
	case 2: // list
		k := "lst:" + s
		switch {
		case pick < 0.35:
			return []string{"RPUSH", k, "e" + strconv.FormatInt(r.Int63(), 36)}, "rpush"
		case pick < 0.55:
			return []string{"LPUSH", k, "h" + strconv.FormatInt(r.Int63(), 36)}, "lpush"
		case pick < 0.65:
			return []string{"LPOP", k}, "lpop"
		case pick < 0.72:
			return []string{"RPOP", k}, "rpop"
		case pick < 0.82:
			return []string{"LTRIM", k, "0", "60"}, "ltrim"
		case pick < 0.92:
			return []string{"DEL", k}, "del"
		default:
			return []string{"RPUSH", k, "seed" + s}, "rpush"
		}
	case 3: // set
		k := "set:" + s
		switch {
		case pick < 0.40:
			return []string{"SADD", k, "m" + strconv.FormatInt(r.Int63n(10000), 10)}, "sadd"
		case pick < 0.60:
			return []string{"SREM", k, "m" + strconv.FormatInt(r.Int63n(10000), 10)}, "srem"
		case pick < 0.70:
			return []string{"SPOP", k}, "spop"
		case pick < 0.85:
			return []string{"DEL", k}, "del"
		default:
			return []string{"SADD", k, "seed" + s}, "sadd"
		}
	case 4: // zset + global ops
		if pick < 0.55 {
			k := "zst:" + s
			switch {
			case r.Intn(100) < 50:
				return []string{"ZADD", k, strconv.FormatFloat(float64(r.Intn(1000)), 'f', 1, 64), "zz" + strconv.FormatInt(r.Int63n(500), 10)}, "zadd"
			case r.Intn(100) < 75:
				return []string{"ZREM", k, "zz" + strconv.FormatInt(r.Int63n(500), 10)}, "zrem"
			default:
				return []string{"DEL", k}, "del"
			}
		}
		// global mix: new keys, multi-key, rename
		switch r.Intn(6) {
		case 0, 1: // brand-new keys of random type
			nn := *newCnt
			*newCnt++
			nk := fmt.Sprintf("new:%d:%d", worker, nn)
			switch r.Intn(5) {
			case 0:
				return []string{"SET", nk, "nv" + strconv.Itoa(nn)}, "new-str"
			case 1:
				return []string{"HSET", nk, "f", "v" + strconv.Itoa(nn)}, "new-hash"
			case 2:
				return []string{"RPUSH", nk, "e" + strconv.Itoa(nn)}, "new-list"
			case 3:
				return []string{"SADD", nk, "m" + strconv.Itoa(nn)}, "new-set"
			default:
				return []string{"ZADD", nk, "1.25", "z" + strconv.Itoa(nn)}, "new-zset"
			}
		case 2: // delete some previously created new keys
			nn := r.Intn(*newCnt + 1)
			return []string{"DEL", fmt.Sprintf("new:%d:%d", worker, nn)}, "del-new"
		case 3:
			if !risky {
				return []string{"SET", "str:" + s, "x" + strconv.FormatInt(r.Int63(), 36)}, "set"
			}
			return []string{"MSET", "str:" + s, "m1v" + strconv.FormatInt(r.Int63(), 36), "hsh:" + s, "m2v" + strconv.FormatInt(r.Int63(), 36)}, "mset"
		case 4:
			if !risky {
				return []string{"SET", "str:" + s, "y" + strconv.FormatInt(r.Int63(), 36)}, "set"
			}
			// multi-key delete + mset on two fresh keys (fold-coverage probe)
			j := r.Intn(perType)
			return []string{"DEL", "set:" + strconv.Itoa(j)}, "del"
		default:
			j := r.Intn(perType)
			return []string{"DEL", "hsh:" + strconv.Itoa(j)}, "del"
		}
	}
	return []string{"PING"}, "ping"
}
