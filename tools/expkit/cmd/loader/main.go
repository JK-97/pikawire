// Command loader preloads a deterministic mixed-type dataset.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

// Universe: 5 types x perType keys each = 5*perType total.
// str:<i> value "sv<i>" (every 10th numeric for INCR), hsh:<i> 4 fields,
// lst:<i> 8 elements, set:<i> 6 members, zst:<i> 6 members with scores.
func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	perType := flag.Int("per-type", 200000, "keys per type")
	workers := flag.Int("workers", 4, "parallel connections")
	batch := flag.Int("batch", 200, "pipeline batch (commands)")
	flag.Parse()

	total := 0
	var wg sync.WaitGroup
	var errs int64
	t0 := time.Now()
	perWorker := *perType / *workers
	type job struct{ lo, hi int }
	jobs := make([]job, 0, *workers)
	for w := 0; w < *workers; w++ {
		lo := w * perWorker
		hi := lo + perWorker
		if w == *workers-1 {
			hi = *perType
		}
		jobs = append(jobs, job{lo, hi})
	}
	var mu sync.Mutex
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			c, err := exp.Dial(*addr, exp.DIAL)
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial:", err)
				return
			}
			defer c.Close()
			cmds := make([][]string, 0, *batch)
			n := 0
			flush := func() {
				if len(cmds) == 0 {
					return
				}
				reps, err := c.Pipeline(cmds)
				if err != nil {
					fmt.Fprintln(os.Stderr, "pipeline:", err)
					mu.Lock()
					errs += int64(len(cmds))
					mu.Unlock()
					return
				}
				for _, r := range reps {
					if r.T == '-' {
						mu.Lock()
						errs++
						mu.Unlock()
					} else {
						n++
					}
				}
				cmds = cmds[:0]
			}
			for i := j.lo; i < j.hi; i++ {
				s := strconv.Itoa(i)
				val := "sv" + s
				if i%10 == 0 {
					val = strconv.Itoa(1000 + i) // numeric for INCR
				}
				cmds = append(cmds, []string{"SET", "str:" + s, val})
				cmds = append(cmds, []string{"HMSET", "hsh:" + s, "f0", "v0_" + s, "f1", "v1_" + s, "f2", "v2_" + s, "f3", "v3_" + s})
				cmds = append(cmds, []string{"RPUSH", "lst:" + s, "e0_" + s, "e1_" + s, "e2_" + s, "e3_" + s, "e4_" + s, "e5_" + s, "e6_" + s, "e7_" + s})
				cmds = append(cmds, []string{"SADD", "set:" + s, "m0_" + s, "m1_" + s, "m2_" + s, "m3_" + s, "m4_" + s, "m5_" + s})
				cmds = append(cmds, []string{"ZADD", "zst:" + s, "1.5", "z0_" + s, "2.5", "z1_" + s, "3.5", "z2_" + s, "4.5", "z3_" + s, "5.5", "z4_" + s, "6.5", "z5_" + s})
				if len(cmds) >= *batch {
					flush()
				}
			}
			flush()
			mu.Lock()
			total += n
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	fmt.Printf("loaded=%d commands-errs=%d elapsed=%s\n", total, errs, time.Since(t0).Round(time.Second))
	pos, err := exp.FetchInfoOffset(*addr, "db0")
	if err == nil {
		fmt.Println("producer_pos_after_load:", pos)
	}
	if errs > 0 || total != 5*(*perType) {
		os.Exit(1)
	}
}
