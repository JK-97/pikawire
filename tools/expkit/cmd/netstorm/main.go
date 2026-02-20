// Command netstorm runs a net-zero mixed-type operation storm: exactly
// -ops logical operations, each either (A) create a temp key + schedule its
// delete, or (B) delete an existing universe key + schedule its restore with
// the loader's exact value. The key count therefore returns to its start
// (5 * universe) once all operations complete, while the binlog sees the
// full life cycle of all five types.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

var types = []string{"str", "hsh", "lst", "set", "zst"}

func fullValueCmds(kind, key, idx string) []string {
	switch kind {
	case "str":
		v := "sv" + idx
		if n, _ := strconv.Atoi(idx); n%10 == 0 {
			v = strconv.Itoa(1000 + n)
		}
		return []string{"SET", key, v}
	case "hsh":
		return []string{"HMSET", key, "f0", "v0_" + idx, "f1", "v1_" + idx, "f2", "v2_" + idx, "f3", "v3_" + idx}
	case "lst":
		return []string{"RPUSH", key, "e0_" + idx, "e1_" + idx, "e2_" + idx, "e3_" + idx, "e4_" + idx, "e5_" + idx, "e6_" + idx, "e7_" + idx}
	case "set":
		return []string{"SADD", key, "m0_" + idx, "m1_" + idx, "m2_" + idx, "m3_" + idx, "m4_" + idx, "m5_" + idx}
	default:
		return []string{"ZADD", key, "1.5", "z0_" + idx, "2.5", "z1_" + idx, "3.5", "z2_" + idx, "4.5", "z3_" + idx, "5.5", "z4_" + idx, "6.5", "z5_" + idx}
	}
}

type pending struct {
	cmd []string
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	ops := flag.Int("ops", 10000, "number of logical operations")
	universe := flag.Int("universe", 10000, "keys per type in the preloaded universe")
	lag := flag.Int("lag", 1000, "ops between a delete and its counterpart (keeps net zero at the end)")
	interval := flag.Duration("interval", 3*time.Millisecond, "sleep between operations")
	stats := flag.String("stats", "netstorm-stats.json", "stats output file")
	flag.Parse()

	c, err := exp.Dial(*addr, 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer c.Close()

	rng := rand.New(rand.NewSource(42))
	queue := make([][]pending, *lag)
	busy := make(map[string]bool)
	commands, errs := 0, 0

	run := func(cmds ...[]string) {
		for _, cmd := range cmds {
			v, err := c.Do(cmd...)
			if err != nil {
				errs++
				continue
			}
			if verr := v.Err(); verr != nil {
				errs++
				continue
			}
			commands++
		}
	}

	t0 := time.Now()
	for j := 0; j < *ops; j++ {
		kind := types[j%5]
		// flush whatever matured this round (keeps the storm interleaved)
		slot := queue[j%*lag]
		queue[j%*lag] = nil
		for _, p := range slot {
			run(p.cmd)
			if p.cmd[0] != "DEL" {
				delete(busy, p.cmd[1])
			}
		}

		if j%2 == 0 { // mode A: temp key: create now, delete later
			key := "tmp:" + strconv.Itoa(j)
			run(fullValueCmds(kind, key, strconv.Itoa(j)))
			queue[(j+*lag)%*lag] = append(queue[(j+*lag)%*lag], pending{cmd: []string{"DEL", key}})
		} else { // mode B: delete an existing key, restore the exact value later
			idx := ""
			for i := 0; i < 20; i++ {
				k := strconv.Itoa(rng.Intn(*universe))
				if !busy[kind+":"+k] {
					idx = k
					busy[kind+":"+k] = true
					break
				}
			}
			if idx == "" {
				continue // universe saturated this round; storm still net zero
			}
			key := kind + ":" + idx
			run([]string{"DEL", key})
			queue[(j+*lag)%*lag] = append(queue[(j+*lag)%*lag], pending{cmd: fullValueCmds(kind, key, idx)})
		}
		if *interval > 0 {
			time.Sleep(*interval)
		}
	}
	// drain the tail: every pending counterpart completes
	for round := 0; round < *lag; round++ {
		for _, p := range queue[round] {
			run(p.cmd)
		}
	}
	elapsed := time.Since(t0)

	dbsize := "-1"
	if v, err := c.Do("DBSIZE"); err == nil {
		dbsize = strconv.FormatInt(v.I, 10)
	}
	st := struct {
		Ops      int    `json:"ops"`
		Commands int    `json:"commands"`
		Errors   int    `json:"errors"`
		Elapsed  string `json:"elapsed"`
		DBSize   string `json:"dbsize"`
	}{*ops, commands, errs, elapsed.Round(time.Millisecond).String(), dbsize}
	b, _ := json.MarshalIndent(st, "", "  ")
	os.WriteFile(*stats, b, 0o644)
	fmt.Printf("netstorm ops=%d commands=%d errs=%d elapsed=%s dbsize=%s\n", *ops, commands, errs, elapsed.Round(time.Millisecond), dbsize)
	if errs > 0 {
		os.Exit(1)
	}
}
