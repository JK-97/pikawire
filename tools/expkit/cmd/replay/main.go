// Command replay applies the captured event stream (JSONL) to a target
// instance, preserving per-key order, to reconstruct the source state.
package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

type ev struct {
	Phase        string   `json:"phase"`
	Key          string   `json:"key"`
	Command      string   `json:"command"`
	Args         []string `json:"args"`
	ArgsEncoding string   `json:"args_encoding"`
	Source       struct {
		Filenum uint32 `json:"filenum"`
		Offset  uint64 `json:"offset"`
	} `json:"source"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9222", "target host:port")
	events := flag.String("events", "events.jsonl", "input JSONL")
	workers := flag.Int("workers", 8, "parallel appliers (key-sharded)")
	batch := flag.Int("batch", 200, "pipeline batch per worker")
	dedupe := flag.Bool("dedupe", true, "skip duplicate incremental (filenum,offset) events (at-least-once contract)")
	flag.Parse()

	f, err := os.Open(*events)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	shards := make([]chan ev, *workers)
	var wg sync.WaitGroup
	var counters struct {
		mu      sync.Mutex
		applied int64
		errs    int64
		log     []string
	}
	for i := range shards {
		shards[i] = make(chan ev, 4096)
		wg.Add(1)
		go func(ch chan ev) {
			defer wg.Done()
			c, err := exp.Dial(*addr, exp.DIAL)
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial:", err)
				return
			}
			defer c.Close()
			buf := make([][]string, 0, *batch)
			flush := func() {
				if len(buf) == 0 {
					return
				}
				reps, err := c.Pipeline(buf)
				if err != nil {
					counters.mu.Lock()
					counters.errs += int64(len(buf))
					counters.log = append(counters.log, "PIPELINE: "+err.Error())
					counters.mu.Unlock()
				} else {
					for i, rp := range reps {
						if rp.T == '-' {
							counters.mu.Lock()
							counters.errs++
							if len(counters.log) < 100 {
								counters.log = append(counters.log, fmt.Sprintf("cmd=%q err=%s", strings.Join(buf[i], " "), rp.S))
							}
							counters.mu.Unlock()
						} else {
							counters.mu.Lock()
							counters.applied++
							counters.mu.Unlock()
						}
					}
				}
				buf = buf[:0]
			}
			for e := range ch {
				args := e.Args
				if e.ArgsEncoding == "base64" {
					args = make([]string, len(e.Args))
					for i, a := range e.Args {
						b, err := base64.StdEncoding.DecodeString(a)
						if err != nil {
							counters.mu.Lock()
							counters.errs++
							counters.log = append(counters.log, "b64: "+err.Error())
							counters.mu.Unlock()
							continue
						}
						args[i] = string(b)
					}
				}
				buf = append(buf, args)
				if len(buf) >= *batch {
					flush()
				}
			}
			flush()
		}(shards[i])
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	var total, skipped, deduped int
	lineNo := 0
	seen := map[uint64]struct{}{} // at-least-once contract: dedupe incrementals by (filenum,offset)
	for sc.Scan() {
		lineNo++
		var e ev
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			fmt.Fprintf(os.Stderr, "line %d: %v\n", lineNo, err)
			os.Exit(1)
		}
		if e.Phase == "heartbeat" || len(e.Args) == 0 {
			skipped++
			continue
		}
		if *dedupe && e.Phase == "incremental" {
			k := uint64(e.Source.Filenum)<<48 | e.Source.Offset
			if _, dup := seen[k]; dup {
				deduped++
				continue
			}
			seen[k] = struct{}{}
		}
		h := fnv.New32a()
		h.Write([]byte(e.Key))
		shards[int(h.Sum32())%*workers] <- e
		total++
	}
	for _, ch := range shards {
		close(ch)
	}
	wg.Wait()
	fmt.Printf("replay total=%d skipped=%d deduped=%d applied=%d errors=%d\n", total, skipped, deduped, counters.applied, counters.errs)
	for _, l := range counters.log {
		fmt.Println("ERR:", l)
	}
	if counters.errs > 0 {
		os.Exit(1)
	}
}
