// Command ordchk analyzes the incremental event stream for order violations:
// global position monotonicity and, critically, PER-KEY order (the property
// the consistency contract depends on).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type pos struct {
	f uint32
	o uint64
}

func main() {
	evPath := flag.String("events", "events.jsonl", "product event stream")
	sample := flag.Int("sample", 5, "per-key violation samples")
	flag.Parse()

	f, err := os.Open(*evPath)
	if err != nil {
		die(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)

	lastGlobal := pos{}
	var globalBad, total int
	keyLast := map[string]pos{}
	keyViol := map[string]int{}
	var firstBadLines []int
	lineNo := 0
	for sc.Scan() {
		var e struct {
			Phase  string `json:"phase"`
			Key    string `json:"key"`
			Op     string `json:"op"`
			Source struct {
				Filenum uint32 `json:"filenum"`
				Offset  uint64 `json:"offset"`
			} `json:"source"`
		}
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			die(err)
		}
		if e.Phase != "incremental" {
			continue
		}
		lineNo++
		total++
		p := pos{e.Source.Filenum, e.Source.Offset}
		if total > 1 && !(p.f > lastGlobal.f || (p.f == lastGlobal.f && p.o > lastGlobal.o)) {
			globalBad++
			if len(firstBadLines) < *sample {
				firstBadLines = append(firstBadLines, lineNo)
			}
		}
		lastGlobal = p
		if lp, ok := keyLast[e.Key]; ok {
			if !(p.f > lp.f || (p.f == lp.f && p.o > lp.o)) {
				keyViol[e.Key]++
			}
		}
		keyLast[e.Key] = p
	}
	fmt.Printf("incremental events=%d global-order-violations=%d (first at events #%v)\n", total, globalBad, firstBadLines)
	nKeys := len(keyViol)
	sum := 0
	worst := [][2]int64{}
	for k, v := range keyViol {
		sum += v
		worst = append(worst, [2]int64{int64(v), 0})
		_ = k
	}
	fmt.Printf("keys with per-key order violations=%d total-violations=%d\n", nKeys, sum)
	if nKeys > 0 {
		// rescan top offenders cheaply: print a few sample keys + their violation counts
		i := 0
		for k, v := range keyViol {
			if i >= *sample {
				break
			}
			fmt.Printf("  key=%s violations=%d\n", k, v)
			i++
		}
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
