// Command compare diffs the full live state of two instances (source vs
// replayed target): key sets, types, and values for every key.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

type keySet = map[string]struct{}

func scanAll(addr string) (keySet, error) {
	c, err := exp.Dial(addr, exp.DIAL)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	out := keySet{}
	cur := "0"
	for {
		v, err := c.Do("SCAN", cur, "COUNT", "2000")
		if err != nil {
			return nil, err
		}
		if v.T != '*' || len(v.Items) != 2 {
			return nil, fmt.Errorf("bad scan reply")
		}
		cur = v.Items[0].S
		for _, k := range v.Items[1].Items {
			out[k.S] = struct{}{}
		}
		if cur == "0" {
			return out, nil
		}
	}
}

type diff struct {
	Key  string `json:"key"`
	Kind string `json:"kind"`
	Src  string `json:"src"`
	Tgt  string `json:"tgt"`
}

func main() {
	source := flag.String("source", "127.0.0.1:9221", "host:port")
	target := flag.String("target", "127.0.0.1:9222", "host:port")
	workers := flag.Int("workers", 6, "parallel comparators")
	out := flag.String("out", "compare-report.json", "report file")
	flag.Parse()

	fmt.Println("scanning source...")
	src, err := scanAll(*source)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("scanning target...")
	tgt, err := scanAll(*target)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var onlySrc, onlyTgt []string
	common := make([]string, 0, len(src))
	for k := range src {
		if _, ok := tgt[k]; ok {
			common = append(common, k)
		} else {
			onlySrc = append(onlySrc, k)
		}
	}
	for k := range tgt {
		if _, ok := src[k]; !ok {
			onlyTgt = append(onlyTgt, k)
		}
	}
	sort.Strings(onlySrc)
	sort.Strings(onlyTgt)
	sort.Strings(common)
	fmt.Printf("keys: src=%d tgt=%d common=%d onlySrc=%d onlyTgt=%d\n", len(src), len(tgt), len(common), len(onlySrc), len(onlyTgt))

	var mu sync.Mutex
	var diffs []diff
	perWorker := (len(common) + *workers - 1) / *workers
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		lo := w * perWorker
		hi := min(lo+perWorker, len(common))
		if lo >= hi {
			break
		}
		wg.Add(1)
		go func(keys []string) {
			defer wg.Done()
			cs, err := exp.Dial(*source, exp.DIAL)
			if err != nil {
				mu.Lock()
				diffs = append(diffs, diff{Kind: "dial-src", Src: err.Error()})
				mu.Unlock()
				return
			}
			defer cs.Close()
			ct, err := exp.Dial(*target, exp.DIAL)
			if err != nil {
				mu.Lock()
				diffs = append(diffs, diff{Kind: "dial-tgt", Src: err.Error()})
				mu.Unlock()
				return
			}
			defer ct.Close()
			for off := 0; off < len(keys); off += 100 {
				end := min(off+100, len(keys))
				b := keys[off:end]
				if err := compareBatch(cs, ct, b, &mu, &diffs); err != nil {
					mu.Lock()
					diffs = append(diffs, diff{Key: b[0], Kind: "batch-err", Src: err.Error()})
					mu.Unlock()
					return
				}
			}
		}(common[lo:hi])
	}
	wg.Wait()

	rep := struct {
		SrcKeys    int      `json:"src_keys"`
		TgtKeys    int      `json:"tgt_keys"`
		Common     int      `json:"common"`
		OnlySrc    []string `json:"only_src_sample"`
		OnlyTgt    []string `json:"only_tgt_sample"`
		OnlySrcN   int      `json:"only_src_n"`
		OnlyTgtN   int      `json:"only_tgt_n"`
		ValueDiffs int      `json:"value_diffs"`
		Diffs      []diff   `json:"diffs_sample"`
	}{len(src), len(tgt), len(common), capSlice(onlySrc, 50), capSlice(onlyTgt, 50), len(onlySrc), len(onlyTgt), 0, capSlice2(diffs, 100)}
	vd := 0
	for _, d := range diffs {
		if d.Kind != "batch-err" && d.Kind != "dial-src" && d.Kind != "dial-tgt" {
			vd++
		}
	}
	rep.ValueDiffs = vd
	j, _ := json.MarshalIndent(rep, "", " ")
	_ = os.WriteFile(*out, j, 0o644)
	fmt.Printf("value/type diffs=%d only_src=%d only_tgt=%d (report: %s)\n", vd, len(onlySrc), len(onlyTgt), *out)
	for i, d := range diffs {
		if i >= 20 {
			fmt.Println("...")
			break
		}
		fmt.Printf("DIFF %-14s key=%-30s src=%-40.40s tgt=%-40.40s\n", d.Kind, d.Key, d.Src, d.Tgt)
	}
	if vd > 0 || len(onlySrc) > 0 || len(onlyTgt) > 0 {
		os.Exit(1)
	}
}

func capSlice(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func capSlice2(s []diff, n int) []diff {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func compareBatch(cs, ct *exp.Client, keys []string, mu *sync.Mutex, diffs *[]diff) error {
	// pass 1: types on both sides
	typeCmds := make([][]string, 0, len(keys)*2)
	for _, k := range keys {
		typeCmds = append(typeCmds, []string{"TYPE", k})
	}
	tsS, err := cs.Pipeline(typeCmds)
	if err != nil {
		return err
	}
	tsT, err := ct.Pipeline(typeCmds)
	if err != nil {
		return err
	}
	// pass 2: value reads keyed by source type
	rcS := make([][]string, len(keys))
	rcT := make([][]string, len(keys))
	live := make([]bool, len(keys))
	for i, k := range keys {
		stype := tsS[i].S
		ttype := tsT[i].S
		if stype != ttype {
			mu.Lock()
			*diffs = append(*diffs, diff{Key: k, Kind: "type", Src: stype, Tgt: ttype})
			mu.Unlock()
			continue
		}
		if stype == "none" {
			continue
		}
		rc, ok := readFor(stype, k)
		if !ok {
			mu.Lock()
			*diffs = append(*diffs, diff{Key: k, Kind: "unsupported-type", Src: stype})
			mu.Unlock()
			continue
		}
		rcS[i], rcT[i], live[i] = rc, rc, true
	}
	flat := make([][]string, 0, len(keys)*2)
	_ = flat
	// source reads then target reads (separate connections, batched)
	vals := make([][2]exp.Val, len(keys))
	scmds, tcmds := make([][]string, 0, len(keys)), make([][]string, 0, len(keys))
	smap := make([]int, 0, len(keys))
	for i := range keys {
		if live[i] {
			scmds = append(scmds, rcS[i])
			tcmds = append(tcmds, rcT[i])
			smap = append(smap, i)
		}
	}
	rs, err := cs.Pipeline(scmds)
	if err != nil {
		return err
	}
	rt, err := ct.Pipeline(tcmds)
	if err != nil {
		return err
	}
	for j, i := range smap {
		vals[i] = [2]exp.Val{rs[j], rt[j]}
	}
	for i := range keys {
		if !live[i] {
			continue
		}
		stype := tsS[i].S
		s, t := vals[i][0], vals[i][1]
		if stype == "string" {
			if !strEq(s, t) {
				add(mu, diffs, diff{Key: keys[i], Kind: "value-string", Src: digest(s), Tgt: digest(t)})
			}
			continue
		}
		if stype == "hash" || stype == "set" || stype == "zset" || stype == "list" {
			if !arrEq(stype, s, t) {
				add(mu, diffs, diff{Key: keys[i], Kind: "value-" + stype, Src: digest(s), Tgt: digest(t)})
			}
			continue
		}
	}
	return nil
}

func add(mu *sync.Mutex, d *[]diff, x diff) {
	mu.Lock()
	*d = append(*d, x)
	mu.Unlock()
}

func readFor(typ, k string) ([]string, bool) {
	switch typ {
	case "string":
		return []string{"GET", k}, true
	case "hash":
		return []string{"HGETALL", k}, true
	case "set":
		return []string{"SMEMBERS", k}, true
	case "zset":
		return []string{"ZRANGE", k, "0", "-1", "WITHSCORES"}, true
	case "list":
		return []string{"LRANGE", k, "0", "-1"}, true
	}
	return nil, false
}

func strEq(a, b exp.Val) bool {
	if a.Nil && b.Nil {
		return true
	}
	return !a.Nil && !b.Nil && a.S == b.S
}

// arrEq compares collection replies canonically: hash/set/zset as (multi)sets
// (zset: member-score pairs), list element-order sensitive.
func arrEq(typ string, a, b exp.Val) bool {
	as, bs := norm(typ, a), norm(typ, b)
	return as == bs
}

func norm(typ string, v exp.Val) string {
	if v.T != '*' {
		return "ERR:" + v.S
	}
	items := make([]string, 0, len(v.Items))
	switch typ {
	case "list":
		for _, it := range v.Items {
			items = append(items, it.S)
		}
	case "hash":
		for i := 0; i+1 < len(v.Items); i += 2 {
			items = append(items, v.Items[i].S+"="+v.Items[i+1].S)
		}
	case "set":
		for _, it := range v.Items {
			items = append(items, it.S)
		}
	case "zset":
		for i := 0; i+1 < len(v.Items); i += 2 {
			m, sc := v.Items[i].S, v.Items[i+1].S
			f, err := strconv.ParseFloat(sc, 64)
			if err == nil {
				sc = strconv.FormatFloat(f, 'g', -1, 64)
			}
			items = append(items, m+":"+sc)
		}
	}
	if typ != "list" {
		sort.Strings(items)
	}
	return fmt.Sprint(items)
}

func digest(v exp.Val) string {
	if v.T == '-' {
		return "E:" + v.S
	}
	if v.T != '*' {
		if v.Nil {
			return "<nil>"
		}
		return v.S
	}
	var sb strings.Builder
	for _, it := range v.Items {
		sb.WriteString(it.S)
		sb.WriteByte(0)
	}
	s := sb.String()
	if len(s) > 80 {
		return fmt.Sprintf("%d:%s…", len(s), s[:40])
	}
	return s
}
