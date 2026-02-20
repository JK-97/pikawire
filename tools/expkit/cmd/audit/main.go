// Command audit joins the independent binlog capture (ground truth) with
// the product's event stream and checks the consistency contract:
//
//	A1 every GT entry with pb-end > H appears exactly once as an
//	   incremental event, with matching cmd/key, in increasing pos order.
//	A2 no incremental event has pb-end <= H (window must be folded, not emitted).
//	A3 every key of a GT entry in (L0, H] is covered by a correction DEL event.
//	A4 correction events all share one anchor H.
//	A5 GT stream is gap-free and monotonic (capture integrity).
//
// Both files are stream-ordered; the join is a merge cursor with O(1) memory
// beyond the correction key set.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

type pos struct {
	f uint32
	o uint64
}

func (a pos) after(b pos) bool    { return a.f > b.f || (a.f == b.f && a.o > b.o) }
func (a pos) equal(b pos) bool    { return a.f == b.f && a.o == b.o }
func (a pos) String() string      { return fmt.Sprintf("%d:%d", a.f, a.o) }
func (a pos) notAfter(b pos) bool { return !a.after(b) }

type gtRec struct {
	start, end pos
	cmd, key   string
}

func streamGT(path string, fn func(gtRec) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var r struct {
			F   uint32 `json:"f"`
			O   uint64 `json:"o"`
			PE  uint32 `json:"pef"`
			PEO uint64 `json:"peo"`
			Cmd string `json:"cmd"`
			Key string `json:"key"`
			Bad string `json:"bad"`
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return fmt.Errorf("gt line: %w", err)
		}
		if r.Bad != "" {
			continue
		}
		g := gtRec{start: pos{r.F, r.O}, end: pos{r.PE, r.PEO}, cmd: strings.ToUpper(r.Cmd), key: r.Key}
		if g.end.f == 0 && g.end.o == 0 {
			g.end = g.start
		}
		if err := fn(g); err != nil {
			return err
		}
	}
	return sc.Err()
}

type incItem struct {
	ok       bool
	p        pos
	cmd, key string
}

// streamInc walks incremental events in order via a pull iterator.
type incIter struct {
	sc       *bufio.Scanner
	cur      incItem
	done     bool
	total    int
	orderBad int
}

func newIncIter(path string) (*incIter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	return &incIter{sc: sc}, nil
}

func (it *incIter) next() {
	for it.sc.Scan() {
		var e struct {
			Phase   string `json:"phase"`
			Key     string `json:"key"`
			Command string `json:"command"`
			Source  struct {
				Filenum uint32 `json:"filenum"`
				Offset  uint64 `json:"offset"`
			} `json:"source"`
		}
		if err := json.Unmarshal(it.sc.Bytes(), &e); err != nil {
			it.cur = incItem{}
			it.done = true
			return
		}
		if e.Phase != "incremental" {
			continue
		}
		it.total++
		p := pos{e.Source.Filenum, e.Source.Offset}
		if it.cur.ok && !p.after(it.cur.p) {
			it.orderBad++
		}
		it.cur = incItem{ok: true, p: p, cmd: strings.ToUpper(e.Command), key: e.Key}
		return
	}
	it.cur = incItem{}
	it.done = true
}

func main() {
	gtPath := flag.String("gt", "binlogcat.jsonl", "ground-truth capture")
	evPath := flag.String("events", "events.jsonl", "product event stream")
	l0 := flag.String("l0", "", "session start filenum:offset")
	hOver := flag.String("h", "", "override H (filenum:offset)")
	flag.Parse()
	l0p := parsePos(*l0)

	// ---- pass 1: events -> H, correction key set, counts ----
	ef, err := os.Open(*evPath)
	if err != nil {
		die(err)
	}
	var h pos
	hFound := false
	var firstInc *pos
	corrKeys := map[string]struct{}{}
	var corrEvents, snapEvents, totalEvents, anchorDrift, phaseOrderBad int
	sawIncremental := false
	sc := bufio.NewScanner(ef)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var e struct {
			Phase   string `json:"phase"`
			Key     string `json:"key"`
			Command string `json:"command"`
			Source  struct {
				Filenum uint32 `json:"filenum"`
				Offset  uint64 `json:"offset"`
			} `json:"source"`
		}
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			ef.Close()
			die(err)
		}
		totalEvents++
		p := pos{e.Source.Filenum, e.Source.Offset}
		switch e.Phase {
		case "snapshot":
			snapEvents++
		case "correction":
			corrEvents++
			if !hFound {
				if *hOver != "" {
					h, hFound = parsePos(*hOver), true
				} else {
					h, hFound = p, true
				}
			}
			if hFound && !p.equal(h) {
				anchorDrift++
			}
			if strings.EqualFold(e.Command, "DEL") {
				corrKeys[e.Key] = struct{}{}
			}
			if sawIncremental {
				phaseOrderBad++
			}
		case "incremental":
			if firstInc == nil {
				cp := p
				firstInc = &cp
			}
			sawIncremental = true
		}
	}
	ef.Close()
	if !hFound {
		if firstInc != nil {
			h = *firstInc
			hFound = true
			fmt.Println("note: no correction events; H = first incremental event start (folded window empty)")
		} else {
			die(fmt.Errorf("cannot determine H: no correction and no incremental events"))
		}
	}

	// ---- pass 2: merge join GT (ordered) x inc events (ordered) ----
	it, err := newIncIter(*evPath)
	if err != nil {
		die(err)
	}
	it.next()

	var missing, ghost, mismatched, foldedUncorrected, extra int
	var gtN, gtBadDup, gaps, tailPending int64
	var missingSample, uncorrSample, ghostSample, mismatchSample, gapSample, extraSample []string
	var lastEnd pos
	var seenLast map[string]bool // duplicate-start detector within GT (small)
	_ = seenLast
	var prevStart pos
	havePrev := false

	err = streamGT(*gtPath, func(g gtRec) error {
		gtN++
		if havePrev {
			if g.start.equal(prevStart) {
				gtBadDup++ // redelivery after capture resume
			} else if g.start.f == prevStart.f && !g.start.equal(lastEnd) && g.start.after(prevStart) {
				// same filenum: starts must be exactly adjacent (prev entry's pb-end)
				if !g.start.equal(lastEnd) {
					gaps++
					if len(gapSample) < 10 {
						gapSample = append(gapSample, fmt.Sprintf("gap %d:%d..%d:%d", lastEnd.f, lastEnd.o, g.start.f, g.start.o))
					}
				}
			}
		}
		prevStart = g.start
		lastEnd = g.end

		if !it.cur.ok {
			tailPending++ // delivered stream ended before GT (capture ahead)
			return nil
		}
		// drain any inc events that precede this GT entry (should be none)
		for it.cur.ok && it.cur.p.after(g.start) == false && it.cur.p.equal(g.start) == false {
			extra++
			if len(extraSample) < 10 {
				extraSample = append(extraSample, fmt.Sprintf("event-without-gt %s %s", it.cur.p, it.cur.cmd))
			}
			it.next()
		}
		if g.end.notAfter(h) { // folded window
			if it.cur.ok && it.cur.p.equal(g.start) {
				ghost++
				if len(ghostSample) < 10 {
					ghostSample = append(ghostSample, fmt.Sprintf("ghost %s %s key=%s", g.start, g.cmd, g.key))
				}
				it.next()
			}
			if _, ok := corrKeys[g.key]; !ok {
				foldedUncorrected++
				if len(uncorrSample) < 30 {
					uncorrSample = append(uncorrSample, fmt.Sprintf("%s %s key=%s", g.start, g.cmd, g.key))
				}
			}
			return nil
		}
		// beyond H: must match the next incremental event exactly
		if !it.cur.ok {
			tailPending++
			return nil
		}
		if !it.cur.p.equal(g.start) {
			missing++
			if len(missingSample) < 30 {
				missingSample = append(missingSample, fmt.Sprintf("%s %s key=%s (next-event %s %s)", g.start, g.cmd, g.key, it.cur.p, it.cur.cmd))
			}
			return nil
		}
		if it.cur.cmd != g.cmd || it.cur.key != g.key {
			mismatched++
			if len(mismatchSample) < 10 {
				mismatchSample = append(mismatchSample, fmt.Sprintf("%s gt=%s/%s ev=%s/%s", g.start, g.cmd, g.key, it.cur.cmd, it.cur.key))
			}
		}
		it.next()
		return nil
	})
	if err != nil {
		die(err)
	}
	for ; it.cur.ok; it.next() {
		extra++
		if len(extraSample) < 10 {
			extraSample = append(extraSample, fmt.Sprintf("event-without-gt %s %s", it.cur.p, it.cur.cmd))
		}
	}

	fmt.Printf("== audit ==\n")
	fmt.Printf("H=%s  L0=%s\n", h, l0p)
	fmt.Printf("gt entries=%d bad/dup=%d capture-gaps=%d\n", gtN, gtBadDup, gaps)
	fmt.Printf("events total=%d snapshot=%d correction=%d incremental=%d\n", totalEvents, snapEvents, corrEvents, it.total)
	fmt.Printf("tail-pending (GT entries beyond delivered stream) = %d\n", tailPending)
	fmt.Printf("A1 missing incremental        = %d\n", missing)
	fmt.Printf("A2 ghost incremental (<=H)    = %d\n", ghost)
	fmt.Printf("A3 folded keys w/o correction = %d\n", foldedUncorrected)
	fmt.Printf("A4 anchor drift=%d correction-after-incremental=%d\n", anchorDrift, phaseOrderBad)
	fmt.Printf("A5 capture gaps               = %d %v\n", gaps, gapSample)
	fmt.Printf("cmd/key mismatch              = %d\n", mismatched)
	fmt.Printf("incremental events w/o GT     = %d\n", extra)
	fmt.Printf("incremental out-of-order      = %d\n", it.orderBad)
	for _, s := range missingSample {
		fmt.Println("MISSING:", s)
	}
	for _, s := range uncorrSample {
		fmt.Println("UNCORRECTED-FOLD:", s)
	}
	for _, s := range mismatchSample {
		fmt.Println("MISMATCH:", s)
	}
	for _, s := range ghostSample {
		fmt.Println("GHOST:", s)
	}
	for _, s := range extraSample {
		fmt.Println("EXTRA:", s)
	}
	fail := missing > 0 || ghost > 0 || foldedUncorrected > 0 || mismatched > 0 || extra > 0 || it.orderBad > 0 || gaps > 0 || anchorDrift > 0 || phaseOrderBad > 0
	if fail {
		fmt.Println("AUDIT: FAIL")
		os.Exit(1)
	}
	fmt.Println("AUDIT: PASS")
}

func parsePos(s string) pos {
	if s == "" {
		return pos{}
	}
	parts := strings.SplitN(s, ":", 2)
	var f, o uint64
	fmt.Sscanf(parts[0], "%d", &f)
	fmt.Sscanf(parts[1], "%d", &o)
	return pos{uint32(f), o}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
