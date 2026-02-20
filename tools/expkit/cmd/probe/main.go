// Command probe writes deliberately-shaped fresh keys during the scan
// window so fold-coverage defects surface precisely:
//
//	pm2:<n> + pm2b:<n>  via one MSET   (multi-key: only argv[1] can be folded)
//	psx:<n>             via SETEX      (binlog cmd = pksetexat)
//	pup:<n>             via HSET       (control: must converge)
//	pdel:<n>            HSET then DEL  (deleted-in-window control)
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	stopFile := flag.String("stop", "probe-stop", "stop when this file appears")
	tick := flag.Duration("tick", 20*time.Millisecond, "interval between probe groups")
	flag.Parse()

	c, err := exp.Dial(*addr, exp.DIAL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	n := 0
	t0 := time.Now()
	for {
		if _, err := os.Stat(*stopFile); err == nil {
			break
		}
		s := strconv.Itoa(n)
		cmds := [][]string{
			{"MSET", "pm2:" + s, "V1", "pm2b:" + s, "V2"},
			{"SETEX", "psx:" + s, "7200", "S" + s},
			{"HSET", "pup:" + s, "f", "u" + s},
			{"HSET", "pdel:" + s, "f", "d" + s},
			{"DEL", "pdel:" + s},
		}
		reps, err := c.Pipeline(cmds)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pipeline:", err)
			os.Exit(1)
		}
		for _, rp := range reps {
			if rp.T == '-' {
				fmt.Fprintf(os.Stderr, "probe err: %s\n", rp.S)
			}
		}
		n++
		time.Sleep(*tick)
	}
	fmt.Printf("probe groups=%d elapsed=%s\n", n, time.Since(t0).Round(time.Second))
}
