// Command dumptsv exports the str:/hsh: universe keys of a pika master as
// TSV rows matching the Doris pipeline tables (k \t v | k \t f0..f3), for
// per-key comparison against SELECT output from Doris.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	out := flag.String("out", "source.tsv", "output TSV (rows prefixed by table tag)")
	flag.Parse()

	c, err := exp.Dial(*addr, 10*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer c.Close()

	var keys []string
	cur := "0"
	for {
		v, err := c.Do("SCAN", cur, "COUNT", "1000")
		if err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
		cur = v.Items[0].StrVal()
		for _, k := range v.Items[1].Items {
			keys = append(keys, k.StrVal())
		}
		if cur == "0" {
			break
		}
	}
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	n := 0
	for _, k := range keys {
		switch {
		case strings.HasPrefix(k, "str:"):
			v, err := c.Do("GET", k)
			if err != nil || v.IsNil() {
				continue
			}
			fmt.Fprintf(f, "str\t%s\t%s\n", k[4:], v.StrVal())
			n++
		case strings.HasPrefix(k, "hsh:"):
			v, err := c.Do("HGETALL", k)
			if err != nil || v.IsNil() {
				continue
			}
			vals := map[string]string{}
			items := v.Items
			for i := 0; i+1 < len(items); i += 2 {
				vals[items[i].StrVal()] = items[i+1].StrVal()
			}
			fmt.Fprintf(f, "hsh\t%s\t%s\t%s\t%s\t%s\n", k[4:], vals["f0"], vals["f1"], vals["f2"], vals["f3"])
			n++
		}
	}
	fmt.Printf("exported=%d\n", n)
}
