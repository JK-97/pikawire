// Command admin runs arbitrary RESP commands against an instance, plus a
// probe mode that checks which commands the server actually supports.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jk-97/pikawire/tools/expkit/exp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9221", "host:port")
	probe := flag.Bool("probe", false, "probe command support instead of executing")
	flag.Parse()
	c, err := exp.Dial(*addr, exp.DIAL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()

	if *probe {
		scratch := "__probe__"
		// seed all type scratch keys, then run each candidate once
		c.Do("SET", scratch, "1")
		c.Do("LPUSH", scratch+":l", "a")
		c.Do("SADD", scratch+":s", "a")
		c.Do("HSET", scratch+":h", "f", "v")
		c.Do("ZADD", scratch+":z", "1", "a")
		cands := [][]string{
			{"GET", scratch}, {"SETEX", scratch, "7200", "x"}, {"APPEND", scratch, "y"},
			{"INCR", scratch}, {"GETSET", scratch, "z"}, {"LPUSH", scratch + ":l", "b"},
			{"RPUSH", scratch + ":l", "c"}, {"LPOP", scratch + ":l"}, {"RPOP", scratch + ":l"},
			{"LTRIM", scratch + ":l", "0", "-1"}, {"LREM", scratch + ":l", "1", "b"},
			{"SREM", scratch + ":s", "a"}, {"SMEMBERS", scratch + ":s"},
			{"HDEL", scratch + ":h", "f"}, {"HMSET", scratch + ":h", "a", "1"},
			{"ZREM", scratch + ":z", "a"}, {"ZSCORE", scratch + ":z", "a"},
			{"MSET", scratch + ":m1", "1", scratch + ":m2", "2"},
			{"RENAME", scratch + ":m1", scratch + ":m3"},
			{"EXPIRE", scratch, "7200"}, {"PERSIST", scratch},
			{"PKSCANRANGE", "string", "", "", "LIMIT", "2"},
			{"SCAN", "0", "COUNT", "2"}, {"TYPE", scratch}, {"PTTL", scratch},
		}
		for _, cmd := range cands {
			v, err := c.Do(cmd...)
			status := "OK"
			if err == nil && v.T == '-' {
				if strings.Contains(strings.ToUpper(v.S), "UNKNOWN") || strings.Contains(v.S, "not support") {
					status = "UNSUPPORTED"
				} else {
					status = "supported(err:" + strings.SplitN(v.S, " ", 3)[0] + ")"
				}
			} else if err != nil {
				status = "CONN:" + err.Error()
			}
			fmt.Printf("%-40s %s\n", strings.Join(cmd, " "), status)
		}
		c.Do("DEL", scratch, scratch+":l", scratch+":s", scratch+":h", scratch+":z", scratch+":m2", scratch+":m3")
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: admin -addr host:port COMMAND ARGS...")
		os.Exit(2)
	}
	v, err := c.Do(args...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printVal(v)
}

func printVal(v exp.Val) {
	switch v.T {
	case '*':
		for _, it := range v.Items {
			printVal(it)
		}
	case '$':
		if v.Nil {
			fmt.Println("(nil)")
		} else {
			fmt.Println(v.S)
		}
	case ':':
		fmt.Println(v.I)
	default:
		fmt.Println(v.S)
	}
}
