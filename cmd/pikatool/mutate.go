package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jk-97/pikawire/internal/redisclient"
)

// mutate drives a sustained write storm over a seeded dataset while a full
// snapshot runs, covering the operation surface the pipeline must sync:
//
//	string : SET updates over [0,keys) (skip hash slots); DEL of [keys, keys+del)
//	hash   : HSET f3 + HDEL f2 on seeded h<prefix><k> (k % hash-every == 0)
//	list   : RPUSH an element (+ LPOP on alternating keys) on l<prefix><k>
//	set    : SADD a member (+ SREM on alternating keys) on s<prefix><k>
//	zset   : ZADD (+ ZREM on alternating keys) on z<prefix><k>
//	new typed keys beyond the seed range (hn/l/s/z<prefix>2000000000+i)
//
// Op choices are deterministic so the verifier can replay the topic and
// compare the final state against pika exactly.
func mutate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mutate", flag.ExitOnError)
	host := fs.String("host", "127.0.0.1", "server host")
	port := fs.Int("port", 9221, "server port")
	passwd := fs.String("password", "", "auth")
	prefix := fs.String("prefix", "bench:", "seed key prefix")
	keys := fs.Int64("keys", 100000, "string update window [0,keys)")
	del := fs.Int64("del", 0, "delete seeded strings in [keys, keys+del) at midpoint")
	hashEvery := fs.Int64("hash-every", 0, "seed hash stride (typed hash ops when >0)")
	listEvery := fs.Int64("list-every", 0, "seed list stride (typed list ops when >0)")
	setEvery := fs.Int64("set-every", 0, "seed set stride")
	zsetEvery := fs.Int64("zset-every", 0, "seed zset stride")
	newTyped := fs.Int64("new-typed", 0, "create this many new keys per enabled typed stride at a high index offset")
	workers := fs.Int("workers", 8, "concurrent writers")
	batch := fs.Int("batch", 256, "pipelined commands per round")
	dur := fs.Duration("dur", 60*time.Second, "storm duration")
	rate := fs.Int("rate", 0, "cap aggregate writes/sec (0 = unlimited)")
	_ = fs.Parse(args)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	var keySeq atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	w := *workers
	errCh := make(chan error, w+2)
	fail := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}
	dial := func() *redisclient.Client {
		c, err := redisclient.Dial(ctx, addr, *passwd, 5*time.Second)
		if err != nil {
			fail(err)
			return nil
		}
		return c
	}
	pipeline := func(c *redisclient.Client, cmds [][][]byte) bool {
		if c == nil || len(cmds) == 0 {
			return false
		}
		vals, err := c.Do(cmds)
		if err != nil {
			fail(fmt.Errorf("mutate: %w", err))
			return false
		}
		for i, v := range vals {
			if v.IsError() {
				fail(fmt.Errorf("mutate: command %d rejected: %s", i, v.Text()))
				return false
			}
		}
		return true
	}
	var valSeqN atomic.Int64
	uniq := func() []byte { return []byte(fmt.Sprintf("m%012d", valSeqN.Add(1))) }

	// continuous string SET updates over the update window
	for i := 0; i < w; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := dial()
			if c == nil {
				return
			}
			defer c.Close()
			cmds := make([][][]byte, 0, *batch)
			for time.Since(start) < *dur {
				cmds = cmds[:0]
				for j := 0; j < *batch; j++ {
					s := keySeq.Add(1)
					k := (s - 1) % *keys
					if *hashEvery > 0 && k%*hashEvery == 0 {
						continue
					}
					key := *prefix + strconv.FormatInt(k, 10)
					cmds = append(cmds, [][]byte{[]byte("SET"), []byte(key), uniq()})
				}
				if !pipeline(c, cmds) {
					return
				}
				// drift-free global pacing: keep the aggregate write count on
				// schedule with -rate (shared across all updater workers).
				if *rate > 0 {
					target := time.Duration(float64(valSeqN.Load()) / float64(*rate) * float64(time.Second))
					if d := start.Add(target).Sub(time.Now()); d > 0 {
						select {
						case <-time.After(d):
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}(i)
	}

	// one-shot op bursts at the midpoint (deletes + typed mutations)
	burst := func() {
		defer wg.Done()
		c := dial()
		if c == nil {
			return
		}
		defer c.Close()
		cmds := make([][][]byte, 0, 4096)
		flush := func() bool {
			ok := pipeline(c, cmds)
			cmds = cmds[:0]
			return ok
		}
		if *del > 0 {
			for i := *keys; i < *keys+*del; i++ {
				cmds = append(cmds, [][]byte{[]byte("DEL"), []byte(*prefix + strconv.FormatInt(i, 10))})
				if len(cmds) >= 1024 && !flush() {
					return
				}
			}
		}
		typed := func(step int64, prefixKey func(int64) string, makeCmds func(int64) [][][]byte) {
			for k := int64(0); k < *keys; k += step {
				_ = prefixKey(k)
				for _, c := range makeCmds(k) {
					cmds = append(cmds, c)
				}
				if len(cmds) >= 1024 && !flush() {
					return
				}
			}
		}
		if *hashEvery > 0 {
			typed(*hashEvery, func(k int64) string { return "h" + *prefix + strconv.FormatInt(k, 10) },
				func(k int64) [][][]byte {
					hkey := "h" + *prefix + strconv.FormatInt(k, 10)
					out := [][][]byte{{[]byte("HSET"), []byte(hkey), []byte("f3"), uniq()}}
					if (k / *hashEvery)%2 == 0 {
						out = append(out, [][]byte{[]byte("HDEL"), []byte(hkey), []byte("f2")})
					}
					return out
				})
		}
		if *listEvery > 0 {
			typed(*listEvery, func(k int64) string { return "l" + *prefix + strconv.FormatInt(k, 10) },
				func(k int64) [][][]byte {
					lkey := "l" + *prefix + strconv.FormatInt(k, 10)
					out := [][][]byte{{[]byte("RPUSH"), []byte(lkey), []byte("L" + strconv.FormatInt(k, 10) + "3")}}
					if (k / *listEvery)%2 == 1 {
						out = append(out, [][]byte{[]byte("LPOP"), []byte(lkey)})
					}
					return out
				})
		}
		if *setEvery > 0 {
			typed(*setEvery, func(k int64) string { return "s" + *prefix + strconv.FormatInt(k, 10) },
				func(k int64) [][][]byte {
					skey := "s" + *prefix + strconv.FormatInt(k, 10)
					out := [][][]byte{{[]byte("SADD"), []byte(skey), []byte("S" + strconv.FormatInt(k, 10) + "9")}}
					if (k / *setEvery)%2 == 0 {
						out = append(out, [][]byte{[]byte("SREM"), []byte(skey), []byte("S" + strconv.FormatInt(k, 10) + "0")})
					}
					return out
				})
		}
		if *zsetEvery > 0 {
			typed(*zsetEvery, func(k int64) string { return "z" + *prefix + strconv.FormatInt(k, 10) },
				func(k int64) [][][]byte {
					zkey := "z" + *prefix + strconv.FormatInt(k, 10)
					out := [][][]byte{{[]byte("ZADD"), []byte(zkey), []byte("9"), []byte("Z" + strconv.FormatInt(k, 10) + "9")}}
					if (k / *zsetEvery)%2 == 0 {
						out = append(out, [][]byte{[]byte("ZREM"), []byte(zkey), []byte("Z" + strconv.FormatInt(k, 10) + "1")})
					}
					return out
				})
		}
		// new typed keys created mid-scan (beyond the seeded index space)
		const newBase = int64(2000000000)
		if *newTyped > 0 {
			typ := [][2]any{}
			if *hashEvery > 0 {
				typ = append(typ, [2]any{"h", "nf"})
			}
			if *listEvery > 0 {
				typ = append(typ, [2]any{"l", "nL"})
			}
			if *setEvery > 0 {
				typ = append(typ, [2]any{"s", "nS"})
			}
			if *zsetEvery > 0 {
				typ = append(typ, [2]any{"z", "nZ"})
			}
			for _, t := range typ {
				tag := t[0].(string)
				mk := t[1].(string)
				for i := int64(0); i < *newTyped; i++ {
					key := tag + *prefix + strconv.FormatInt(newBase+i, 10)
					v := uniq()
					switch tag {
					case "h":
						cmds = append(cmds, [][]byte{[]byte("HSET"), []byte(key), []byte("nf1"), v})
					case "l":
						cmds = append(cmds, [][]byte{[]byte("RPUSH"), []byte(key), []byte(mk + strconv.FormatInt(newBase+i, 10) + "0"), v})
					case "s":
						cmds = append(cmds, [][]byte{[]byte("SADD"), []byte(key), []byte(mk + strconv.FormatInt(newBase+i, 10) + "0"), v})
					case "z":
						cmds = append(cmds, [][]byte{[]byte("ZADD"), []byte(key), []byte("1"), []byte(mk + strconv.FormatInt(newBase+i, 10) + "0"), []byte("2"), v})
					}
					if len(cmds) >= 1024 && !flush() {
						return
					}
				}
			}
		}
		if len(cmds) > 0 {
			flush()
		}
	}
	wg.Add(1)
	go func() {
		select {
		case <-time.After(*dur / 2):
			burst()
		case <-ctx.Done():
		}
	}()

	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return err
	}
	fmt.Printf("mutated %d writes in %s (%.0f writes/s)\n", valSeqN.Load(), time.Since(start).Round(time.Millisecond), float64(valSeqN.Load())/time.Since(start).Seconds())
	return nil
}
