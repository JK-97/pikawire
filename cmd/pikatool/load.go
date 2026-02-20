package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jk-97/pikawire/internal/redisclient"
)

// loadWorkers write synthetic keys with pipelined batches, used to build the
// 1M/10M/50M scale-test datasets quickly.
func load(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	host := fs.String("host", "127.0.0.1", "server host")
	port := fs.Int("port", 9221, "server port")
	passwd := fs.String("password", "", "auth")
	keys := fs.Int64("keys", 1000000, "total keys to write")
	workers := fs.Int("workers", 8, "concurrent writers")
	batch := fs.Int("batch", 512, "pipelined commands per round")
	prefix := fs.String("prefix", "bench:", "key prefix")
	vsize := fs.Int("value", 32, "value size bytes")
	hashEvery := fs.Int("hash-every", 0, "write bench:h<i> hash every N keys (0=string only)")
	listEvery := fs.Int("list-every", 0, "write l<i> list every N keys (3 elems)")
	setEvery := fs.Int("set-every", 0, "write s<i> set every N keys (3 members)")
	zsetEvery := fs.Int("zset-every", 0, "write z<i> zset every N keys (3 members with scores)")
	_ = fs.Parse(args)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	var done atomic.Int64
	val := func(k int64) []byte {
		v := make([]byte, *vsize)
		b := strconv.AppendUint(v[:0], uint64(k), 10)
		for i := len(b); i < *vsize; i++ {
			v[i] = 'x'
		}
		return v
	}
	valSuffix := func(k int64, tag byte) []byte {
		// printable deterministic element/member: "<tag><k>"
		return []byte(string(tag) + strconv.FormatInt(k, 10))
	}
	var wg sync.WaitGroup
	w := *workers
	if int64(w) > *keys {
		w = int(*keys)
	}
	errCh := make(chan error, w)
	start := time.Now()
	for i := 0; i < w; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := redisclient.Dial(ctx, addr, *passwd, 5*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			defer c.Close()
			cmds := make([][][]byte, 0, *batch)
			// Deterministic stripes: worker id owns windows id, id+w, ... of
			// `batch` keys each, so every key is claimed exactly once no
			// matter how many commands a window expands to.
			for r := int64(id); ; r += int64(w) {
				lo := r * int64(*batch)
				if lo >= *keys {
					return
				}
				hi := lo + int64(*batch)
				if hi > *keys {
					hi = *keys
				}
				cmds = cmds[:0]
				for k := lo; k < hi; k++ {
					key := *prefix + strconv.FormatInt(k, 10)
					isHash := *hashEvery > 0 && k%int64(*hashEvery) == 0
					if !isHash {
						cmds = append(cmds, [][]byte{[]byte("SET"), []byte(key), val(k)})
					} else {
						// PikiwiDB HSET accepts exactly one field per call.
						cmds = append(cmds,
							[][]byte{[]byte("HSET"), []byte("h" + key), []byte("f1"), val(k)},
							[][]byte{[]byte("HSET"), []byte("h" + key), []byte("f2"), valSuffix(k, 'f')})
					}
					if *listEvery > 0 && k%int64(*listEvery) == 0 {
						e := []string{"L" + strconv.FormatInt(k, 10) + "0", "L" + strconv.FormatInt(k, 10) + "1", "L" + strconv.FormatInt(k, 10) + "2"}
						cmd := [][]byte{[]byte("RPUSH"), []byte("l" + key)}
						for _, x := range e {
							cmd = append(cmd, []byte(x))
						}
						cmds = append(cmds, cmd)
					}
					if *setEvery > 0 && k%int64(*setEvery) == 0 {
						m := []string{"S" + strconv.FormatInt(k, 10) + "0", "S" + strconv.FormatInt(k, 10) + "1", "S" + strconv.FormatInt(k, 10) + "2"}
						cmd := [][]byte{[]byte("SADD"), []byte("s" + key)}
						for _, x := range m {
							cmd = append(cmd, []byte(x))
						}
						cmds = append(cmds, cmd)
					}
					if *zsetEvery > 0 && k%int64(*zsetEvery) == 0 {
						cmd := [][]byte{[]byte("ZADD"), []byte("z" + key)}
						for s := 1; s <= 3; s++ {
							cmd = append(cmd, []byte(strconv.Itoa(s)), []byte("Z"+strconv.FormatInt(k, 10)+strconv.Itoa(s)))
						}
						cmds = append(cmds, cmd)
					}
				}
				done.Add(hi - lo)
				vals, err := c.Do(cmds)
				if err != nil {
					errCh <- fmt.Errorf("worker %d: %w", id, err)
					return
				}
				for i, v := range vals {
					if v.IsError() {
						errCh <- fmt.Errorf("worker %d: command %d rejected: %s", id, i, v.Text())
						return
					}
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}(i)
	}
	// progress
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				d := done.Load()
				rate := float64(d) / time.Since(start).Seconds()
				fmt.Fprintf(os.Stderr, "load %d/%d (%.0f keys/s)\n", d, *keys, rate)
			case <-stop:
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	close(errCh)
	if err := <-errCh; err != nil {
		return err
	}
	fmt.Printf("loaded %d keys in %s (%.0f keys/s)\n", *keys, time.Since(start).Round(time.Millisecond), float64(*keys)/time.Since(start).Seconds())
	return nil
}
