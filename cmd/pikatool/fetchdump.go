package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"os"
	"path/filepath"

	"github.com/jk-97/pikawire/internal/dbsync"
	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/replica"
	"github.com/jk-97/pikawire/internal/store"
)

// fetchdump exercises the DBSync + rsync protocol path standalone:
// handshake, master bgsave, dump transfer, bgsave-position parse.
func fetchdump(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("fetchdump", flag.ExitOnError)
	host := fs.String("host", "127.0.0.1", "master host")
	port := fs.Int("port", 9221, "master redis port (rsync service is +10001)")
	passwd := fs.String("password", "", "auth")
	freshBgsave := fs.Bool("fresh-bgsave", false, "force a fresh master BGSAVE before DBSync (probe the full path)")
	db := fs.String("db", "db0", "db name")
	out := fs.String("out", "./pikawire-dump", "dump output root")
	localPort := fs.Int("local-port", 21350, "slave identity port")
	_ = fs.Parse(args)

	stubStore := tempStore()
	r := replica.NewRunner(replica.Config{
		MasterHost: *host, MasterPort: *port, Password: *passwd, DBName: *db,
		LocalIP: "127.0.0.1", LocalPort: *localPort, SourceID: fmt.Sprintf("%s:%d", *host, *port),
		Store: stubStore, Sink: nopSink{}, Logger: log,
	})
	t0 := time.Now()
	dir, pos, err := dbsync.Fetch(ctx, r, *out, *host, *port, *passwd, replica.Offset{}, *freshBgsave, nil, log)
	if err != nil {
		return err
	}
	fmt.Printf("dump at %s\nbgsave anchor filenum=%d offset=%d (took %s)\nreader built-in: %v\n", dir, pos.Filenum, pos.Offset, time.Since(t0).Round(time.Millisecond), dbsync.ReaderAvailable())
	return nil
}

type nopSink struct{}

func (nopSink) Emit(context.Context, *envelope.Event) error { return nil }

func tempStore() *store.Store {
	f := filepath.Join(os.TempDir(), "pikawire-fetchdump.checkpoint.json")
	_ = os.Remove(f)
	st, err := store.Open(f, "fetchdump")
	if err != nil {
		panic(err)
	}
	return st
}
