// Command pikatool is a debugging / inspection companion for Pikawire:
//
//	pikatool peek  - connect as a replication slave and print the binlog
//	                 stream as decoded events (CTRL-C to stop)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/replica"
	"github.com/jk-97/pikawire/internal/store"
)

var version = "dev"

func main() {
	debug := os.Getenv("PIKA_DEBUG") != ""
	lvl := slog.LevelInfo
	if debug {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "peek":
		err = peek(ctx, log, os.Args[2:])
	case "load":
		err = load(ctx, os.Args[2:])
	case "mutate":
		err = mutate(ctx, os.Args[2:])
	case "verifykafka":
		err = verifyKafka(ctx, os.Args[2:])
	case "fetchdump":
		err = fetchdump(ctx, log, os.Args[2:])
	case "version":
		fmt.Println("pikatool", version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		log.Error("pikatool failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pikatool <peek|load|mutate|verifykafka|fetchdump|version> [flags]\n  run 'pikatool <subcommand> -h' for flags")
}

func peek(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("peek", flag.ExitOnError)
	host := fs.String("host", "127.0.0.1", "master host")
	port := fs.Int("port", 9221, "master port")
	passwd := fs.String("password", "", "master auth")
	db := fs.String("db", "db0", "db name")
	filenum := fs.Uint("filenum", 0, "start filenum (0 = latest tip)")
	offset := fs.Uint64("offset", 0, "start offset")
	localIP := fs.String("local-ip", "127.0.0.1", "slave identity ip advertised to master")
	localPort := fs.Int("local-port", 31333, "slave identity port advertised to master")
	_ = fs.Parse(args)

	st, err := store.Open(os.TempDir()+"/pikawire-peek-checkpoint.json", "peek")
	if err != nil {
		return err
	}
	r := replica.NewRunner(replica.Config{
		MasterHost: *host, MasterPort: *port, Password: *passwd, DBName: *db,
		LocalIP: *localIP, LocalPort: *localPort, SourceID: fmt.Sprintf("%s:%d", *host, *port),
		Store:   st,
		StartAt: replica.Offset{Filenum: uint32(*filenum), Offset: *offset},
		Sink: sinkFunc(func(_ context.Context, ev *envelope.Event) error {
			b, _ := ev.MarshalJSON()
			fmt.Println(string(b))
			return nil
		}),
		Logger: log,
	})
	return r.Run(ctx)
}

type sinkFunc func(context.Context, *envelope.Event) error

func (f sinkFunc) Emit(ctx context.Context, ev *envelope.Event) error { return f(ctx, ev) }
