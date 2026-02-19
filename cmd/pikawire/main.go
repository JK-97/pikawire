// Command pikawire streams PikiwiDB changes (full + incremental) as a
// mode-A raw event stream into Kafka (or a file for debugging).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jk-97/pikawire/internal/app"
)

var version = "dev" // set via -ldflags

func main() {
	var (
		cfgPath = flag.String("c", "", "path to YAML config file (required)")
		showVer = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("pikawire", version)
		return
	}
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: pikawire -c config.yaml")
		os.Exit(2)
	}

	cfg, err := app.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("pikawire starting", "version", version, "source", cfg.SourceID(), "checkpoint", cfg.Checkpoint)
	if err := app.RunVersion(ctx, cfg, log, version); err != nil && ctx.Err() == nil {
		log.Error("pikawire exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("pikawire stopped")
}
