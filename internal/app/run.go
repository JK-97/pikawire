package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/jk-97/pikawire/internal/dbsync"
	"github.com/jk-97/pikawire/internal/dorissink"
	"github.com/jk-97/pikawire/internal/envelope"
	"github.com/jk-97/pikawire/internal/info"
	"github.com/jk-97/pikawire/internal/metrics"
	"github.com/jk-97/pikawire/internal/replica"
	"github.com/jk-97/pikawire/internal/sink"
	"github.com/jk-97/pikawire/internal/sqlsink"
	"github.com/jk-97/pikawire/internal/store"
)

// Run executes the full pikiwidb -> sink pipeline until ctx is cancelled or
// a fatal error occurs. Semantics: mode A raw event stream, single topic.
func Run(ctx context.Context, cfg *Config, log *slog.Logger) error {
	return RunVersion(ctx, cfg, log, "dev")
}

// RunVersion is Run with build metadata for the metrics endpoint.
func RunVersion(ctx context.Context, cfg *Config, log *slog.Logger, version string) error {
	reg := metrics.New(version)
	recv := reg.Counter("pikawire_binlog_entries_received_total", "binlog entries consumed from the source")
	emitted := reg.Counter("pikawire_events_emitted_total", "events handed to the sink after acknowledgement")
	delivered := reg.Gauge("pikawire_delivered_offset", "delivered binlog offset (checkpoint)")
	lag := reg.Gauge("pikawire_source_lag_seconds", "now minus last observed source exec time")
	st, err := store.Open(cfg.Checkpoint, cfg.SourceID())
	if err != nil {
		return err
	}

	if cfg.MetricsAddr != "" {
		ln, err := net.Listen("tcp", cfg.MetricsAddr)
		if err != nil {
			return fmt.Errorf("app: metrics listener: %w", err)
		}
		srv := &http.Server{Handler: reg.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		defer srv.Close()
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for range t.C {
				delivered.Set(int64(st.State().Offset))
			}
		}()
	}

	sk, err := buildSink(ctx, cfg)
	if err != nil {
		return err
	}
	if c, ok := sk.(interface{ Close() error }); ok {
		defer c.Close()
	}
	if cfg.Pipeline != nil {
		log.Info("app: embedded SQL pipeline mode", "driver", cfg.Pipeline.Driver, "rules", len(cfg.Pipeline.Rules))
	}
	if ref := dbsync.ReaderRef(); ref != "" {
		log.Info("app: dbsync dump reader", "built_against", ref)
	}

	makeRunner := func(localPort int, startAt replica.Offset) *replica.Runner {
		return replica.NewRunner(replica.Config{
			StartAt:          startAt,
			MasterHost:       cfg.Source.Host,
			MasterPort:       cfg.Source.Port,
			Password:         cfg.Source.Password,
			DBName:           cfg.Source.DB,
			LocalIP:          cfg.Source.LocalIP,
			LocalPort:        localPort,
			SourceID:         cfg.SourceID(),
			Store:            st,
			Sink:             sk,
			AckEvery:         time.Duration(cfg.AckEvery),
			Heartbeat:        time.Duration(cfg.Heartbeat),
			BufferDir:        cfg.BufferDir,
			PendingBytesHigh: cfg.PendingBytesHigh,
			ReserveFreeBytes: cfg.ReserveFreeBytes,
			BufferDurable:    cfg.BufferFsync == "durable",
			Logger:           log,
			Metrics: &replica.Metrics{
				EntriesReceived: recv, EventsEmitted: emitted,
				DeliveredOffset: delivered, SourceLagSeconds: lag,
			},
		})
	}

	// incrementalLoop supervises the post-snapshot tail: a stalled session
	// (master ahead while we receive nothing) tears down and reconnects from
	// the checkpoint; the master replays the gap, so nothing is lost.
	incrementalLoop := func() error {
		for {
			err := makeRunner(cfg.Source.LocalPort, replica.Offset{}).Run(ctx)
			switch {
			case errors.Is(err, replica.ErrStalled):
				log.Warn("replica: sync stalled; reconnecting from checkpoint", "err", err)
				select {
				case <-time.After(2 * time.Second):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			case errors.Is(err, context.Canceled), err == nil:
				return nil
			default:
				return err
			}
		}
	}

	if st.State().Snapshot == store.PhaseDone {
		log.Info("app: resuming incremental from checkpoint", "filenum", st.State().Filenum, "offset", st.State().Offset)
		return incrementalLoop()
	}
	return runDbsync(ctx, cfg, log, st, makeRunner, incrementalLoop)
}

func buildSink(ctx context.Context, cfg *Config) (replica.Sink, error) {
	if pc := cfg.Pipeline; pc != nil {
		rules := make([]*sqlsink.RuleConfig, len(pc.Rules))
		for i := range pc.Rules {
			r := pc.Rules[i]
			rules[i] = &r
		}
		if pc.Driver == "doris" {
			return dorissink.New(dorissink.Config{
				Fenodes:        pc.Fenodes,
				User:           pc.User,
				Password:       pc.Password,
				Rules:          rules,
				MaxBatchRows:   pc.MaxBatchRows,
				FlushEvery:     time.Duration(pc.FlushEvery),
				SequenceColumn: pc.SequenceColumn,
			})
		}
		return sqlsink.New(sqlsink.Config{
			Driver:       pc.Driver,
			DSN:          pc.DSN,
			Rules:        rules,
			MaxBatchRows: pc.MaxBatchRows,
			FlushEvery:   time.Duration(pc.FlushEvery),
		})
	}
	if k := cfg.Sink.Kafka; k != nil {
		ks, err := sink.NewKafkaSink(ctx, sink.KafkaConfig{
			Brokers: k.Brokers, Topic: k.Topic, ClientID: k.ClientID,
			WorkersPerPart: k.WorkersPerPart, BatchSize: k.BatchSize,
			Compression: k.Compression,
		})
		if err != nil {
			return nil, err
		}
		ks.IncludeHeartbeats(k.IncludeHeartbeats)
		return ks, nil
	}
	fs, err := sink.NewFileSink(cfg.Sink.File.Path)
	if err != nil {
		return nil, err
	}
	fs.IncludeHeartbeats(cfg.Sink.File.IncludeHeartbeats)
	return fs, nil
}

// runDbsync performs the full path in anchor-first order ("B'"):
//
//  1. DBSync handshake (fast): the master validates its own checkpoint —
//     it re-bgsaves when no dump exists, when the dump's anchor binlog
//     file was purged, or when the gap exceeds kDBSyncMaxGap (50 files).
//  2. rsync meta + the small info file ONLY -> the anchor is now known.
//  3. open the PB replication session with TrySync(anchor) — from this
//     moment every entry >= anchor is consumed (into the disk backlog) and
//     acked, so correctness no longer depends on master-side retention;
//     the DBSync connection stays open through the bulk transfer, which
//     additionally freezes master purge while a slave is in DbSync state.
//  4. bulk-transfer the dump while the stream fills the backlog; emit the
//     dump as snapshot events, then Release drains the backlog in binlog
//     order (entries <= anchor would be dropped defensively).
//
// With snapshot_bgsave=force a fresh BGSAVE (LASTSAVE-poll) is demanded
// before step 1; the rest is identical. If opening the stream at the anchor
// fails with ErrPurged (the gap vanished between steps 1 and 3), the whole
// attempt escalates once to force mode.
func runDbsync(ctx context.Context, cfg *Config, log *slog.Logger, st *store.Store,
	makeRunner func(localPort int, startAt replica.Offset) *replica.Runner, incrementalLoop func() error) error {
	dumpHost := cfg.Source.Host
	dumpPort := cfg.Source.Port
	forceBgsave := cfg.SnapshotBgsave == "force"

	startSession := func(at replica.Offset) (*replica.Runner, chan error, context.CancelFunc, error) {
		sctx, cancel := context.WithCancel(ctx)
		rs := makeRunner(cfg.Source.LocalPort, at)
		rs.AttachGate(replica.NewGateCtl())
		serr := make(chan error, 1)
		go func() { serr <- rs.Run(sctx) }()
		if err := rs.WaitReady(sctx); err != nil {
			realErr := err
			select {
			case re := <-serr:
				if re != nil {
					realErr = re
				}
			case <-time.After(time.Second):
			}
			cancel()
			<-serr
			return nil, nil, nil, realErr
		}
		return rs, serr, cancel, nil
	}

	durable := cfg.BufferFsync == "durable"

	// ---------- durable crash-resume of the snapshot phase ----------
	// (a snapshot already marked done needs no dump at all — the plain
	// incremental resume path in RunVersion handled that earlier)
	if rst := st.State(); durable && rst.Snapshot == store.PhaseRunning && rst.Resume != nil {
		done, err := runDbsyncResume(ctx, cfg, log, st, makeRunner, incrementalLoop, rst.Resume)
		if err != nil {
			if !errors.Is(err, errResumeGone) {
				return err
			}
			log.Warn("app: dbsync: resume coordinates unusable, falling back to full re-scan", "err", err)
		} else if done {
			return nil
		}
	}

	for {
		st.ResetForRescan() // has_pos=false => session anchors at the live master tip
		_ = st.Flush()
		if durable {
			if err := replica.WipeBacklog(cfg.BufferDir); err != nil {
				return fmt.Errorf("app: wipe backlog: %w", err)
			}
		}

		var (
			rs     *replica.Runner
			serr   chan error
			cancel context.CancelFunc
		)
		onAnchor := func(anchor replica.Offset) error {
			var err error
			rs, serr, cancel, err = startSession(anchor)
			return err
		}

		rSync := makeRunner(cfg.Source.LocalPort+2, replica.Offset{})
		tip := replica.Offset{}
		if ri, err := info.FetchReplicationInfo(ctx, dumpHost, dumpPort, cfg.Source.Password, 5*time.Second); err == nil {
			tip = replica.Offset{Filenum: ri.Filenum, Offset: ri.Offset}
		}
		log.Info("app: dbsync: requesting dump (anchor-first)", "db", cfg.Source.DB, "bgsave_policy", cfg.SnapshotBgsave)
		dumpDir, anchor, err := dbsync.Fetch(ctx, rSync, cfg.DumpRoot, dumpHost, dumpPort, cfg.Source.Password, tip, forceBgsave, onAnchor, log)
		if err != nil {
			if cancel != nil {
				cancel()
				<-serr
			}
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			if !forceBgsave && errors.Is(err, replica.ErrPurged) {
				log.Warn("app: dbsync: anchor binlog purged before the stream attached; escalating to forced bgsave", "err", err)
				forceBgsave = true
				continue
			}
			return fmt.Errorf("app: dbsync fetch: %w", err)
		}
		if cancel != nil {
			defer cancel()
		}
		log.Info("app: dbsync: stream live at anchor",
			"anchor_filenum", anchor.Filenum, "anchor_offset", anchor.Offset)

		st.SetDelivered(anchor.Filenum, anchor.Offset)
		if durable {
			st.SetResume(&store.SnapshotResume{
				AnchorFilenum: anchor.Filenum, AnchorOffset: anchor.Offset, DumpDir: dumpDir,
			})
		}
		_ = st.Flush()
		rs.RequestOpenAt(anchor) // defensive: entries <= anchor belong to the dump

		inject := func(ev *envelope.Event) error {
			if err := rs.Inject(ctx, ev); err != nil {
				// the stream session died mid-dump: buffered continuity is
				// no longer guaranteed, redo the whole snapshot
				return fmt.Errorf("app: dbsync inject (stream session lost, re-snapshot required): %w", err)
			}
			return nil
		}
		if err := emitDumpAndRelease(ctx, cfg, log, st, rs, anchor, dumpDir, durable, dbsync.ReaderResume{}, false, inject); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-serr:
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if errors.Is(err, replica.ErrStalled) {
				log.Warn("app: incremental session stalled; reconnecting", "err", err)
				return incrementalLoop()
			}
			return err
		}
	}
}

// emitDumpAndRelease runs the (possibly resumed) dump emission, records
// durable progress, drains the backlog via Release and commits the snapshot
// phase. inject wires snapshot events into the runner's ordered stream.
func emitDumpAndRelease(ctx context.Context, cfg *Config, log *slog.Logger, st *store.Store,
	rs *replica.Runner, anchor replica.Offset, dumpDir string, durable bool,
	resume dbsync.ReaderResume, dumpDone bool, inject func(*envelope.Event) error) error {
	eng := dbsync.NewEngine(dbsync.EngineConfig{
		DBName: cfg.Source.DB, SourceID: cfg.SourceID(), IncludeTTL: cfg.IncludeTTL, Logger: log,
	})
	t0 := time.Now()
	if durable {
		st.SetResume(&store.SnapshotResume{
			AnchorFilenum: anchor.Filenum, AnchorOffset: anchor.Offset,
			TypeName: resume.TypeName, Key: resume.Key, DumpDone: dumpDone, DumpDir: dumpDir,
		})
		_ = st.Flush()
	}
	var emitted int64
	if !dumpDone {
		it, err := dbsync.OpenIterator(dumpDir, cfg.Source.DB, resume)
		if err != nil {
			return fmt.Errorf("app: dbsync open dump: %w", err)
		}
		defer it.Close()
		_, err = eng.Emit(ctx, it, replica.Offset{Filenum: anchor.Filenum, Offset: anchor.Offset}, func(ev *envelope.Event) error {
			if err := inject(ev); err != nil {
				return err
			}
			emitted++
			if durable && emitted%20000 == 0 {
				st.SetResume(&store.SnapshotResume{
					AnchorFilenum: anchor.Filenum, AnchorOffset: anchor.Offset,
					TypeName: ev.Type, Key: ev.Key, DumpDir: dumpDir,
				})
				_ = st.Flush()
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("app: dbsync emit: %w", err)
		}
		if durable {
			st.SetResume(&store.SnapshotResume{
				AnchorFilenum: anchor.Filenum, AnchorOffset: anchor.Offset,
				DumpDone: true, DumpDir: dumpDir,
			})
			_ = st.Flush()
		}
	}
	if err := rs.Release(); err != nil {
		return err
	}
	st.SetSnapshot(store.PhaseDone)
	st.ClearResume()
	if err := st.Flush(); err != nil {
		return err
	}
	log.Info("app: dbsync snapshot complete", "emitted", emitted, "elapsed", time.Since(t0).Round(time.Millisecond))
	return nil
}

// errResumeGone signals the durable resume coordinates no longer describe a
// usable state (dump folder gone, backlog tail purged...) — the caller falls
// back to a full re-scan.
var errResumeGone = errors.New("dbsync resume unavailable")

// runDbsyncResume continues an interrupted durable snapshot: reuse the
// fetched dump, adopt the retained backlog, attach the stream at the backlog
// tail, finish emission and drain. done=false + errResumeGone means the
// caller must fall back to a full re-scan.
func runDbsyncResume(ctx context.Context, cfg *Config, log *slog.Logger, st *store.Store,
	makeRunner func(int, replica.Offset) *replica.Runner, incrementalLoop func() error,
	rp *store.SnapshotResume) (bool, error) {
	anchor := replica.Offset{Filenum: rp.AnchorFilenum, Offset: rp.AnchorOffset}
	if _, err := os.Stat(filepath.Join(rp.DumpDir, "info")); err != nil {
		return false, fmt.Errorf("%w: dump dir %s gone", errResumeGone, rp.DumpDir)
	}
	probe := makeRunner(cfg.Source.LocalPort, replica.Offset{})
	probe.AttachGate(replica.NewGateCtl())
	tail, n, err := probe.PrepareBacklog()
	if err != nil {
		probe.Shutdown()
		return false, fmt.Errorf("dbsync resume backlog: %w", err)
	}
	probe.Shutdown()
	startAt := tail
	if startAt == (replica.Offset{}) {
		startAt = anchor
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rs := makeRunner(cfg.Source.LocalPort, startAt)
	rs.AttachGate(replica.NewGateCtl())
	serr := make(chan error, 1)
	go func() { serr <- rs.Run(sctx) }()
	if err := rs.WaitReady(sctx); err != nil {
		if errors.Is(err, replica.ErrPurged) {
			return false, fmt.Errorf("%w: backlog tail purged on master", errResumeGone)
		}
		return false, err
	}
	log.Info("app: dbsync: RESUMING snapshot durably",
		"anchor_filenum", anchor.Filenum, "anchor_offset", anchor.Offset,
		"backlog_records", n,
		"stream_start_filenum", startAt.Filenum, "stream_start_offset", startAt.Offset,
		"resume_type", rp.TypeName, "resume_key", rp.Key, "dump_done", rp.DumpDone)
	rs.RequestOpenAt(anchor)
	inject := func(ev *envelope.Event) error {
		if err := rs.Inject(ctx, ev); err != nil {
			return fmt.Errorf("app: dbsync inject during resume (re-scan required): %w", err)
		}
		return nil
	}
	resume := dbsync.ReaderResume{TypeName: rp.TypeName, Key: rp.Key}
	if err := emitDumpAndRelease(ctx, cfg, log, st, rs, anchor, rp.DumpDir, true, resume, rp.DumpDone, inject); err != nil {
		return true, err
	}
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case err := <-serr:
		if errors.Is(err, context.Canceled) {
			return true, nil
		}
		if errors.Is(err, replica.ErrStalled) {
			log.Warn("app: incremental session stalled after resume; reconnecting", "err", err)
			return true, incrementalLoop()
		}
		return true, err
	}
}
