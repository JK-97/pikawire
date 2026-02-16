// Package dbsync implements the fast full-snapshot path: DBSync handshake
// (master bgsave of a RocksDB checkpoint) + rsync fetch of the dump + exact
// bgsave binlog anchor; no reconciliation machinery is needed. Dump parsing
// itself is registered by the cgo reader build
// (tag pikadump); tagless binaries report it unavailable.
package dbsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jk-97/pikawire/internal/redisclient"
	"github.com/jk-97/pikawire/internal/replica"
)

var errNoReader = errors.New("dbsync: this binary has no dump reader (build with -tags pikadump)")

// ErrNoReader surfaces the missing-cgo case to callers.
var ErrNoReader = errNoReader

// Fetch returns (localDumpPath, bgsavePosition) for a FULLY FRESH checkpoint.
// info is left in place for the reader.
//
// Two defenses against PikaServer::TryDBSync's checkpoint REUSE (3.5.6 re-
// uses any existing bgsave dir when its filenum is within kDBSyncMaxGap of
// the requested position — a stale reused anchor can precede the caller's
// live replication stream, opening a coverage hole):
//
//  1. force a fresh bgsave via the client port (BGSAVE + poll LASTSAVE until
//     it advances), so the served checkpoint is current by construction;
//  2. send the caller's stream start as the DBSync filenum (`top`), so the
//     master's own gap check also demands a fresh save.
func Fetch(ctx context.Context, r *replica.Runner, dumpRoot, host string, port int, passwd string, want replica.Offset, freshBgsave bool, onAnchor func(anchor replica.Offset) error, log *slog.Logger) (string, replica.Offset, error) {
	if freshBgsave {
		if err := ensureFreshBgsave(ctx, host, port, passwd, log); err != nil {
			return "", replica.Offset{}, err
		}
	}
	conn, _, err := r.DBSyncSetup(ctx, want)
	if err != nil {
		return "", replica.Offset{}, err
	}

	dbName := r.DBName()
	rc := &Rsync{
		MasterHost: host, MasterPort: port,
		DBName: dbName, DumpPath: dumpRoot, Log: log,
	}
	uuid, files, err := rc.Begin() // meta + the small info file only
	if err != nil {
		return "", replica.Offset{}, fmt.Errorf("dbsync: rsync begin: %w", err)
	}
	// The rsync file list is relative to the master's <bgsave>/<date>/dbX
	// directory, so content lands directly under dumpRoot — unless the
	// server ships the db-name level itself. Resolve the real layout.
	dbDir := filepath.Join(dumpRoot, dbName)
	if _, err := os.Stat(filepath.Join(dumpRoot, "info")); err == nil {
		dbDir = dumpRoot
	}
	pos, err := LoadBgsaveInfo(dbDir, dumpRoot)
	if err != nil {
		return "", replica.Offset{}, err
	}
	// The anchor is known before the bulk transfer: the caller opens its
	// replication stream exactly here (TrySync(anchor)), so no binlog
	// between dump and stream can ever be missed and no repositioning
	// machinery is needed.
	if err := rc.Rest(uuid, files); err != nil {
		conn.Close()
		return dbDir, pos, fmt.Errorf("dbsync: rsync fetch: %w", err)
	}
	// Close the DBSync protocol session BEFORE the replication stream is
	// attached: while the DbSync slave node is being retired the master
	// silently refuses to push binlog to BinlogSync sessions of the same db
	// (observed: a stream established around the DBSync window receives
	// zero entries, while a fresh TrySync at the very same position after
	// the DBSync connection is closed gets the full backlog).
	conn.Close()
	if onAnchor != nil {
		if err := onAnchor(pos); err != nil {
			return dbDir, pos, err
		}
	}
	return dbDir, pos, nil
}

// ensureFreshBgsave forces the master to produce a NEW checkpoint before we
// request the dump: LASTSAVE -> BGSAVE -> poll LASTSAVE until it advances
// (pika updates it only on full bgsave success, pika_db.cc RunBgsaveEngine).
// A concurrent in-flight bgsave is also waited on: its completion likewise
// advances LASTSAVE.
func ensureFreshBgsave(ctx context.Context, host string, port int, passwd string, log *slog.Logger) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	c, err := redisclient.Dial(ctx, addr, passwd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dbsync: bgsave dial: %w", err)
	}
	defer c.Close()
	t0, err := lastSave(c)
	if err != nil {
		return err
	}
	v, err := c.Do([][][]byte{{[]byte("BGSAVE")}})
	if err != nil {
		return fmt.Errorf("dbsync: bgsave: %w", err)
	}
	if len(v) > 0 && v[0].Kind == redisclient.KindError {
		return fmt.Errorf("dbsync: bgsave rejected: %v", v[0].Err)
	}
	if log != nil {
		log.Info("dbsync: BGSAVE issued, waiting for LASTSAVE to advance", "lastsave", t0)
	}
	deadline := time.Now().Add(15 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
		t1, err := lastSave(c)
		if err != nil {
			return err
		}
		if t1 > t0 {
			if log != nil {
				log.Info("dbsync: fresh checkpoint ready", "lastsave", t1)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dbsync: bgsave did not complete within 15m (LASTSAVE stuck at %d)", t0)
		}
	}
}

func lastSave(c *redisclient.Client) (int64, error) {
	v, err := c.Do([][][]byte{{[]byte("LASTSAVE")}})
	if err != nil {
		return 0, fmt.Errorf("dbsync: lastsave: %w", err)
	}
	if len(v) == 0 {
		return 0, errors.New("dbsync: empty LASTSAVE reply")
	}
	return v[0].AsInt()
}

// LoadBgsaveInfo parses the bgsave binlog position recorded by the master in
// the dumped db (lines[3]=filenum, lines[4]=offset per Pika's dbsync meta).
func LoadBgsaveInfo(dbRoot, dumpRoot string) (replica.Offset, error) {
	infoPath := filepath.Join(dbRoot, "info")
	if _, err := os.Stat(infoPath); err != nil {
		alt := filepath.Join(dumpRoot, "info")
		if _, err2 := os.Stat(alt); err2 != nil {
			return replica.Offset{}, fmt.Errorf("dbsync: info file missing after fetch (looked in %q)", dbRoot)
		}
		infoPath = alt
	}
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return replica.Offset{}, err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) < 5 {
		return replica.Offset{}, fmt.Errorf("dbsync: info file malformed (need >=5 lines)")
	}
	filenum, err := strconv.ParseUint(strings.TrimSpace(lines[3]), 10, 32)
	if err != nil {
		return replica.Offset{}, fmt.Errorf("dbsync: parse info filenum: %w", err)
	}
	offset, err := strconv.ParseUint(strings.TrimSpace(lines[4]), 10, 64)
	if err != nil {
		return replica.Offset{}, fmt.Errorf("dbsync: parse info offset: %w", err)
	}
	return replica.Offset{Filenum: uint32(filenum), Offset: offset}, nil
}
