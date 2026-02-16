package dbsync

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/pb/rsyncservice"
	"github.com/jk-97/pikawire/internal/pbnet"
)

const (
	defaultChunkBytes = 4 * 1024 * 1024
	portShiftRsync2   = 10001
)

type Rsync struct {
	MasterHost string
	MasterPort int

	DBName   string
	DumpPath string

	ChunkBytes        int
	Timeout           time.Duration
	WaitBgsaveTimeout time.Duration
	Concurrency       int // parallel file fetches in Rest (default 4)

	Log *slog.Logger
}

// Fetch downloads the whole dump (convenience wrapper around Begin+Rest).
func (c *Rsync) Fetch() error {
	uuid, files, err := c.Begin()
	if err != nil {
		return err
	}
	return c.Rest(uuid, files)
}

// Begin prepares the dump directory, waits for bgsave via the meta request,
// and downloads ONLY the small `info` file, so the bgsave anchor is known
// before the bulk SST transfer starts (callers open the replication stream
// from the anchor in between — the kDBSyncMaxGap reuse rule guarantees the
// anchor's binlog file exists at decision time).
func (c *Rsync) Begin() (snapshotUUID string, files []string, err error) {
	if c.MasterHost == "" || c.MasterPort == 0 {
		return "", nil, fmt.Errorf("master host/port required")
	}
	if c.DBName == "" {
		return "", nil, fmt.Errorf("db_name required")
	}
	if c.DumpPath == "" {
		return "", nil, fmt.Errorf("dump_path required")
	}
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	if err := os.RemoveAll(c.DumpPath); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(c.DumpPath, 0o755); err != nil {
		return "", nil, err
	}
	snapshotUUID, files, err = c.fetchMeta(timeout)
	if err != nil {
		return "", nil, err
	}
	for _, file := range files {
		if filepath.Base(file) == "info" {
			if err := c.fetchFile(file, snapshotUUID, defaultChunkBytes, timeout); err != nil {
				return "", nil, err
			}
			break
		}
	}
	return snapshotUUID, files, nil
}

// Rest transfers every file except the already-fetched `info`, concurrently.
// Each file opens its own rsync2 connection, so a bounded worker pool is safe
// and materially shortens the bulk-transfer phase (which sits on the critical
// path between "anchor known" and "stream attached"). On the first error the
// remaining files are skipped.
func (c *Rsync) Rest(snapshotUUID string, files []string) error {
	chunk := c.ChunkBytes
	if chunk <= 0 {
		chunk = defaultChunkBytes
	}
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	concurrency := c.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	work := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Base(f) != "info" {
			work = append(work, f)
		}
	}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		firstE error
		next   int
	)
	if concurrency > len(work) && len(work) > 0 {
		concurrency = len(work)
	}
	if concurrency <= 0 {
		return nil // nothing to transfer
	}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				failed := firstE != nil
				var file string
				if !failed && next < len(work) {
					file = work[next]
					next++
				}
				mu.Unlock()
				if failed || file == "" {
					return
				}
				if err := c.fetchFile(file, snapshotUUID, chunk, timeout); err != nil {
					mu.Lock()
					if firstE == nil {
						firstE = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstE
}

func (c *Rsync) fetchMeta(timeout time.Duration) (snapshotUUID string, files []string, err error) {
	start := time.Now()
	attempt := 0
	metaErrorCount := 0
	wait := c.WaitBgsaveTimeout
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	backoff := time.Second
	for {
		if time.Since(start) >= wait {
			return "", nil, fmt.Errorf("rsync2 meta timeout after %s attempts=%d", time.Since(start), attempt)
		}
		attempt++
		conn, err := pbnet.Dial(fmt.Sprintf("%s:%d", c.MasterHost, c.MasterPort+portShiftRsync2), timeout)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		req := &rsyncservice.RsyncRequest{
			Type:        rsyncservice.Type_kRsyncMeta.Enum(),
			ReaderIndex: protoInt32(0),
			DbName:      protoString(c.DBName),
			SlotId:      protoUint32(0),
		}
		if err := conn.Send(req); err != nil {
			_ = conn.Close()
			time.Sleep(time.Second)
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		var resp rsyncservice.RsyncResponse
		err = conn.Recv(&resp)
		_ = conn.Close()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if resp.GetCode() != rsyncservice.StatusCode_kOk || resp.GetMetaResp() == nil {
			metaErrorCount++
			if metaErrorCount == 1 || metaErrorCount%5 == 0 {
				if c.Log != nil {
					c.Log.Info("rsync meta not ready, waiting bgsave", "attempt", metaErrorCount, "elapsed", time.Since(start).Truncate(time.Second), "next_sleep", backoff)
				}
			}
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
			continue
		}
		snapshotUUID = resp.GetSnapshotUuid()
		files = append([]string(nil), resp.GetMetaResp().GetFilenames()...)
		return snapshotUUID, files, nil
	}
}

func (c *Rsync) fetchFile(filename, snapshotUUID string, chunkBytes int, timeout time.Duration) error {
	if filename == "" {
		return errors.New("rsync2: empty filename")
	}
	conn, err := pbnet.Dial(fmt.Sprintf("%s:%d", c.MasterHost, c.MasterPort+portShiftRsync2), timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	localPath := filepath.Join(c.DumpPath, filename)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	var offset uint64
	for {
		req := &rsyncservice.RsyncRequest{
			Type:        rsyncservice.Type_kRsyncFile.Enum(),
			ReaderIndex: protoInt32(0),
			DbName:      protoString(c.DBName),
			SlotId:      protoUint32(0),
			FileReq: &rsyncservice.FileRequest{
				Filename: protoString(filename),
				Offset:   protoUint64(offset),
				Count:    protoUint64(uint64(chunkBytes)),
			},
		}
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		if err := conn.Send(req); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		var resp rsyncservice.RsyncResponse
		if err := conn.Recv(&resp); err != nil {
			return err
		}
		if resp.GetCode() != rsyncservice.StatusCode_kOk || resp.GetFileResp() == nil {
			return fmt.Errorf("rsync2: file response error")
		}
		if resp.GetSnapshotUuid() != snapshotUUID {
			return fmt.Errorf("rsync2: snapshot uuid changed expected=%s actual=%s", snapshotUUID, resp.GetSnapshotUuid())
		}
		fileResp := resp.GetFileResp()
		if fileResp.GetOffset() != offset {
			// Keep writing at the requested offset (same as C++ seekp).
		}
		if _, err := out.Seek(int64(offset), io.SeekStart); err != nil {
			return err
		}
		if _, err := out.Write(fileResp.GetData()); err != nil {
			return err
		}
		offset += fileResp.GetCount()
		if fileResp.GetEof() != 0 {
			return nil
		}
	}
}

func protoString(v string) *string { return &v }
func protoInt32(v int32) *int32    { return &v }
func protoUint32(v uint32) *uint32 { return &v }
func protoUint64(v uint64) *uint64 { return &v }
