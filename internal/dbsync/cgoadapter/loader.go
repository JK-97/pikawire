//go:build pikadump

// Package cgoadapter runs the embedded per-pika-version reader drivers as
// sidecar processes (see drivers/ and pikawire_dumper_proto.h): the child
// iterates that version's pika storage layer and streams records over a
// framed pipe protocol.
//
// Why a sidecar rather than dlopen: the drivers carry the whole RocksDB +
// pika-storage world with a ~34KB TLS image containing initial-exec
// relocations, which exceeds glibc's static TLS surplus for dlopened
// objects ("cannot allocate memory in static TLS block") — a startup-time
// exec sizes it correctly. The process boundary additionally contains any
// LOG(FATAL)/segfault inside the third-party storage code: the parent
// surfaces a clean iterator error instead of dying.
package cgoadapter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/jk-97/pikawire/internal/dbsync"
)

// pikaSourceRefs is stamped by scripts/build-cgo.sh (-X ldflags) with the
// pikiwidb revisions each embedded dumper was built against.
var pikaSourceRefs = "v35=unknown;v40=unknown"

const wireOpen = 8192 // fixed request frame; body must match kBody in dumper_main.cc

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

func extractBins() (string, error) {
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "pikawire-dumper-")
		if err != nil {
			binErr = err
			return
		}
		binDir = dir
		for _, name := range []string{"pikawire-dumper-v35", "pikawire-dumper-v40"} {
			b, err := driverFS.ReadFile("drivers/" + name)
			if err != nil {
				binErr = err
				return
			}
			if err := os.WriteFile(filepath.Join(dir, name), b, 0o700); err != nil {
				binErr = err
				return
			}
		}
	})
	return binDir, binErr
}

// sniffLayout picks the driver by engine-directory naming: 4.0.x materializes
// numeric kv/svc/meta dirs (0/1/2); 3.5.x uses per-type dirs (strings/...).
func sniffLayout(dbDir string) string {
	if st, err := os.Stat(filepath.Join(dbDir, "0")); err == nil && st.IsDir() {
		return "v40"
	}
	return "v35"
}

type iterator struct {
	cmd   *exec.Cmd
	in    io.WriteCloser
	out   io.Reader
	batch chan batchResult // prefetch pipeline: feeder owns the pipe
	quit  chan struct{}    // closes on abort/Close to retire the feeder
	done  bool
	buf   []dbsync.Record
}

type batchResult struct {
	recs []dbsync.Record
	err  error
}

// prefetchDepth > 1 keeps the dumper's rocksdb scans busy while the consumer
// side emits: without it, every round-trip serially alternates scan and emit.
const prefetchDepth = 4

// push hands a batch to the consumer unless the iterator was abandoned.
func (i *iterator) push(b batchResult) {
	select {
	case i.batch <- b:
	case <-i.quit:
	}
}

func (i *iterator) startFeeder() {
	go func() {
		defer close(i.batch)
		for {
			select {
			case <-i.quit:
				return
			default:
			}
			if _, err := i.in.Write([]byte{'N'}); err != nil {
				i.push(batchResult{err: errDumperDead})
				return
			}
			recs, err := i.readBatch()
			if err != nil {
				i.push(batchResult{err: err})
				return
			}
			i.push(batchResult{recs: recs})
			if len(recs) == 0 {
				return // exhausted; feeder retires (channel close signals EOF)
			}
		}
	}()
}

func putStr(b *bytes.Buffer, s string) {
	_ = binary.Write(b, binary.LittleEndian, uint16(len(s)))
	b.WriteString(s)
}

func open(dbDir, dbName string, resume dbsync.ReaderResume) (dbsync.Iterator, error) {
	dir, err := extractBins()
	if err != nil {
		return nil, err
	}
	bin := filepath.Join(dir, "pikawire-dumper-"+sniffLayout(dbDir))
	cmd := exec.Command(bin)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cgoadapter: start %s: %w", bin, err)
	}
	var req bytes.Buffer
	req.WriteByte('O')
	putStr(&req, dbDir)
	putStr(&req, dbName)
	binary.Write(&req, binary.LittleEndian, int32(512))         // batch_num
	binary.Write(&req, binary.LittleEndian, int64(4096))        // scan_batch
	binary.Write(&req, binary.LittleEndian, uint32(0xFFFFFFFF)) // type_mask
	binary.Write(&req, binary.LittleEndian, int32(0))           // cursor strategy
	binary.Write(&req, binary.LittleEndian, int64(-1))          // list_tail_n
	putStr(&req, "")                                            // scan_pattern
	if resume.TypeName != "" || resume.Key != "" {              // durable snapshot resume point
		req.WriteByte(byte(len(resume.TypeName))) // u8-length string (see GetStr)
		req.WriteString(resume.TypeName)
		putStr(&req, resume.Key)
	}
	pad := make([]byte, wireOpen-req.Len())
	if len(pad) < 0 {
		cmd.Process.Kill()
		return nil, errors.New("cgoadapter: open request too large")
	}
	req.Write(pad)
	if _, err := in.Write(req.Bytes()); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("cgoadapter: dumper stdin: %w", err)
	}
	it := &iterator{cmd: cmd, in: in, out: out,
		batch: make(chan batchResult, prefetchDepth), quit: make(chan struct{})}
	var status uint8
	if err := it.readReply(func(r *bytes.Reader) error {
		s, err := r.ReadByte()
		if err != nil {
			return err
		}
		status = s
		if s != 0 {
			var el uint16
			binary.Read(r, binary.LittleEndian, &el)
			msg := make([]byte, el)
			r.Read(msg)
			return fmt.Errorf("dumper open failed: %s", msg)
		}
		return nil
	}); err != nil {
		it.abort()
		return nil, err
	}
	_ = status
	it.startFeeder()
	return it, nil
}

// readBatch pulls one 'N' reply frame and decodes its records.
func (i *iterator) readBatch() ([]dbsync.Record, error) {
	var recs []dbsync.Record
	err := i.readReply(func(r *bytes.Reader) error {
		var count uint32
		if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
			return err
		}
		recs = make([]dbsync.Record, 0, count)
		for n := uint32(0); n < count; n++ {
			var rec dbsync.Record
			var sl uint16
			if err := binary.Read(r, binary.LittleEndian, &sl); err != nil {
				return err
			}
			t := make([]byte, sl)
			if _, err := io.ReadFull(r, t); err != nil {
				return err
			}
			rec.Type = string(t)
			if err := binary.Read(r, binary.LittleEndian, &sl); err != nil {
				return err
			}
			k := make([]byte, sl)
			if _, err := io.ReadFull(r, k); err != nil {
				return err
			}
			rec.Key = string(k)
			var rl uint32
			if err := binary.Read(r, binary.LittleEndian, &rl); err != nil {
				return err
			}
			raw := make([]byte, rl)
			if _, err := io.ReadFull(r, raw); err != nil {
				return err
			}
			rec.RawRESP = raw
			recs = append(recs, rec)
		}
		return nil
	})
	return recs, err
}

func (i *iterator) readReply(fn func(*bytes.Reader) error) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(i.out, lenBuf[:]); err != nil {
		return fmt.Errorf("cgoadapter: dumper reply len: %w", err)
	}
	payload := make([]byte, binary.LittleEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(i.out, payload); err != nil {
		return fmt.Errorf("cgoadapter: dumper reply: %w", err)
	}
	return fn(bytes.NewReader(payload))
}

var errDumperDead = errors.New("cgoadapter: dumper process died")

func (i *iterator) Next() (*dbsync.Record, bool, error) {
	if i.done {
		return nil, false, nil
	}
	for {
		if len(i.buf) > 0 {
			rec := i.buf[0]
			i.buf = i.buf[1:]
			return &rec, true, nil
		}
		br, ok := <-i.batch
		if !ok {
			i.done = true
			return nil, false, nil
		}
		if br.err != nil {
			i.abort()
			return nil, false, br.err
		}
		i.buf = br.recs
	}
}

func (i *iterator) abort() {
	select {
	case <-i.quit:
	default:
		close(i.quit)
	}
	_ = i.in.Close()
	if i.cmd != nil && i.cmd.Process != nil {
		_ = i.cmd.Wait()
	}
}

func (i *iterator) Close() error {
	select {
	case <-i.quit:
	default:
		close(i.quit)
	}
	i.in.Write([]byte{'C'})
	i.in.Close()
	return i.cmd.Wait()
}

func init() {
	dbsync.RegisterReader(open)
	dbsync.SetReaderRef("dumpers(" + pikaSourceRefs + ")")
}
