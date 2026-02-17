//go:build pikadump

package cgoadapter

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func frame(payload []byte) []byte {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	return append(lenBuf[:], payload...)
}

func batchFrame(recs [][3]string) []byte {
	var payload bytes.Buffer
	binary.Write(&payload, binary.LittleEndian, uint32(len(recs)))
	for _, r := range recs {
		binary.Write(&payload, binary.LittleEndian, uint16(len(r[0])))
		payload.WriteString(r[0])
		binary.Write(&payload, binary.LittleEndian, uint16(len(r[1])))
		payload.WriteString(r[1])
		binary.Write(&payload, binary.LittleEndian, uint32(len(r[2])))
		payload.WriteString(r[2])
	}
	return frame(payload.Bytes())
}

func TestReadBatchParsesFrames(t *testing.T) {
	out := bytes.NewBuffer(batchFrame([][3]string{
		{"string", "k1", "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$1\r\nv\r\n"},
		{"hash", "h:9", "*5\r\n$5\r\nHMSET\r\n$3\r\nh:9\r\n$1\r\nf\r\n$1\r\nv\r\n"},
	}))
	out.Write(batchFrame(nil))
	it := &iterator{out: out}
	recs, err := it.readBatch()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("recs=%d", len(recs))
	}
	if recs[0].Type != "string" || recs[0].Key != "k1" ||
		string(recs[0].RawRESP) != "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$1\r\nv\r\n" {
		t.Fatalf("rec0=%#v", recs[0])
	}
	if recs[1].Type != "hash" || recs[1].Key != "h:9" {
		t.Fatalf("rec1=%#v", recs[1])
	}
	recs, err = it.readBatch()
	if err != nil || len(recs) != 0 {
		t.Fatalf("EOF frame: recs=%d err=%v", len(recs), err)
	}
}

func TestPrefetchFeederDrains(t *testing.T) {
	// feeder reads frames off `out` until an empty batch closes the channel
	out := io.MultiReader(bytes.NewBuffer(batchFrame([][3]string{{"string", "a", "v"}})),
		bytes.NewBuffer(batchFrame([][3]string{{"string", "b", "v"}})),
		bytes.NewBuffer(batchFrame(nil)))
	it := &iterator{in: nopWriteCloser{}, out: out,
		batch: make(chan batchResult, prefetchDepth), quit: make(chan struct{})}
	it.startFeeder()
	var keys []string
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys = append(keys, rec.Key)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("keys=%v", keys)
	}
	if _, ok, _ := it.Next(); ok {
		t.Fatal("must stay exhausted")
	}
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

func TestSniffLayout(t *testing.T) {
	dir := t.TempDir()
	if got := sniffLayout(dir); got != "v35" {
		t.Fatalf("empty dir -> %s", got)
	}
	if err := os.MkdirAll(filepath.Join(dir, "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := sniffLayout(dir); got != "v40" {
		t.Fatalf("with 0/ -> %s", got)
	}
}
