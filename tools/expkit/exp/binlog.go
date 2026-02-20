package exp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

// Entry is one decoded PikiwiDB binlog record (type-first layout).
// Offsets are independent re-derivation of the on-disk/wire format.
type Entry struct {
	Type     uint16
	ExecTime uint32
	Filenum  uint32
	Offset   uint64 // entry START (item header space)
	Argv     []string
}

// DecodeEntry parses one PB-wrapped binlog payload.
func DecodeEntry(b []byte) (Entry, error) {
	const header = 34
	if len(b) < header {
		return Entry{}, fmt.Errorf("entry too short: %d", len(b))
	}
	t := binary.LittleEndian.Uint16(b[0:2])
	if t != 1 {
		return Entry{}, fmt.Errorf("unexpected entry type %d", t)
	}
	execTime := binary.LittleEndian.Uint32(b[2:6])
	filenum := binary.LittleEndian.Uint32(b[18:22])
	offset := binary.LittleEndian.Uint64(b[22:30])
	clen := binary.LittleEndian.Uint32(b[30:34])
	if uint32(len(b)-header) != clen {
		return Entry{}, fmt.Errorf("content length mismatch: header says %d, got %d", clen, len(b)-header)
	}
	argv, err := ParseRespArray(b[header:])
	if err != nil {
		return Entry{}, err
	}
	return Entry{Type: t, ExecTime: execTime, Filenum: filenum, Offset: offset, Argv: argv}, nil
}

// ParseRespArray decodes a RESP array of bulk strings into argv.
func ParseRespArray(p []byte) ([]string, error) {
	r := bytes.NewReader(p)
	line, err := readLineB(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("argv: expected array, got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		line, err := readLineB(r)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("argv: expected bulk at %d, got %q", i, line)
		}
		l, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := ioReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:l]))
	}
	return out, nil
}

func readLineB(r *bytes.Reader) ([]byte, error) {
	var out []byte
	for {
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == '\n' {
			if len(out) > 0 && out[len(out)-1] == '\r' {
				out = out[:len(out)-1]
			}
			return out, nil
		}
		out = append(out, c)
		if len(out) > 1<<24 {
			return nil, errors.New("line too long")
		}
	}
}

func ioReadFull(r *bytes.Reader, p []byte) (int, error) {
	n, err := r.Read(p)
	for n < len(p) && err == nil {
		var m int
		m, err = r.Read(p[n:])
		n += m
	}
	if n < len(p) {
		return n, ioErrShort
	}
	return n, nil
}

var ioErrShort = errors.New("unexpected EOF")
