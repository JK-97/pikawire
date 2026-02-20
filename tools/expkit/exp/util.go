package exp

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// DIAL is the default dial/read timeout for experiment clients.
const DIAL = 10 * time.Second

// ProducerPos is a master binlog producer position from INFO replication.
type ProducerPos struct {
	Filenum uint32
	Offset  uint64
}

// FetchInfoOffset reads "<db>:binlog_offset=<f> <o>" from INFO replication.
func FetchInfoOffset(addr, db string) (ProducerPos, error) {
	c, err := Dial(addr, DIAL)
	if err != nil {
		return ProducerPos{}, err
	}
	defer c.Close()
	v, err := c.Do("INFO", "replication")
	if err != nil {
		return ProducerPos{}, err
	}
	for _, line := range strings.Split(v.StrVal(), "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, db+":binlog_offset=") {
			continue
		}
		var f, o uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(line, db+":binlog_offset="), "%d %d", &f, &o); err != nil {
			return ProducerPos{}, fmt.Errorf("parse %q: %w", line, err)
		}
		return ProducerPos{Filenum: uint32(f), Offset: o}, nil
	}
	return ProducerPos{}, fmt.Errorf("no %s binlog_offset in INFO", db)
}

// ParsePos parses "filenum:offset".
func ParsePos(s string) (ProducerPos, error) {
	var f, o uint64
	if _, err := fmt.Sscanf(s, "%d:%d", &f, &o); err != nil {
		return ProducerPos{}, err
	}
	return ProducerPos{Filenum: uint32(f), Offset: o}, nil
}

// String renders "filenum:offset".
func (p ProducerPos) String() string {
	return strconv.FormatUint(uint64(p.Filenum), 10) + ":" + strconv.FormatUint(p.Offset, 10)
}

// After reports p > q.
func (p ProducerPos) After(q ProducerPos) bool {
	return p.Filenum > q.Filenum || (p.Filenum == q.Filenum && p.Offset > q.Offset)
}

// Rand is a shared-seed generator helper for experiment tools.
type Rand struct{ r *rand.Rand }

// NewRand builds a Rand over an explicit seed.
func NewRand(seed int64) *Rand { return &Rand{rand.New(rand.NewSource(seed))} }

// Intn returns [0,n).
func (x *Rand) Intn(n int) int { return x.r.Intn(n) }

// Float64 returns [0,1).
func (x *Rand) Float64() float64 { return x.r.Float64() }

// Shuffle wraps rand.Shuffle.
func (x *Rand) Shuffle(n int, swap func(i, j int)) { x.r.Shuffle(n, swap) }
