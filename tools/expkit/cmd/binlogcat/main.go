// Command binlogcat connects to the PikiwiDB replication port as an
// independent slave and records EVERY binlog entry (filenum, offset,
// command, key) to a JSONL file. This file is the ground truth the audit
// tool joins the product's event stream against. Protocol knowledge comes
// from docs/design.md; framing and decoding are re-implemented here so a
// product bug cannot hide itself in the capture.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jk-97/pikawire/internal/pb/innermessage"
	"github.com/jk-97/pikawire/tools/expkit/exp"
	"google.golang.org/protobuf/proto"
)

type pconn struct{ c net.Conn }

func (p *pconn) send(m proto.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	_ = p.c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := p.c.Write(hdr[:]); err != nil {
		return err
	}
	_, err = p.c.Write(data)
	return err
}

func (p *pconn) recv(msg proto.Message) error {
	var hdr [4]byte
	if _, err := io.ReadFull(p.c, hdr[:]); err != nil {
		return err
	}
	buf := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(p.c, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf, msg)
}

type record struct {
	F    uint32 `json:"f"`
	O    uint64 `json:"o"`
	PE   uint32 `json:"pef"`
	PEO  uint64 `json:"peo"`
	Cmd  string `json:"cmd,omitempty"`
	Key  string `json:"key,omitempty"`
	Argc int    `json:"argc,omitempty"`
	Ex   uint32 `json:"ex,omitempty"`
	Bad  string `json:"bad,omitempty"`
	Raw  string `json:"raw,omitempty"`
}

type pos struct {
	f uint32
	o uint64
}

func (a pos) after(b pos) bool { return a.f > b.f || (a.f == b.f && a.o > b.o) }
func (a pos) zero() bool       { return a.f == 0 && a.o == 0 }

func main() {
	repl := flag.String("repl", "127.0.0.1:11221", "master replication host:port")
	db := flag.String("db", "db0", "slot db name")
	lip := flag.String("local-ip", "127.0.0.1", "slave identity ip")
	lport := flag.Int("local-port", 19999, "slave identity port (must differ from other slaves)")
	start := flag.String("start", "info", "start pos filenum:offset, or 'info'")
	infoAddr := flag.String("info-addr", "127.0.0.1:9221", "host:port for INFO when -start=info")
	out := flag.String("out", "binlogcat.jsonl", "output JSONL")
	_ = flag.String("anchor", "", "dump anchor this capture must cover (recorded into .done)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var st pos
	if *start == "info" {
		pp, err := exp.FetchInfoOffset(*infoAddr, *db)
		if err != nil {
			log.Error("resolve start", "err", err)
			os.Exit(1)
		}
		st = pos{pp.Filenum, pp.Offset}
	} else {
		var fv, ov uint64
		if _, err := fmt.Sscanf(*start, "%d:%d", &fv, &ov); err != nil {
			log.Error("bad -start", "err", err)
			os.Exit(1)
		}
		st = pos{uint32(fv), ov}
	}

	fh, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Error("open out", "err", err)
		os.Exit(1)
	}
	bw := bufio.NewWriterSize(fh, 1<<20)
	enc := json.NewEncoder(bw)

	var n, bad int64
	lastSeen := st // highest consumed pb-end
	firstUnacked := pos{}

	trySyncReq := func(p pos) *innermessage.InnerRequest {
		return &innermessage.InnerRequest{
			Type: innermessage.Type_kTrySync.Enum(),
			TrySync: &innermessage.InnerRequest_TrySync{
				Node:         &innermessage.Node{Ip: lip, Port: proto.Int32(int32(*lport))},
				Slot:         &innermessage.Slot{DbName: db, SlotId: proto.Uint32(0)},
				BinlogOffset: &innermessage.BinlogOffset{Filenum: proto.Uint32(p.f), Offset: proto.Uint64(p.o)},
			},
		}
	}
	ack := func(pc *pconn, sid int32, from, to pos, first bool) error {
		return pc.send(&innermessage.InnerRequest{
			Type: innermessage.Type_kBinlogSync.Enum(),
			BinlogSync: &innermessage.InnerRequest_BinlogSync{
				Node:          &innermessage.Node{Ip: lip, Port: proto.Int32(int32(*lport))},
				DbName:        db,
				SlotId:        proto.Uint32(0),
				AckRangeStart: &innermessage.BinlogOffset{Filenum: proto.Uint32(from.f), Offset: proto.Uint64(from.o)},
				AckRangeEnd:   &innermessage.BinlogOffset{Filenum: proto.Uint32(to.f), Offset: proto.Uint64(to.o)},
				SessionId:     proto.Int32(sid),
				FirstSend:     proto.Bool(first),
			},
		})
	}

outer:
	for {
		if ctx.Err() != nil {
			break
		}
		c, err := net.DialTimeout("tcp", *repl, 5*time.Second)
		if err != nil {
			log.Warn("dial", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		pc := &pconn{c}
		var resp innermessage.InnerResponse
		if err := pc.send(&innermessage.InnerRequest{
			Type: innermessage.Type_kMetaSync.Enum(),
			MetaSync: &innermessage.InnerRequest_MetaSync{
				Node: &innermessage.Node{Ip: lip, Port: proto.Int32(int32(*lport))},
			},
		}); err != nil {
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		if err := pc.recv(&resp); err != nil || resp.GetCode() != innermessage.StatusCode_kOk {
			log.Warn("metasync", "err", err, "code", resp.GetCode().String())
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		if err := pc.send(trySyncReq(lastSeen)); err != nil {
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		var sid int32
		for attempt := 0; attempt < 10 && sid == 0; attempt++ {
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err := pc.recv(&resp); err != nil {
				log.Warn("trysync recv", "err", err)
				break
			}
			if resp.GetType() == innermessage.Type_kTrySync && resp.GetTrySync() != nil {
				sid = resp.GetTrySync().GetSessionId()
				log.Info("trysync ok", "sid", sid, "code", resp.GetTrySync().GetReplyCode().String(), "at", fmt.Sprintf("%d:%d", lastSeen.f, lastSeen.o))
			}
		}
		if sid == 0 {
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		if err := ack(pc, sid, lastSeen, lastSeen, true); err != nil {
			log.Warn("first ack", "err", err)
			c.Close()
			continue
		}
		firstUnacked = pos{}
		log.Info("session live", "from", fmt.Sprintf("%d:%d", lastSeen.f, lastSeen.o))

		tick := time.NewTicker(time.Second)
		var respLoopErr error
		for respLoopErr == nil {
			select {
			case <-ctx.Done():
				tick.Stop()
				c.Close()
				break outer
			case <-tick.C:
				if err := pc.send(trySyncReq(lastSeen)); err != nil {
					respLoopErr = err
					break
				}
				if !firstUnacked.zero() {
					if err := ack(pc, sid, firstUnacked, lastSeen, false); err != nil {
						respLoopErr = err
						break
					}
					firstUnacked = pos{}
				} else {
					_ = ack(pc, sid, pos{}, pos{}, false)
				}
				_ = bw.Flush()
			default:
			}
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			if err := pc.recv(&resp); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				respLoopErr = err
				break
			}
			if resp.GetType() != innermessage.Type_kBinlogSync {
				continue
			}
			batchHadEntries := false
			for _, res := range resp.GetBinlogSync() {
				if res.GetSessionId() != sid {
					continue
				}
				pbf, peo := uint32(0), uint64(0)
				if off := res.GetBinlogOffset(); off != nil {
					pbf, peo = off.GetFilenum(), off.GetOffset()
				}
				if len(res.GetBinlog()) == 0 {
					continue // keepalive packet
				}
				rec := record{PE: pbf, PEO: peo}
				cur := pos{pbf, peo}
				item, err := exp.DecodeEntry(res.GetBinlog())
				if err != nil {
					rec.Bad = err.Error()
					rec.Raw = hex.EncodeToString(res.GetBinlog()[:min(64, len(res.GetBinlog()))])
					bad++
					if cur.zero() {
						cur = pos{pbf, peo}
					}
				} else {
					rec.F, rec.O, rec.Ex = item.Filenum, item.Offset, item.ExecTime
					rec.Cmd, rec.Argc = item.Argv[0], len(item.Argv)
					if len(item.Argv) > 1 {
						rec.Key = item.Argv[1]
					}
					if cur.zero() {
						cur = pos{item.Filenum, item.Offset}
					}
					if !cur.after(lastSeen) {
						rec.Bad = "out-of-order"
						bad++
					}
				}
				if cur.after(lastSeen) {
					lastSeen = cur
				}
				if firstUnacked.zero() {
					firstUnacked = cur
				}
				batchHadEntries = true
				if err := enc.Encode(&rec); err != nil {
					log.Error("write", "err", err)
					os.Exit(1)
				}
				n++
			}
			if batchHadEntries && !firstUnacked.zero() {
				if err := ack(pc, sid, firstUnacked, lastSeen, false); err != nil {
					log.Warn("batch ack", "err", err)
					respLoopErr = err
					break
				}
				firstUnacked = pos{}
			}
		}
		tick.Stop()
		log.Warn("session ended, will resync", "err", respLoopErr, "consumed", n)
		c.Close()
		time.Sleep(time.Second)
	}
	_ = bw.Flush()
	log.Info("binlogcat done", "records", n, "bad", bad, "last_pb_end", fmt.Sprintf("%d:%d", lastSeen.f, lastSeen.o))
	anchor := st
	if a := flag.CommandLine.Lookup("anchor"); a != nil && a.Value.String() != "" {
		if pp, err := exp.ParsePos(a.Value.String()); err == nil {
			anchor = pos{pp.Filenum, pp.Offset}
		}
	}
	_ = os.WriteFile(*out+".done", []byte(fmt.Sprintf("%d:%d %d %d:%d\n", lastSeen.f, lastSeen.o, n, anchor.f, anchor.o)), 0o644)
}
