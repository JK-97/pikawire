// Package replica implements the PikiwiDB slave-protocol reader: PB
// (protobuf) replication handshake (MetaSync/TrySync) plus the binlog
// receive loop with snapshot gating.
package replica

import (
	"fmt"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"

	"github.com/jk-97/pikawire/internal/pb/innermessage"
	"github.com/jk-97/pikawire/internal/pbnet"
	"google.golang.org/protobuf/proto"
)

const (
	portShiftReplServer = 2000

	connectTimeout = 2 * time.Second
	sendTimeout    = 30 * time.Second
	recvTimeout    = 1 * time.Second
)

// Offset is a binlog position.
type Offset = envelope.Position

// newer reports whether a > b positionally.
func offsetAfter(a, b Offset) bool {
	return a.Filenum > b.Filenum || (a.Filenum == b.Filenum && a.Offset > b.Offset)
}

func (r *Runner) connect() (*pbnet.Conn, error) {
	addr := fmt.Sprintf("%s:%d", r.cfg.MasterHost, r.cfg.MasterPort+portShiftReplServer)
	c, err := pbnet.Dial(addr, connectTimeout)
	if err != nil {
		return nil, err
	}
	_ = c.SetWriteDeadline(time.Now().Add(sendTimeout))
	return c, nil
}

func (r *Runner) metaSync(c *pbnet.Conn) error {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kMetaSync.Enum(),
		MetaSync: &innermessage.InnerRequest_MetaSync{
			Node: &innermessage.Node{Ip: proto.String(r.cfg.LocalIP), Port: proto.Int32(int32(r.cfg.LocalPort))},
		},
	}
	if r.cfg.Password != "" {
		req.MetaSync.Auth = proto.String(r.cfg.Password)
	}
	if err := c.Send(req); err != nil {
		return err
	}
	_ = c.SetReadDeadline(time.Now().Add(recvTimeout))
	var resp innermessage.InnerResponse
	if err := c.Recv(&resp); err != nil {
		return err
	}
	if resp.GetCode() != innermessage.StatusCode_kOk {
		return fmt.Errorf("metasync rejected: %s", resp.GetReply())
	}
	return nil
}

// trySync asks the master to start streaming from start. The returned
// masterTip is the master's producer position carried by the TrySync
// response (authoritative, unlike INFO which can lag/race).
func (r *Runner) trySync(c *pbnet.Conn, start Offset) (sessionID int32, code innermessage.InnerResponse_TrySync_ReplyCode, masterTip Offset, err error) {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kTrySync.Enum(),
		TrySync: &innermessage.InnerRequest_TrySync{
			Node:         &innermessage.Node{Ip: proto.String(r.cfg.LocalIP), Port: proto.Int32(int32(r.cfg.LocalPort))},
			Slot:         &innermessage.Slot{DbName: proto.String(r.cfg.DBName), SlotId: proto.Uint32(0)},
			BinlogOffset: &innermessage.BinlogOffset{Filenum: proto.Uint32(start.Filenum), Offset: proto.Uint64(start.Offset)},
		},
	}
	if err := c.Send(req); err != nil {
		return 0, 0, Offset{}, err
	}
	for attempt := 0; attempt < 10; attempt++ {
		_ = c.SetReadDeadline(time.Now().Add(recvTimeout))
		var resp innermessage.InnerResponse
		if err := c.Recv(&resp); err != nil {
			return 0, 0, Offset{}, err
		}
		if resp.GetType() == innermessage.Type_kTrySync && resp.GetTrySync() != nil {
			ts := resp.GetTrySync()
			tip := Offset{Filenum: ts.GetBinlogOffset().GetFilenum(), Offset: ts.GetBinlogOffset().GetOffset()}
			return ts.GetSessionId(), ts.GetReplyCode(), tip, nil
		}
	}
	return 0, 0, Offset{}, fmt.Errorf("no TrySync reply")
}

// pingTrySync re-sends the session TrySync request without consuming a
// reply: the master refreshes slave liveness state on receipt, and the
// reply is skipped by the main loop's message dispatch. Do NOT Recv here:
// any binlog batch in flight must be handled by the loop, not swallowed.
func (r *Runner) pingTrySync(c *pbnet.Conn, start Offset) error {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kTrySync.Enum(),
		TrySync: &innermessage.InnerRequest_TrySync{
			Node:         &innermessage.Node{Ip: proto.String(r.cfg.LocalIP), Port: proto.Int32(int32(r.cfg.LocalPort))},
			Slot:         &innermessage.Slot{DbName: proto.String(r.cfg.DBName), SlotId: proto.Uint32(0)},
			BinlogOffset: &innermessage.BinlogOffset{Filenum: proto.Uint32(start.Filenum), Offset: proto.Uint64(start.Offset)},
		},
	}
	_ = c.SetWriteDeadline(time.Now().Add(sendTimeout))
	return c.Send(req)
}

// ackToMaster informs the master of the consumed range [from, to].
func (r *Runner) ack(c *pbnet.Conn, from, to Offset, sessionID int32, first bool) error {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kBinlogSync.Enum(),
		BinlogSync: &innermessage.InnerRequest_BinlogSync{
			Node:          &innermessage.Node{Ip: proto.String(r.cfg.LocalIP), Port: proto.Int32(int32(r.cfg.LocalPort))},
			DbName:        proto.String(r.cfg.DBName),
			SlotId:        proto.Uint32(0),
			AckRangeStart: &innermessage.BinlogOffset{Filenum: proto.Uint32(from.Filenum), Offset: proto.Uint64(from.Offset)},
			AckRangeEnd:   &innermessage.BinlogOffset{Filenum: proto.Uint32(to.Filenum), Offset: proto.Uint64(to.Offset)},
			SessionId:     proto.Int32(sessionID),
			FirstSend:     proto.Bool(first),
		},
	}
	_ = c.SetWriteDeadline(time.Now().Add(sendTimeout))
	return c.Send(req)
}

// DBSync asks the master to bgsave and register us for a full dump transfer.
// The master blocks until the dump is ready; after this returns OK the dump
// is fetched via the rsync service and TrySync must resume from the dump's
// bgsave position (see internal/dbsync).
func (r *Runner) dbSync(c *pbnet.Conn, start Offset) (sessionID int32, err error) {
	req := &innermessage.InnerRequest{
		Type: innermessage.Type_kDBSync.Enum(),
		DbSync: &innermessage.InnerRequest_DBSync{
			Node:         &innermessage.Node{Ip: proto.String(r.cfg.LocalIP), Port: proto.Int32(int32(r.cfg.LocalPort))},
			Slot:         &innermessage.Slot{DbName: proto.String(r.cfg.DBName), SlotId: proto.Uint32(0)},
			BinlogOffset: &innermessage.BinlogOffset{Filenum: proto.Uint32(start.Filenum), Offset: proto.Uint64(start.Offset)},
		},
	}
	_ = c.SetWriteDeadline(time.Now().Add(sendTimeout))
	if err := c.Send(req); err != nil {
		return 0, err
	}
	// bgsave may take a while; allow a generous read deadline
	_ = c.SetReadDeadline(time.Now().Add(r.cfg.DBSyncTimeout))
	var resp innermessage.InnerResponse
	if err := c.Recv(&resp); err != nil {
		return 0, err
	}
	if resp.GetType() != innermessage.Type_kDBSync || resp.GetDbSync() == nil {
		return 0, fmt.Errorf("replica: dbsync response missing")
	}
	if resp.GetCode() != innermessage.StatusCode_kOk {
		return 0, fmt.Errorf("replica: dbsync rejected: %s", resp.GetReply())
	}
	return resp.GetDbSync().GetSessionId(), nil
}
