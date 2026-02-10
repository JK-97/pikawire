package pbnet

import (
	"net"
	"testing"

	"github.com/jk-97/pikawire/internal/pb/innermessage"
)

func TestConn_SendRecv(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	sender := Wrap(c1)
	receiver := Wrap(c2)

	want := &innermessage.InnerRequest{
		Type: innermessage.Type_kMetaSync.Enum(),
		MetaSync: &innermessage.InnerRequest_MetaSync{
			Node: &innermessage.Node{Ip: protoString("127.0.0.1"), Port: protoInt32(21333)},
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := sender.Send(want); err != nil {
			t.Errorf("Send: %v", err)
		}
	}()

	var got innermessage.InnerRequest
	if err := receiver.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	<-done

	if got.GetType() != innermessage.Type_kMetaSync {
		t.Fatalf("type=%v", got.GetType())
	}
	if got.GetMetaSync() == nil || got.GetMetaSync().GetNode().GetPort() != 21333 {
		t.Fatalf("meta_sync=%v", got.GetMetaSync())
	}
}

func protoString(v string) *string { return &v }
func protoInt32(v int32) *int32    { return &v }
