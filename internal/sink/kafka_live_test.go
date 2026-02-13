package sink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"
)

// TestKafkaLiveRoundtrip runs only when PIKAWIRE_KAFKA is set to a broker
// list and PIKAWIRE_TOPIC to an existing topic with >=1 partition.
func TestKafkaLiveRoundtrip(t *testing.T) {
	brokers := os.Getenv("PIKAWIRE_KAFKA")
	topic := os.Getenv("PIKAWIRE_TOPIC")
	if brokers == "" || topic == "" {
		t.Skip("set PIKAWIRE_KAFKA + PIKAWIRE_TOPIC to run live kafka test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := NewKafkaSink(ctx, KafkaConfig{Brokers: []string{brokers}, Topic: topic, WorkersPerPart: 2, BatchSize: 8, BatchTimeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const n = 20
	var acked int
	s.SetDeliveryNotifier(func(pos envelope.Position) error { return nil })
	for i := 0; i < n; i++ {
		ev := &envelope.Event{
			SchemaVersion: envelope.SchemaVersion,
			Phase:         envelope.PhaseIncremental, Op: envelope.OpUpdate,
			DB: "db0", Type: "string", Key: "livet:", Command: "SET",
			Args:   [][]byte{[]byte("SET"), []byte("livet:"), []byte("v")},
			Source: envelope.Source{ID: "live", DB: "db0", Filenum: 7, Offset: uint64(1000 + i), Seq: uint64(i)},
		}
		if err := s.Emit(ctx, ev); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}
	// wait for all ordinals acked: ring prefix reaches n completions
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, rerr, _ := s.ring.Advance(); rerr != nil {
			t.Fatalf("ring error: %v", rerr)
		}
		if s.ring.AdvanceConsumed() >= n {
			acked = n
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if acked != n {
		t.Fatalf("only %d/%d acked within deadline (sink err: %v)", acked, n, s.Err())
	}
}
