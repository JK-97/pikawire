package sink

import (
	"context"
	"strings"
	"testing"
)

func TestKafkaUnknownCompressionRejected(t *testing.T) {
	_, err := NewKafkaSink(context.Background(), KafkaConfig{
		Brokers: []string{"127.0.0.1:1"}, Topic: "t", Compression: "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("want compression error, got %v", err)
	}
}
