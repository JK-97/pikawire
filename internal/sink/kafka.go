package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jk-97/pikawire/internal/envelope"

	kafka "github.com/segmentio/kafka-go"
)

// KafkaConfig configures the async partition-parallel kafka sink.
type KafkaConfig struct {
	Brokers        []string
	Topic          string
	ClientID       string
	RequiredAcks   int           // 0/1/-1 (all, default)
	WorkersPerPart int           // parallel producers per partition (default 2)
	BatchSize      int           // max messages per produce request (default 256)
	BatchTimeout   time.Duration // max queue wait before flush (default 20ms)
	QueueDepth     int           // per-worker buffer (default 512)
	Compression    string        // none|gzip|snappy|lz4|zstd (default none)
}

// ErrSinkStopped surfaces terminal produce failure to later Emit calls.
var ErrSinkStopped = errors.New("sink: kafka producer stopped after delivery failure")

// KafkaSink emits envelope events partitioned by entity key. Ordering per
// partition is preserved because each partition has dedicated ordered
// workers; throughput comes from batching + parallelism. Delivery
// confirmation flows through a global sequence ring so the checkpoint only
// advances over a contiguous acked prefix.
type KafkaSink struct {
	cfg   KafkaConfig
	w     *kafka.Writer
	ring  *seqRing
	topic string
	parts int

	mu      sync.Mutex
	queues  []chan kafkaItem // one per worker (partition-major)
	err     error
	started bool
	stop    chan struct{}
	wg      sync.WaitGroup

	notifier   func(envelope.Position) error
	notifyPoke chan struct{}
	includeHB  bool
	closeOnce  sync.Once
}

type kafkaItem struct {
	msg kafka.Message
	ord uint64
	pos envelope.Position
}

// NewKafkaSink connects eagerly to discover topic partitions.
func NewKafkaSink(ctx context.Context, cfg KafkaConfig) (*KafkaSink, error) {
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = -1
	}
	if cfg.WorkersPerPart <= 0 {
		cfg.WorkersPerPart = 2
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 20 * time.Millisecond
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 512
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	}
	switch cfg.Compression {
	case "", "none":
	case "gzip":
		w.Compression = kafka.Gzip
	case "snappy":
		w.Compression = kafka.Snappy
	case "lz4":
		w.Compression = kafka.Lz4
	case "zstd":
		w.Compression = kafka.Zstd
	default:
		_ = w.Close()
		return nil, fmt.Errorf("sink: unknown kafka compression %q (want gzip|snappy|lz4|zstd)", cfg.Compression)
	}
	if cfg.ClientID != "" {
		w.Transport = &kafka.Transport{ClientID: cfg.ClientID}
	}
	conn, err := kafka.DialContext(ctx, "tcp", cfg.Brokers[0])
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("sink: kafka dial: %w", err)
	}
	parts, err := conn.ReadPartitions(cfg.Topic)
	conn.Close()
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("sink: kafka partitions %s: %w", cfg.Topic, err)
	}
	if len(parts) == 0 {
		_ = w.Close()
		return nil, fmt.Errorf("sink: topic %s has no partitions (create it or enable auto-creation)", cfg.Topic)
	}
	s := &KafkaSink{
		cfg: cfg, w: w, ring: &seqRing{}, topic: cfg.Topic, parts: len(parts),
		stop: make(chan struct{}),
	}
	s.queues = make([]chan kafkaItem, len(parts)*cfg.WorkersPerPart)
	for i := range s.queues {
		s.queues[i] = make(chan kafkaItem, cfg.QueueDepth)
		part := i / cfg.WorkersPerPart
		for q := 0; q < cfg.WorkersPerPart; q++ {
			s.wg.Add(1)
			go s.worker(part, i)
		}
	}
	s.wg.Add(1)
	go s.notifyLoop()
	return s, nil
}

// SetDeliveryNotifier registers a callback invoked as the acked contiguous
// prefix advances (checkpoint/metrics integration).
func (s *KafkaSink) SetDeliveryNotifier(fn func(envelope.Position) error) {
	s.mu.Lock()
	s.notifier = fn
	s.mu.Unlock()
}

// IncludeHeartbeats opts heartbeat records into the kafka stream.
func (s *KafkaSink) IncludeHeartbeats(v bool) { s.includeHB = v }

// Err returns the terminal error if any batch failed to deliver.
func (s *KafkaSink) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Emit implements replica.Sink asynchronously: returns after enqueue (or a
// recorded terminal error).
func (s *KafkaSink) Emit(ctx context.Context, ev *envelope.Event) error {
	if err := s.Err(); err != nil {
		return err
	}
	if !s.includeHB && ev.Phase == envelope.PhaseHeartbeat {
		return nil
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	key := ev.KafkaKey()
	part := int(hashKey(key) % uint32(s.parts))
	worker := part*s.cfg.WorkersPerPart + int(hashKey(key)>>1%uint32(s.cfg.WorkersPerPart))

	var pos envelope.Position
	if ev.Source.Filenum != 0 || ev.Source.Offset != 0 {
		pos = envelope.Position{Filenum: ev.Source.Filenum, Offset: ev.Source.Offset}
	}
	ord := s.ring.Next()
	item := kafkaItem{
		msg: kafka.Message{Topic: s.topic, Partition: part, Key: key, Value: b},
		ord: ord, pos: pos,
	}
	q := s.queues[worker]
	select {
	case q <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stop:
		return ErrSinkStopped
	}
}

func (s *KafkaSink) worker(part, idx int) {
	defer s.wg.Done()
	q := s.queues[idx]
	batch := make([]kafkaItem, 0, s.cfg.BatchSize)
	var timer *time.Timer
	for {
		timerC := (<-chan time.Time)(nil)
		if timer != nil {
			timerC = timer.C
		}
		select {
		case it := <-q:
			batch = append(batch, it)
			if timer == nil {
				timer = time.NewTimer(s.cfg.BatchTimeout)
			}
			if len(batch) >= s.cfg.BatchSize {
				s.flush(part, batch)
				batch = batch[:0]
				if timer != nil {
					timer.Stop()
					timer = nil
				}
			}
		case <-timerC:
			s.flush(part, batch)
			batch = batch[:0]
			timer = nil
		case <-s.stop:
			if timer != nil {
				timer.Stop()
			}
			for {
				select {
				case it := <-q:
					batch = append(batch, it)
				default:
					if len(batch) > 0 {
						s.flush(part, batch)
					}
					return
				}
				if len(batch) >= s.cfg.BatchSize {
					s.flush(part, batch)
					batch = batch[:0]
				}
			}
		}
	}
}

func (s *KafkaSink) flush(part int, batch []kafkaItem) {
	if len(batch) == 0 {
		return
	}
	msgs := make([]kafka.Message, len(batch))
	for i := range batch {
		msgs[i] = batch[i].msg
	}
	err := s.w.WriteMessages(context.Background(), msgs...)
	if err != nil {
		s.mu.Lock()
		if s.err == nil {
			s.err = fmt.Errorf("sink: kafka produce partition %d x%d: %w", part, len(batch), err)
		}
		s.mu.Unlock()
	}
	for _, it := range batch {
		s.ring.Done(it.ord, it.pos, err)
	}
	s.signalNotify()
}

// notifyLoop advances the delivery notifier over the ring prefix.
func (s *KafkaSink) notifyLoop() {
	defer s.wg.Done()
	poke := make(chan struct{}, 1)
	s.mu.Lock()
	s.notifyPoke = poke
	s.mu.Unlock()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-poke:
		case <-t.C:
		case <-s.stop:
			s.drainPrefix()
			return
		}
		s.drainPrefix()
	}
}

func (s *KafkaSink) signalNotify() {
	s.mu.Lock()
	p := s.notifyPoke
	s.mu.Unlock()
	if p != nil {
		select {
		case p <- struct{}{}:
		default:
		}
	}
}

func (s *KafkaSink) drainPrefix() {
	for {
		pos, err, advanced := s.ring.Advance()
		if !advanced {
			return
		}
		s.mu.Lock()
		fn := s.notifier
		s.mu.Unlock()
		if fn != nil && !pos.IsZero() {
			_ = fn(pos) // commit the durable prefix even when a gap failed
		}
		if err != nil {
			return // terminal error recorded on the sink; stop advancing
		}
	}
}

// Close drains and stops all workers.
func (s *KafkaSink) Close() error {
	s.closeOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
	s.ring.Close()
	err := s.w.Close()
	if e := s.Err(); e != nil {
		return e
	}
	return err
}

func hashKey(k []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(k)
	return h.Sum32()
}
