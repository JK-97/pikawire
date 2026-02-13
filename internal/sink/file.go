// Package sink implements replica.Sink backends.
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/jk-97/pikawire/internal/envelope"
)

// FileSink appends events as JSON lines.
type FileSink struct {
	f         *os.File
	w         *bufio.Writer
	mu        sync.Mutex
	heartbeat bool
}

// NewFileSink opens path in append mode.
func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sink: file: %w", err)
	}
	return &FileSink{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

// IncludeHeartbeats opts heartbeat events into the stream.
func (s *FileSink) IncludeHeartbeats(v bool) { s.heartbeat = v }

// Emit implements replica.Sink.
func (s *FileSink) Emit(ctx context.Context, ev *envelope.Event) error {
	if !s.heartbeat && ev.Phase == envelope.PhaseHeartbeat {
		return nil
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.w.Flush() // sync durability for checkpoint coupling
}

// Close flushes and closes.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
