// Package store persists the delivery checkpoint and snapshot phase so the
// process can resume after a crash without a source-side cursor.
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SnapshotPhase tracks the full-scan lifecycle. "done" survives restarts; a
// run that restarts mid-snapshot resets to "running" and re-scans (events are
// idempotent full images).
type SnapshotPhase string

const (
	PhaseNone    SnapshotPhase = ""
	PhaseRunning SnapshotPhase = "running"
	PhaseDone    SnapshotPhase = "done"
)

// State is the persisted record.
type State struct {
	Version  int           `json:"version"`
	SourceID string        `json:"source_id"`
	HasPos   bool          `json:"has_pos"`  // whether Filenum/Offset are meaningful
	Filenum  uint32        `json:"filenum"`  // last delivered binlog position
	Offset   uint64        `json:"offset"`   // last delivered binlog position
	Snapshot SnapshotPhase `json:"snapshot"` // snapshot lifecycle
	// Resume carries in-progress snapshot coordinates for durable
	// (buffer_fsync=durable) crash-resume: continue the dump from
	// TypeName/Key against the already-fetched DumpDir; the stream
	// reattaches at the fsync'd backlog tail (discovered by scanning the
	// retained segments). Nil for non-durable runs (crash = full re-scan,
	// unchanged) and cleared on completion.
	Resume *SnapshotResume `json:"resume,omitempty"`
}

// SnapshotResume is the mid-dump resume coordinate set.
type SnapshotResume struct {
	AnchorFilenum uint32 `json:"anchor_filenum"`
	AnchorOffset  uint64 `json:"anchor_offset"`
	TypeName      string `json:"type"` // dump engine of the last emitted record
	Key           string `json:"key"`  // last emitted dump key (inclusive re-emit: full images are idempotent)
	DumpDone      bool   `json:"dump_done"`
	DumpDir       string `json:"dump_dir"`
}

// Store is a concurrency-safe, atomic-write JSON file store.
type Store struct {
	mu    sync.Mutex
	path  string
	state State
	dirty bool
}

// Open loads path if it exists; missing file yields an empty state.
func Open(path, sourceID string) (*Store, error) {
	s := &Store{path: path, state: State{Version: 1, SourceID: sourceID}}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read: %w", err)
	}
	if err := json.Unmarshal(b, &s.state); err != nil {
		return nil, fmt.Errorf("store: parse %s: %w", path, err)
	}
	if s.state.SourceID != sourceID {
		return nil, fmt.Errorf("store: checkpoint source_id %q does not match current source %q", s.state.SourceID, sourceID)
	}
	return s, nil
}

// State returns a copy of the current state.
func (s *Store) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SetDelivered records a delivered binlog position (monotonic).
func (s *Store) SetDelivered(filenum uint32, offset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.HasPos && (filenum < s.state.Filenum || (filenum == s.state.Filenum && offset <= s.state.Offset)) {
		return
	}
	s.state.Filenum, s.state.Offset, s.state.HasPos = filenum, offset, true
	s.dirty = true
}

// SetSnapshot updates the snapshot phase (monotonic forward-only transitions
// are the caller's responsibility).
func (s *Store) SetSnapshot(p SnapshotPhase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Snapshot = p
	s.dirty = true
}

// ResetForRescan clears the delivery position and marks snapshot running;
// called when a full re-scan starts (events re-converge downstream). It also
// drops any resume coordinates: the rescan starts from scratch.
func (s *Store) ResetForRescan() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Filenum, s.state.Offset = 0, 0
	s.state.HasPos = false
	s.state.Snapshot = PhaseRunning
	s.state.Resume = nil
	s.dirty = true
}

// SetResume records/updates the durable mid-dump resume coordinates.
func (s *Store) SetResume(r *SnapshotResume) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Resume = r
	s.dirty = true
}

// ClearResume drops the resume coordinates (snapshot completed or discarded).
func (s *Store) ClearResume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Resume == nil {
		return
	}
	s.state.Resume = nil
	s.dirty = true
}

// Flush persists if dirty. Writes are atomic (tmp+rename+fsync).
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("store: tmp: %w", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("store: rename: %w", err)
	}
	s.dirty = false
	return nil
}
