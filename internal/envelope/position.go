package envelope

// Position is a source binlog position (file number + byte offset).
type Position struct {
	Filenum uint32
	Offset  uint64
}

// After reports strict positional ordering a > b.
func (a Position) After(b Position) bool {
	return a.Filenum > b.Filenum || (a.Filenum == b.Filenum && a.Offset > b.Offset)
}

// IsZero reports the unset position.
func (a Position) IsZero() bool { return a.Filenum == 0 && a.Offset == 0 }
