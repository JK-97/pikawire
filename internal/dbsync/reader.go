package dbsync

// Record is one command synthesized from the dbsync dump (RocksDB image).
// RawRESP holds the exact RESP array (SET k v / HMSET k f v ... / EXPIRE k s)
// the C++ parser produced from stored state; TTL arrives as follow-up
// commands.
type Record struct {
	Type    string // string|hash|list|set|zset (metadata)
	Key     string
	RawRESP []byte
}

// Iterator walks all records of one database dump.
type Iterator interface {
	// Next returns the next record; ok=false at end (not an error).
	Next() (rec *Record, ok bool, err error)
	Close() error
}

// ReaderResume is the durable resume coordinate: continue iteration at the
// engine whose name equals TypeName, starting from Key inclusive (that one
// full-image re-emit is idempotent). Zero value = from the beginning.
type ReaderResume struct {
	TypeName string
	Key      string
}

// ReaderFactory opens a dump directory (the dbX folder produced by rsync)
// for the given logical db name, optionally resuming at a recorded position.
type ReaderFactory func(dbDir, dbName string, resume ReaderResume) (Iterator, error)

var factory ReaderFactory

var readerRef string

// SetReaderRef records a build stamp for the registered reader (e.g. the
// pikiwidb tag its libstorage was compiled against). Call from the reader's
// init; surfaces in logs and dump-layout error messages.
func SetReaderRef(ref string) { readerRef = ref }

// ReaderRef returns the registered reader's build stamp ("" when unset).
func ReaderRef() string { return readerRef }

// RegisterReader installs the dump reader implementation. The tagless build
// has none (dbsync engine unavailable); a `-tags pikadump` build registers a
// cgo RocksDB reader from init().
func RegisterReader(f ReaderFactory) { factory = f }

// ReaderAvailable reports whether this binary can parse dumps.
func ReaderAvailable() bool { return factory != nil }

// OpenIterator creates the registered reader.
func OpenIterator(dbDir, dbName string, resume ReaderResume) (Iterator, error) {
	if factory == nil {
		return nil, errNoReader
	}
	return factory(dbDir, dbName, resume)
}
