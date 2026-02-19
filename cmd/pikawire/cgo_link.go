//go:build pikadump

// Link the cgo dump reader (RocksDB parser) into the binary when built with
// -tags pikadump; the default tagless build excludes this file entirely.
package main

import _ "github.com/jk-97/pikawire/internal/dbsync/cgoadapter"
