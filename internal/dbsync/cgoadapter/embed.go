//go:build pikadump

package cgoadapter

import "embed"

// Sidecar dumper executables produced by scripts/build-cgo.sh (git-ignored):
// one per supported pika storage-layout family, each statically linked
// against that version's pika storage layer. Embedded so the release
// artifact stays a single binary serving multiple pika majors.
//
//go:embed drivers/pikawire-dumper-v35 drivers/pikawire-dumper-v40
var driverFS embed.FS
