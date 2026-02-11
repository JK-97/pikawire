package binlog

import (
	"strconv"

	"github.com/jk-97/pikawire/internal/resp"
)

// MaybeTransformPksetexatRawRESP converts the raw RESP payload for pksetexat to
// a setex command with a relative TTL, matching the C++ behavior (argv is left
// unchanged).
func MaybeTransformPksetexatRawRESP(argv [][]byte, nowUnixSec uint64, rawRESP []byte) []byte {
	if len(argv) < 4 {
		return rawRESP
	}
	if string(argv[0]) != "pksetexat" {
		return rawRESP
	}
	expireAt, err := strconv.ParseInt(string(argv[2]), 10, 64)
	if err != nil {
		return rawRESP
	}
	ttl := expireAt - int64(nowUnixSec)
	ttlStr := strconv.FormatInt(ttl, 10)
	outArgv := [][]byte{
		[]byte("setex"),
		argv[1],
		[]byte(ttlStr),
		argv[3],
	}
	return resp.SerializeArray(outArgv)
}
