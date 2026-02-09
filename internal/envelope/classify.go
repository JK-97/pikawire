package envelope

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// Classify maps a raw PikiwiDB command to an Op hint.
//
// It is intentionally coarse: op is a convenience, consumers needing exact
// semantics must inspect Command. Full-image snapshot events use OpRead.
func Classify(command string) Op {
	switch strings.ToUpper(command) {
	case "DEL", "UNLINK", "FLUSHDB", "FLUSHALL":
		return OpDelete
	default:
		return OpUpdate
	}
}

// DecodeArg reverses the Args field of a decoded envelope when args_encoding
// is "base64" (caller knows the encoding from the JSON payload).
func DecodeArg(encoding string, value []byte) ([]byte, error) {
	if encoding != "base64" {
		return value, nil
	}
	out := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
	n, err := base64.StdEncoding.Decode(out, value)
	if err != nil {
		return nil, err
	}
	return out[:n], nil
}

// String returns a human-readable identity for debug/metrics.
func (e *Event) String() string {
	return string(e.Phase) + ":" + e.Command + " " + e.DB + "/" + e.Key +
		"@" + strconv.FormatUint(uint64(e.Source.Filenum), 10) + ":" + strconv.FormatUint(e.Source.Offset, 10)
}
