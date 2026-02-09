package resp

import (
	"errors"
	"fmt"
)

var ErrInvalidRESP = errors.New("invalid RESP")

// ParseArray parses a RESP Array of Bulk Strings:
//
//	*<n>\r\n$<len>\r\n<data>\r\n ... (n times)
//
// It returns slices that reference the input buffer.
func ParseArray(content []byte) ([][]byte, error) {
	if len(content) == 0 || content[0] != '*' {
		return nil, fmt.Errorf("%w: missing array prefix", ErrInvalidRESP)
	}
	pos := 1
	n, next, err := parseIntLine(content, pos)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("%w: negative array length", ErrInvalidRESP)
	}
	pos = next

	argv := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		if pos >= len(content) || content[pos] != '$' {
			return nil, fmt.Errorf("%w: missing bulk prefix", ErrInvalidRESP)
		}
		pos++
		blen, next, err := parseIntLine(content, pos)
		if err != nil {
			return nil, err
		}
		if blen < 0 {
			return nil, fmt.Errorf("%w: negative bulk length", ErrInvalidRESP)
		}
		pos = next

		if int64(len(content)-pos) < blen+2 {
			return nil, fmt.Errorf("%w: bulk data truncated", ErrInvalidRESP)
		}
		arg := content[pos : pos+int(blen)]
		pos += int(blen)
		if content[pos] != '\r' || content[pos+1] != '\n' {
			return nil, fmt.Errorf("%w: bulk missing CRLF", ErrInvalidRESP)
		}
		pos += 2
		argv = append(argv, arg)
	}
	if pos != len(content) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrInvalidRESP)
	}
	return argv, nil
}

func parseIntLine(content []byte, start int) (int64, int, error) {
	if start >= len(content) {
		return 0, 0, fmt.Errorf("%w: missing integer", ErrInvalidRESP)
	}
	i := start
	neg := false
	if content[i] == '-' {
		neg = true
		i++
		if i >= len(content) {
			return 0, 0, fmt.Errorf("%w: invalid integer", ErrInvalidRESP)
		}
	}
	if i >= len(content) || content[i] < '0' || content[i] > '9' {
		return 0, 0, fmt.Errorf("%w: invalid integer", ErrInvalidRESP)
	}
	var v int64
	for i < len(content) && content[i] >= '0' && content[i] <= '9' {
		d := int64(content[i] - '0')
		if v > (1<<63-1-d)/10 {
			return 0, 0, fmt.Errorf("%w: integer overflow", ErrInvalidRESP)
		}
		v = v*10 + d
		i++
	}
	if i+1 >= len(content) || content[i] != '\r' || content[i+1] != '\n' {
		return 0, 0, fmt.Errorf("%w: missing CRLF", ErrInvalidRESP)
	}
	i += 2
	if neg {
		v = -v
	}
	return v, i, nil
}
