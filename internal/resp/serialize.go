package resp

import "strconv"

func SerializeArray(argv [][]byte) []byte {
	out := make([]byte, 0, 32*len(argv))
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(argv)), 10)
	out = append(out, '\r', '\n')
	for _, arg := range argv {
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(arg)), 10)
		out = append(out, '\r', '\n')
		out = append(out, arg...)
		out = append(out, '\r', '\n')
	}
	return out
}
