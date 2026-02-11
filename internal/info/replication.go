package info

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jk-97/pikawire/internal/resp"
)

type ReplicationInfo struct {
	HasBinlogOffset bool
	Filenum         uint32
	Offset          uint64

	HasMasterLag bool
	MasterLag    int64
}

func ParseReplicationInfo(info string) (ReplicationInfo, error) {
	var parsed ReplicationInfo
	hasFilenum := false
	var filenum uint32

	scanner := bufio.NewScanner(strings.NewReader(info))
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		lowerLine := strings.ToLower(trimmed)
		if pos := strings.Index(lowerLine, "binlog_offset="); pos >= 0 {
			slice := trimmed[pos+len("binlog_offset="):]
			nums := extractNumbers(slice)
			if len(nums) >= 2 {
				if nums[0] > math.MaxUint32 {
					return ReplicationInfo{}, fmt.Errorf("binlog_offset filenum overflow")
				}
				parsed.Filenum = uint32(nums[0])
				parsed.Offset = nums[1]
				parsed.HasBinlogOffset = true
			} else if len(nums) == 1 {
				parsed.Offset = nums[0]
				if hasFilenum {
					parsed.Filenum = filenum
				}
				parsed.HasBinlogOffset = true
			}
		}

		if pos := strings.Index(lowerLine, "lag=("); pos >= 0 {
			slice := trimmed[pos+len("lag=("):]
			nums := extractNumbers(slice)
			for _, n := range nums {
				if !parsed.HasMasterLag || int64(n) > parsed.MasterLag {
					parsed.MasterLag = int64(n)
					parsed.HasMasterLag = true
				}
			}
		}

		colon := strings.IndexByte(trimmed, ':')
		if colon < 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(trimmed[:colon]))
		value := strings.TrimSpace(trimmed[colon+1:])
		switch key {
		case "binlog_offset":
			nums := extractNumbers(value)
			if len(nums) >= 2 {
				if nums[0] > math.MaxUint32 {
					return ReplicationInfo{}, fmt.Errorf("binlog_offset filenum overflow")
				}
				parsed.Filenum = uint32(nums[0])
				parsed.Offset = nums[1]
				parsed.HasBinlogOffset = true
			} else if len(nums) == 1 {
				parsed.Offset = nums[0]
				if hasFilenum {
					parsed.Filenum = filenum
				}
				parsed.HasBinlogOffset = true
			}
		case "binlog_file_num", "binlog_filenum", "binlog_file":
			nums := extractNumbers(value)
			if len(nums) > 0 {
				if nums[0] > math.MaxUint32 {
					return ReplicationInfo{}, fmt.Errorf("binlog_filenum overflow")
				}
				filenum = uint32(nums[0])
				hasFilenum = true
				if parsed.HasBinlogOffset && parsed.Filenum == 0 {
					parsed.Filenum = filenum
				}
			}
		case "master_lag":
			nums := extractNumbers(value)
			if len(nums) > 0 {
				parsed.MasterLag = int64(nums[0])
				parsed.HasMasterLag = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return ReplicationInfo{}, err
	}
	if parsed.HasBinlogOffset || parsed.HasMasterLag {
		return parsed, nil
	}
	return ReplicationInfo{}, fmt.Errorf("replication info missing binlog_offset/master_lag")
}

func FetchReplicationInfo(ctx context.Context, host string, port int, passwd string, timeout time.Duration) (ReplicationInfo, error) {
	if host == "" || port == 0 {
		return ReplicationInfo{}, fmt.Errorf("host/port required")
	}
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return ReplicationInfo{}, fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(conn)

	if passwd != "" {
		if err := sendRedisCommand(conn, [][]byte{[]byte("auth"), []byte(passwd)}); err != nil {
			return ReplicationInfo{}, fmt.Errorf("auth send: %w", err)
		}
		typ, data, err := readRedisReply(br)
		if err != nil {
			return ReplicationInfo{}, fmt.Errorf("auth recv: %w", err)
		}
		if typ == '-' {
			return ReplicationInfo{}, fmt.Errorf("auth rejected: %s", string(data))
		}
		if !bytes.Equal(bytes.ToLower(data), []byte("ok")) {
			return ReplicationInfo{}, fmt.Errorf("auth rejected: %s", string(data))
		}
	}

	if err := sendRedisCommand(conn, [][]byte{[]byte("info"), []byte("replication")}); err != nil {
		return ReplicationInfo{}, fmt.Errorf("info replication send: %w", err)
	}
	typ, data, err := readRedisReply(br)
	if err != nil {
		return ReplicationInfo{}, fmt.Errorf("info replication recv: %w", err)
	}
	if typ == '-' {
		return ReplicationInfo{}, fmt.Errorf("info replication error: %s", string(data))
	}

	parsed, err := ParseReplicationInfo(string(data))
	if err != nil {
		return ReplicationInfo{}, err
	}
	return parsed, nil
}

func extractNumbers(value string) []uint64 {
	var out []uint64
	var current uint64
	inNumber := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= '0' && ch <= '9' {
			current = current*10 + uint64(ch-'0')
			inNumber = true
			continue
		}
		if inNumber {
			out = append(out, current)
			current = 0
			inNumber = false
		}
	}
	if inNumber {
		out = append(out, current)
	}
	return out
}

func sendRedisCommand(w net.Conn, argv [][]byte) error {
	if w == nil {
		return fmt.Errorf("nil conn")
	}
	_, err := w.Write(resp.SerializeArray(argv))
	return err
}

func readRedisReply(r *bufio.Reader) (typ byte, data []byte, err error) {
	if r == nil {
		return 0, nil, fmt.Errorf("nil reader")
	}
	prefix, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	switch prefix {
	case '+', '-', ':':
		line, err := readLine(r)
		return prefix, line, err
	case '$':
		line, err := readLine(r)
		if err != nil {
			return 0, nil, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid bulk len: %w", err)
		}
		if n < 0 {
			return '$', nil, nil
		}
		if n > int64(^uint(0)>>1)-2 {
			return 0, nil, fmt.Errorf("bulk too large: %d", n)
		}
		buf := make([]byte, int(n)+2)
		if _, err := ioReadFull(r, buf); err != nil {
			return 0, nil, err
		}
		if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
			return 0, nil, fmt.Errorf("bulk missing CRLF")
		}
		return '$', buf[:int(n)], nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return 0, nil, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid array len: %w", err)
		}
		if n <= 0 {
			return '*', nil, nil
		}
		var first []byte
		for i := int64(0); i < n; i++ {
			et, ed, err := readRedisReply(r)
			if err != nil {
				return 0, nil, err
			}
			if i == 0 {
				first = ed
				if et == '-' {
					return '-', ed, nil
				}
			}
		}
		return '*', first, nil
	default:
		return 0, nil, fmt.Errorf("unsupported resp prefix: %q", prefix)
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("resp line missing CRLF")
	}
	return line[:len(line)-2], nil
}

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
