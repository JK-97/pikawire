// Package redisclient is a minimal RESP client with pipelining, used by the
// snapshot engine (SCAN/PEEK read commands). It is intentionally small:
// connect, auth, issue batched commands, read replies in order.
package redisclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/jk-97/pikawire/internal/resp"
)

// Client is a single-connection RESP client (not safe for concurrent use).
type Client struct {
	conn net.Conn
	br   *bufio.Reader
}

// Dial connects (and authenticates when passwd is non-empty).
func Dial(ctx context.Context, addr, passwd string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("redisclient: dial %s: %w", addr, err)
	}
	_ = c.SetDeadline(time.Now().Add(timeout))
	cl := &Client{conn: c, br: bufio.NewReader(c)}
	if passwd != "" {
		replies, err := cl.Do([][][]byte{{[]byte("AUTH"), []byte(passwd)}})
		if err != nil {
			cl.Close()
			return nil, fmt.Errorf("redisclient: auth: %w", err)
		}
		if err := replyErr(replies[0]); err != nil {
			cl.Close()
			return nil, fmt.Errorf("redisclient: auth rejected: %w", err)
		}
	}
	return cl, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// SetDeadline applies a deadline to the underlying connection.
func (c *Client) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// Do writes all commands then reads len(cmds) replies, preserving order.
// A reply element is one of:
//
//	nil            bulk nil
//	[]byte         bulk string / integer / status / error text
//	[][]Reply...   array (recursively same types)
//	ErrorValue     server error (also returned via replyErr)
func (c *Client) Do(cmds [][]([]byte)) ([]Value, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	var buf []byte
	for _, argv := range cmds {
		buf = append(buf, resp.SerializeArray(argv)...)
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.conn.Write(buf); err != nil {
		return nil, fmt.Errorf("redisclient: write: %w", err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	out := make([]Value, 0, len(cmds))
	for range cmds {
		v, err := readValue(c.br)
		if err != nil {
			return nil, fmt.Errorf("redisclient: read reply %d/%d: %w", len(out)+1, len(cmds), err)
		}
		out = append(out, v)
	}
	return out, nil
}

// Value is a decoded RESP value.
type Value struct {
	// Exactly one of Str / Arr / Err is meaningful, discriminated by Kind.
	Kind Kind
	Str  []byte
	Arr  []Value
	Err  error
}

type Kind int

const (
	KindNil Kind = iota
	KindBytes
	KindArray
	KindError
)

func (v Value) IsNil() bool  { return v.Kind == KindNil }
func (v Value) Text() string { return string(v.Str) }
func (v Value) AsInt() (int64, error) {
	if v.Kind != KindBytes {
		return 0, fmt.Errorf("redisclient: not an integer reply: %d", v.Kind)
	}
	return strconv.ParseInt(string(v.Str), 10, 64)
}
func (v Value) IsError() bool { return v.Kind == KindError }

func replyErr(v Value) error {
	if v.Kind == KindError {
		return v.Err
	}
	return nil
}

// readValue reads one RESP2 value.
func readValue(br *bufio.Reader) (Value, error) {
	prefix, err := br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch prefix {
	case '$':
		line, err := readLine(br)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: KindNil}, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(br, buf); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBytes, Str: buf[:n]}, nil
	case '*':
		line, err := readLine(br)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: KindNil}, nil
		}
		arr := make([]Value, 0, n)
		for i := 0; i < n; i++ {
			v, err := readValue(br)
			if err != nil {
				return Value{}, err
			}
			arr = append(arr, v)
		}
		return Value{Kind: KindArray, Arr: arr}, nil
	case ':':
		line, err := readLine(br)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBytes, Str: line}, nil
	case '+':
		line, err := readLine(br)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBytes, Str: line}, nil
	case '-':
		line, err := readLine(br)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindError, Str: line, Err: errors.New(string(line))}, nil
	default:
		return Value{}, fmt.Errorf("redisclient: unexpected reply type %q", prefix)
	}
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("redisclient: invalid line ending")
	}
	return line[:len(line)-2], nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
