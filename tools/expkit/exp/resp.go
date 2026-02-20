// Package exp is the standalone RESP/binlog plumbing for the consistency
// experiment tools. It is deliberately independent of the product packages
// (except generated protobuf messages) so experiment results cannot be
// biased by bugs in the code under test.
package exp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Val is one RESP2 reply.
type Val struct {
	T     byte // '+', '-', ':', '$', '*'
	S     string
	I     int64
	Items []Val
	Nil   bool
}

// Err reports a RESP error reply as a Go error.
func (v Val) Err() error {
	if v.T == '-' {
		return errors.New(v.S)
	}
	return nil
}

// StrVal returns the bulk/simple string payload.
func (v Val) StrVal() string { return v.S }

// IsNil reports a nil bulk reply or empty multi-bulk.
func (v Val) IsNil() bool { return v.Nil }

// Client is a minimal RESP2 client with pipelining.
type Client struct {
	c net.Conn
	r *bufio.Reader
}

// Dial connects with timeouts applied per call.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{c: c, r: bufio.NewReader(c)}, nil
}

func (cl *Client) Close() error { return cl.c.Close() }

// Do sends one command and reads one reply.
func (cl *Client) Do(args ...string) (Val, error) {
	if err := cl.writeCmd(args); err != nil {
		return Val{}, err
	}
	return cl.readReply()
}

// Pipeline writes all commands then reads all replies in order.
func (cl *Client) Pipeline(cmds [][]string) ([]Val, error) {
	for _, c := range cmds {
		if err := cl.writeCmd(c); err != nil {
			return nil, err
		}
	}
	out := make([]Val, 0, len(cmds))
	for range cmds {
		v, err := cl.readReply()
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (cl *Client) writeCmd(args []string) error {
	var b strings.Builder
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	_, err := io.WriteString(cl.c, b.String())
	return err
}

func (cl *Client) readLine() (string, error) {
	line, err := cl.r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(line) > 1<<24 {
		return "", errors.New("resp: line too long")
	}
	return strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r"), nil
}

func (cl *Client) readReply() (Val, error) {
	line, err := cl.readLine()
	if err != nil {
		return Val{}, err
	}
	if len(line) == 0 {
		return Val{}, errors.New("resp: empty line")
	}
	switch line[0] {
	case '+':
		return Val{T: '+', S: line[1:]}, nil
	case '-':
		return Val{T: '-', S: line[1:]}, nil
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return Val{}, err
		}
		return Val{T: ':', I: n}, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Val{}, err
		}
		if n < 0 {
			return Val{T: '$', Nil: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(cl.r, buf); err != nil {
			return Val{}, err
		}
		return Val{T: '$', S: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Val{}, err
		}
		if n < 0 {
			return Val{T: '*', Nil: true}, nil
		}
		items := make([]Val, 0, n)
		for i := 0; i < n; i++ {
			v, err := cl.readReply()
			if err != nil {
				return Val{}, err
			}
			items = append(items, v)
		}
		return Val{T: '*', Items: items}, nil
	}
	return Val{}, fmt.Errorf("resp: bad prefix %q", line)
}
