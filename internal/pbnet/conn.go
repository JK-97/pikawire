package pbnet

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type Conn struct {
	c net.Conn

	writeMu sync.Mutex
}

func Dial(addr string, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{c: c}, nil
}

func Wrap(c net.Conn) *Conn {
	return &Conn{c: c}
}

func (c *Conn) Close() error {
	if c == nil || c.c == nil {
		return nil
	}
	return c.c.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	if c == nil || c.c == nil {
		return nil
	}
	return c.c.LocalAddr()
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	if c == nil || c.c == nil {
		return fmt.Errorf("conn closed")
	}
	return c.c.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if c == nil || c.c == nil {
		return fmt.Errorf("conn closed")
	}
	return c.c.SetWriteDeadline(t)
}

func (c *Conn) Send(msg proto.Message) error {
	if c == nil || c.c == nil {
		return fmt.Errorf("conn closed")
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if len(data) > int(^uint32(0)) {
		return fmt.Errorf("message too large: %d", len(data))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := writeFull(c.c, header[:]); err != nil {
		return err
	}
	return writeFull(c.c, data)
}

func (c *Conn) Recv(msg proto.Message) error {
	if c == nil || c.c == nil {
		return fmt.Errorf("conn closed")
	}
	var header [4]byte
	if _, err := io.ReadFull(c.c, header[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(header[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.c, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf, msg)
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
