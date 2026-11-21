package multicast

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// maxDatagram is the largest UDP payload. A smaller read buffer would
// truncate a long datagram without an error.
const maxDatagram = 65535

// Packet is one datagram received on a group. Data is owned by the packet and
// is shared read-only between the raw log and the downstream channel.
type Packet struct {
	Group      string
	ReceivedAt time.Time
	Data       []byte
}

// Recorder keeps a copy of received packets, such as a raw log. Record must
// not block. It returns false when the packet was dropped.
type Recorder interface {
	Record(Packet) bool
}

// reader is the part of *net.UDPConn the read loop uses. Tests replace it to
// inject read errors.
type reader interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}

// Listener reads the datagrams of one group's socket.
type Listener struct {
	group    string
	conn     reader
	deadline time.Duration
	rec      Recorder
}

// NewListener returns a listener for group that reads from conn. Each Read
// waits at most deadline, which bounds how long Run takes to notice a
// cancelled context. A nil rec records nothing.
func NewListener(group string, conn reader, deadline time.Duration, rec Recorder) *Listener {
	return &Listener{group: group, conn: conn, deadline: deadline, rec: rec}
}

// Run sends every datagram to out until ctx ends or the socket is closed, and
// then returns nil. Any other socket error is returned, since a socket that
// fails once usually keeps failing. A full out blocks Run, which keeps every
// packet and leaves the overflow to the kernel receive buffer.
func (l *Listener) Run(ctx context.Context, out chan<- Packet) error {
	buf := make([]byte, maxDatagram)
	for {
		if err := l.conn.SetReadDeadline(time.Now().Add(l.deadline)); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("group %s: set read deadline: %w", l.group, err)
		}
		n, err := l.conn.Read(buf)
		if err != nil {
			switch {
			case errors.Is(err, os.ErrDeadlineExceeded):
				if ctx.Err() != nil {
					return nil
				}
				continue
			case errors.Is(err, net.ErrClosed):
				return nil
			default:
				return fmt.Errorf("group %s: read: %w", l.group, err)
			}
		}

		// buf is reused by the next Read, so the packet gets its own copy.
		p := Packet{Group: l.group, ReceivedAt: time.Now().UTC(), Data: bytes.Clone(buf[:n])}
		if l.rec != nil {
			l.rec.Record(p)
		}
		select {
		case out <- p:
		case <-ctx.Done():
			return nil
		}
	}
}
