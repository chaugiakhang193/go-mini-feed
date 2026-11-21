// Command mcastdump joins the multicast groups of a market file and prints
// every datagram it receives on one line, SOH shown as "|". It is a lab, not a
// service: the printed lines are its output, so it writes them to stdout
// instead of logging them.
//
// Each group has its own reader goroutine. The readers copy what they receive
// into one shared channel and a single goroutine prints from it, so lines of
// different groups never interleave.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

const (
	transportMulticast = "multicast"
	transportUnicast   = "unicast"

	// maxDatagram is the largest UDP payload. A smaller read buffer would
	// truncate a long datagram without an error.
	maxDatagram = 65535

	// queueSize is the capacity of the channel between the readers and the
	// printer.
	queueSize = 1024
)

// packet is one received datagram and the group it arrived on.
type packet struct {
	group string
	data  []byte
}

// listener is an open socket and the name of its group.
type listener struct {
	group string
	conn  *net.UDPConn
}

func main() {
	multicastPath := flag.String("multicast", "configs/multicast/demo.yaml", "multicast group file")
	transport := flag.String("transport", transportMulticast, "multicast (join the groups of the file) or unicast (listen on 127.0.0.1, same ports)")
	count := flag.Int("count", 0, "exit after this many packets, 0 for no limit")
	flag.Parse()

	if *transport != transportMulticast && *transport != transportUnicast {
		log.Fatalf("-transport %q: want %s or %s", *transport, transportMulticast, transportUnicast)
	}
	if *count < 0 {
		log.Fatalf("-count %d is negative", *count)
	}
	mc, err := config.LoadMulticast(*multicastPath)
	if err != nil {
		log.Fatal(err)
	}
	ls, err := open(mc, *transport)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dump(ctx, ls, *count, os.Stdout)
}

// open creates one socket per group of the file.
func open(mc *config.Multicast, transport string) ([]listener, error) {
	var ls []listener
	for _, g := range mc.Groups {
		conn, err := listen(g, transport)
		if err != nil {
			closeAll(ls)
			return nil, fmt.Errorf("group %s: %w", g.Name, err)
		}
		ls = append(ls, listener{group: g.Name, conn: conn})
	}
	return ls, nil
}

func listen(g config.Group, transport string) (*net.UDPConn, error) {
	if transport == transportUnicast {
		return net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: g.Port})
	}
	ip := net.ParseIP(g.Address)
	if ip == nil {
		return nil, fmt.Errorf("invalid address %q", g.Address)
	}
	return net.ListenMulticastUDP("udp4", nil, &net.UDPAddr{IP: ip, Port: g.Port})
}

func closeAll(ls []listener) {
	for _, l := range ls {
		l.conn.Close()
	}
}

// dump prints the packets of every listener to out until ctx ends or count
// packets have been printed (count 0 means no limit). It closes the sockets
// and returns once every reader goroutine has exited.
func dump(ctx context.Context, ls []listener, count int, out io.Writer) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Read does not take a context. Closing the socket is what makes a
	// blocked Read return.
	go func() {
		<-ctx.Done()
		closeAll(ls)
	}()

	packets := make(chan packet, queueSize)
	var readers sync.WaitGroup
	for _, l := range ls {
		readers.Add(1)
		go func() {
			defer readers.Done()
			read(ctx, l, packets)
		}()
	}
	// The channel is closed by the one goroutine that knows no reader is
	// left to send on it.
	go func() {
		readers.Wait()
		close(packets)
	}()

	printed := 0
	for p := range packets {
		fmt.Fprintln(out, describe(p))
		printed++
		if count > 0 && printed >= count {
			break
		}
	}
	cancel()
	readers.Wait()
}

// read forwards the datagrams of one socket until the socket is closed.
func read(ctx context.Context, l listener, out chan<- packet) {
	buf := make([]byte, maxDatagram)
	for {
		n, err := l.conn.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("op=read_failed group=%s err=%v", l.group, err)
			}
			return
		}
		// buf is reused by the next Read, so the printer gets its own copy.
		p := packet{group: l.group, data: append([]byte(nil), buf[:n]...)}
		select {
		case out <- p:
		case <-ctx.Done():
			return
		}
	}
}

// describe is the printed line of a packet: group, length, tag 34 and the
// message with SOH shown as "|". A datagram that is not tag=value text prints
// with seq=?.
func describe(p packet) string {
	return fmt.Sprintf("group=%s len=%d seq=%s %s", p.group, len(p.data), sequence(p.data), readable(p.data))
}

// sequence returns tag 34 of the message, or "?" when the bytes do not split
// into fields or carry no tag 34. Only the tag=value syntax is checked: a lab
// that shows damaged packets has to show them.
func sequence(data []byte) string {
	fields, err := fix.Split(data)
	if err != nil {
		return "?"
	}
	for _, f := range fields {
		if f.Tag == fix.TagMsgSeqNum {
			return f.Value
		}
	}
	return "?"
}

// lineEscapes shows SOH as "|" and writes CR and LF as the two-character
// escapes \r and \n. A datagram that carries a line break still prints on one
// line.
var lineEscapes = strings.NewReplacer(string(fix.SOH), "|", "\r", `\r`, "\n", `\n`)

func readable(data []byte) string {
	return lineEscapes.Replace(string(data))
}
