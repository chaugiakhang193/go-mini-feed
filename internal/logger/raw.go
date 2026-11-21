// Package logger keeps the raw feed on disk: every packet a listener records
// is appended to a per-day, per-group file by one background writer.
package logger

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chaugiakhang193/go-mini-feed/internal/multicast"
)

// flushInterval bounds how much of the raw feed a crash can lose. Flushing
// per packet would cost one write syscall per datagram.
const flushInterval = time.Second

var _ multicast.Recorder = (*Raw)(nil)

// Raw writes packets to <dir>/<YYYY-MM-DD>/<market>/<group>.raw, one line per
// packet:
//
//	<received at, RFC 3339 UTC> <payload length> <payload bytes>
//
// The payload is written unchanged, SOH and line breaks included. The length
// lets a reader split lines whose payload contains LF.
type Raw struct {
	dir     string
	market  string
	queue   chan multicast.Packet
	done    chan struct{}
	dropped atomic.Uint64

	// The fields below belong to the writer goroutine.
	day      string
	files    map[string]*rawFile
	failed   map[string]bool
	reported uint64
	line     []byte
	err      error
}

type rawFile struct {
	f *os.File
	w *bufio.Writer
}

// NewRaw starts the writer goroutine. queueSize is the number of packets that
// can wait for the disk before Record starts dropping.
func NewRaw(dir, market string, queueSize int) *Raw {
	return newRaw(dir, market, queueSize, nil)
}

// newRaw is NewRaw with a gate the writer waits on before it takes the first
// packet. Tests close the gate to release a writer they hold back.
func newRaw(dir, market string, queueSize int, gate <-chan struct{}) *Raw {
	r := &Raw{
		dir:    dir,
		market: market,
		queue:  make(chan multicast.Packet, queueSize),
		done:   make(chan struct{}),
		files:  make(map[string]*rawFile),
		failed: make(map[string]bool),
	}
	go r.run(gate)
	return r
}

// Record queues p for the writer without blocking. A full queue drops the
// packet and counts it, so a slow disk never stalls the socket reader.
func (r *Raw) Record(p multicast.Packet) bool {
	select {
	case r.queue <- p:
		return true
	default:
		r.dropped.Add(1)
		return false
	}
}

// Dropped returns the number of packets Record has dropped.
func (r *Raw) Dropped() uint64 {
	return r.dropped.Load()
}

// Close writes every queued packet, flushes and closes the files, and returns
// the first error of that final flush. Close must be called once, after every
// goroutine that calls Record has returned.
func (r *Raw) Close() error {
	close(r.queue)
	<-r.done
	return r.err
}

func (r *Raw) run(gate <-chan struct{}) {
	defer close(r.done)
	if gate != nil {
		<-gate
	}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case p, ok := <-r.queue:
			if !ok {
				r.err = r.closeFiles(false)
				r.reportDropped()
				return
			}
			r.write(p)
		case <-ticker.C:
			for path, rf := range r.files {
				if err := rf.w.Flush(); err != nil {
					r.fail(path, err)
				}
			}
			r.reportDropped()
		}
	}
}

func (r *Raw) write(p multicast.Packet) {
	at := p.ReceivedAt.UTC()
	// The date comes from the receive time, so a packet received just before
	// midnight lands in that day's file even when it is written after.
	day := at.Format(time.DateOnly)
	if day != r.day {
		r.closeFiles(true)
		r.day = day
	}
	path := filepath.Join(r.dir, day, r.market, p.Group+".raw")
	rf, err := r.open(path)
	if err != nil {
		r.fail(path, err)
		return
	}

	r.line = at.AppendFormat(r.line[:0], time.RFC3339Nano)
	r.line = append(r.line, ' ')
	r.line = strconv.AppendInt(r.line, int64(len(p.Data)), 10)
	r.line = append(r.line, ' ')
	r.line = append(r.line, p.Data...)
	r.line = append(r.line, '\n')
	if _, err := rf.w.Write(r.line); err != nil {
		r.fail(path, err)
	}
}

func (r *Raw) open(path string) (*rawFile, error) {
	if rf, ok := r.files[path]; ok {
		return rf, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	rf := &rawFile{f: f, w: bufio.NewWriter(f)}
	r.files[path] = rf
	return rf, nil
}

// closeFiles flushes and closes every open file. On a day change the errors
// are logged, since no caller waits for them. On Close they are returned.
func (r *Raw) closeFiles(logErrors bool) error {
	var errs []error
	for path, rf := range r.files {
		err := errors.Join(rf.w.Flush(), rf.f.Close())
		if err != nil {
			if logErrors {
				r.fail(path, err)
			}
			errs = append(errs, fmt.Errorf("raw log %s: %w", path, err))
		}
		delete(r.files, path)
	}
	return errors.Join(errs...)
}

// fail logs the first error of each file. A full disk fails every later write
// to the same file, and one line per packet would flood the log.
func (r *Raw) fail(path string, err error) {
	if r.failed[path] {
		return
	}
	r.failed[path] = true
	log.Printf("op=raw_log_write_failed file=%s err=%v", path, err)
}

// reportDropped logs the drops since the last report, at most once per flush
// interval.
func (r *Raw) reportDropped() {
	total := r.dropped.Load()
	if total == r.reported {
		return
	}
	log.Printf("op=raw_log_dropped count=%d total=%d", total-r.reported, total)
	r.reported = total
}
