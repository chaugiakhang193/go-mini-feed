// Command keyorder runs a keyed worker pool on a RabbitMQ queue and counts,
// per key, the results that come out of order or twice.
//
// One goroutine consumes the input queue. For each delivery it hashes the
// message key to a lane and sends the delivery to that lane. A lane is a
// buffered Go channel with one worker. Messages with the same key go to the
// same worker and are handled one at a time. Different keys run in parallel.
//
// Queue order holds for a key only while no delivery is requeued. A full lane
// requeues the delivery, and a later message with the same key can reach the
// lane first.
//
// -mode goroutine starts one goroutine per message instead.
//
// From the repository root with the local stack up (make up):
//
//	go run ./labs/rabbitmq/keyorder -phase seed
//	go run ./labs/rabbitmq/keyorder -phase run -mode lanes -lanes 4
//	go run ./labs/rabbitmq/keyorder -phase audit
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/chaugiakhang193/go-mini-feed/labs/rabbitmq/internal/rmqlab"
)

const (
	inQueue  = "lab_keyorder_in"
	outQueue = "lab_keyorder_out"
)

func main() {
	phase := flag.String("phase", "", "seed, run or audit")
	n := flag.Int("n", 2000, "messages to seed")
	keys := flag.Int("keys", 4, "distinct keys to seed, 1 to 26")
	mode := flag.String("mode", "lanes", "lanes or goroutine")
	lanes := flag.Int("lanes", 4, "number of lanes")
	buffer := flag.Int("buffer", 1000, "deliveries each lane can hold")
	prefetch := flag.Int("prefetch", 5000, "unacknowledged deliveries the broker may push")
	idle := flag.Duration("idle", 2*time.Second, "stop once no delivery has arrived for this long")
	flag.Parse()

	conn, err := rmqlab.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	switch *phase {
	case "seed":
		if *keys < 1 || *keys > 26 {
			log.Fatalf("-keys must be 1 to 26, got %d", *keys)
		}
		seed(conn, *n, *keys)
	case "run":
		if *lanes < 1 || *buffer < 0 || *prefetch < 1 {
			log.Fatalf("-lanes and -prefetch must be at least 1, -buffer at least 0")
		}
		run(conn, *mode, *lanes, *buffer, *prefetch, *idle)
	case "audit":
		audit(conn, *idle)
	default:
		log.Fatalf("-phase must be seed, run or audit, got %q", *phase)
	}
}

// seed deletes and declares both queues, then publishes n messages. The
// messages take the keys in turn (AAA, BBB, ..., AAA, BBB, ...) and seq counts
// up from 1 for each key.
func seed(conn *amqp.Connection, n, keyCount int) {
	ch := openChannel(conn)
	defer ch.Close()

	for _, q := range []string{inQueue, outQueue} {
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			log.Fatalf("delete queue %s: %v", q, err)
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			log.Fatalf("declare queue %s: %v", q, err)
		}
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirms: %v", err)
	}

	names := keyNames(keyCount)
	seqs := make(map[string]int, len(names))
	confirms := make([]*amqp.DeferredConfirmation, 0, n)
	for i := range n {
		key := names[i%len(names)]
		seqs[key]++
		body := fmt.Sprintf("key=%s|seq=%d", key, seqs[key])
		dc, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), "", inQueue, false, false,
			amqp.Publishing{Body: []byte(body)})
		if err != nil {
			log.Fatalf("publish %s: %v", body, err)
		}
		confirms = append(confirms, dc)
	}
	for _, dc := range confirms {
		if !dc.Wait() {
			log.Fatalf("broker nacked delivery tag %d", dc.DeliveryTag)
		}
	}
	fmt.Printf("seeded %d messages over %d keys %v into %s\n", n, len(names), names, inQueue)
}

// keyNames returns AAA, BBB, CCC... up to count names.
func keyNames(count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = strings.Repeat(string(rune('A'+i)), 3)
	}
	return names
}

// stats is shared by the consuming goroutine and the workers.
type stats struct {
	received atomic.Int64 // deliveries, redeliveries included
	refused  atomic.Int64 // handed back with a requeue because the lane was full
	acked    atomic.Int64
	lastAck  atomic.Int64 // unix nanoseconds
}

func run(conn *amqp.Connection, mode string, laneCount, buffer, prefetch int, idle time.Duration) {
	ch := openChannel(conn)
	defer ch.Close()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatalf("set prefetch: %v", err)
	}

	pubCh := openChannel(conn)
	defer pubCh.Close()
	pub := &publisher{ch: pubCh}

	deliveries, err := ch.Consume(inQueue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume %s: %v", inQueue, err)
	}

	var st stats
	start := time.Now()
	switch mode {
	case "lanes":
		perLane := runLanes(deliveries, laneCount, buffer, idle, pub, &st)
		fmt.Printf("mode=lanes lanes=%d buffer=%d prefetch=%d\n", laneCount, buffer, prefetch)
		printLanes(perLane)
	case "goroutine":
		runGoroutines(deliveries, idle, pub, &st)
		fmt.Printf("mode=goroutine prefetch=%d\n", prefetch)
	default:
		log.Fatalf("-mode must be lanes or goroutine, got %q", mode)
	}

	fmt.Printf("received %d deliveries, refused %d (lane full), acked %d",
		st.received.Load(), st.refused.Load(), st.acked.Load())
	if st.acked.Load() == 0 {
		fmt.Println()
		return
	}
	busy := time.Duration(st.lastAck.Load() - start.UnixNano())
	fmt.Printf(", last ack after %s\n", busy.Round(time.Millisecond))
}

// consume calls dispatch for every delivery, on the calling goroutine, until
// no delivery has arrived for idle.
func consume(deliveries <-chan amqp.Delivery, idle time.Duration, st *stats, dispatch func(amqp.Delivery)) {
	timer := time.NewTimer(idle)
	defer timer.Stop()
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				log.Print("delivery channel closed by the library")
				return
			}
			st.received.Add(1)
			dispatch(d)
			timer.Reset(idle)
		case <-timer.C:
			return
		}
	}
}

// runLanes starts one worker per lane and feeds the lanes from the consuming
// goroutine. The send into a lane does not wait. When the lane is full, the
// delivery goes back to the broker with a requeue. A slow key then cannot
// stall the consumer, and with it every other key.
//
// Workers acknowledge on the consumer's channel. In amqp091-go v1.15.0 Ack and
// Nack hold that channel's lock while they send.
func runLanes(deliveries <-chan amqp.Delivery, laneCount, buffer int, idle time.Duration, pub *publisher, st *stats) []int {
	lanes := make([]chan amqp.Delivery, laneCount)
	var wg sync.WaitGroup
	for i := range lanes {
		lanes[i] = make(chan amqp.Delivery, buffer)
		wg.Go(func() {
			for d := range lanes[i] {
				handle(d, i, pub, st)
			}
		})
	}

	// perLane is written only by the consuming goroutine.
	perLane := make([]int, laneCount)
	consume(deliveries, idle, st, func(d amqp.Delivery) {
		i := laneFor(field(d.Body, "key"), laneCount)
		select {
		case lanes[i] <- d:
			perLane[i]++
		default:
			st.refused.Add(1)
			if err := d.Nack(false, true); err != nil {
				log.Printf("nack: %v", err)
			}
		}
	})

	for _, lane := range lanes {
		close(lane)
	}
	wg.Wait()
	return perLane
}

// runGoroutines starts one goroutine per delivery. Two messages with the same
// key can finish in either order.
func runGoroutines(deliveries <-chan amqp.Delivery, idle time.Duration, pub *publisher, st *stats) {
	var wg sync.WaitGroup
	consume(deliveries, idle, st, func(d amqp.Delivery) {
		wg.Go(func() { handle(d, -1, pub, st) })
	})
	wg.Wait()
}

// laneFor maps a key to a lane with 32-bit FNV-1a. A key always maps to the
// same lane.
func laneFor(key string, lanes int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(lanes))
}

// handle sleeps 2 to 10 ms to simulate work, publishes the result and then
// acknowledges the input. If the process dies after the publish and before the
// ack, the broker delivers the input again and the result can appear twice.
// The result is published without confirms. The first copy may not have
// reached the broker either.
func handle(d amqp.Delivery, worker int, pub *publisher, st *stats) {
	time.Sleep(time.Duration(2+rand.IntN(9)) * time.Millisecond)

	result := fmt.Sprintf("%s|worker=%d", d.Body, worker)
	if err := pub.publish([]byte(result)); err != nil {
		log.Printf("publish result: %v", err)
		if err := d.Nack(false, true); err != nil {
			log.Printf("nack: %v", err)
		}
		return
	}
	if err := d.Ack(false); err != nil {
		log.Printf("ack: %v", err)
		return
	}
	st.acked.Add(1)
	st.lastAck.Store(time.Now().UnixNano())
}

// publisher serializes publishes from many goroutines onto one channel.
// amqp091-go v1.15.0 documents a Channel as unsafe to share between
// goroutines.
type publisher struct {
	mu sync.Mutex
	ch *amqp.Channel
}

func (p *publisher) publish(body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch.PublishWithContext(context.Background(), "", outQueue, false, false, amqp.Publishing{Body: body})
}

func printLanes(perLane []int) {
	used := 0
	for _, count := range perLane {
		if count > 0 {
			used++
		}
	}
	if len(perLane) <= 16 {
		fmt.Printf("lanes with work: %d of %d, deliveries per lane %v\n", used, len(perLane), perLane)
		return
	}
	fmt.Printf("lanes with work: %d of %d\n", used, len(perLane))
}

// tally is the audit result for one key. A duplicate repeats a seq that was
// already seen. An out-of-order result has a new seq that is lower than the
// highest seq seen so far. A result counts in at most one of the two.
type tally struct {
	total, outOfOrder, duplicate int
	maxSeq                       int
	seen                         map[int]bool
}

func newTally() *tally {
	return &tally{seen: make(map[int]bool)}
}

// add records one result. A repeated seq counts as a duplicate even when it is
// also lower than maxSeq.
func (t *tally) add(seq int) {
	t.total++
	switch {
	case t.seen[seq]:
		t.duplicate++
	case seq < t.maxSeq:
		t.outOfOrder++
	}
	t.seen[seq] = true
	t.maxSeq = max(t.maxSeq, seq)
}

// audit drains the result queue and tallies the results per key.
func audit(conn *amqp.Connection, idle time.Duration) {
	ch := openChannel(conn)
	defer ch.Close()
	deliveries, err := ch.Consume(outQueue, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume %s: %v", outQueue, err)
	}

	perKey := make(map[string]*tally)
	var st stats
	consume(deliveries, idle, &st, func(d amqp.Delivery) {
		key := field(d.Body, "key")
		seq, err := strconv.Atoi(field(d.Body, "seq"))
		if err != nil {
			log.Printf("result %q has no numeric seq", d.Body)
			return
		}
		t := perKey[key]
		if t == nil {
			t = newTally()
			perKey[key] = t
		}
		t.add(seq)
	})

	keys := make([]string, 0, len(perKey))
	for key := range perKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var sum tally
	fmt.Printf("%-5s %6s %12s %10s\n", "key", "total", "out_of_order", "duplicate")
	for _, key := range keys {
		t := perKey[key]
		fmt.Printf("%-5s %6d %12d %10d\n", key, t.total, t.outOfOrder, t.duplicate)
		sum.total += t.total
		sum.outOfOrder += t.outOfOrder
		sum.duplicate += t.duplicate
	}
	fmt.Printf("%-5s %6d %12d %10d\n", "all", sum.total, sum.outOfOrder, sum.duplicate)
}

// field returns the value of name in a body shaped like "key=AAA|seq=12", or
// "" when the body has no such field.
func field(body []byte, name string) string {
	for part := range strings.SplitSeq(string(body), "|") {
		if value, ok := strings.CutPrefix(part, name+"="); ok {
			return value
		}
	}
	return ""
}

func openChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel: %v", err)
	}
	return ch
}
