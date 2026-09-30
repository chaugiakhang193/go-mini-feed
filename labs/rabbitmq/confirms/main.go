// Command confirms runs three experiments with publisher confirms.
//
// Throughput: n publishes without confirms, n that wait for each confirm,
// and n that collect the confirms at the end.
//
// Unroutable message: the confirm and the basic.return a producer gets, with
// and without the mandatory flag.
//
// Full queue: a queue with x-max-length=1 and x-overflow=reject-publish
// answers a publish with a nack once it holds a message.
//
// Run from the repository root with the local stack up (make up):
//
//	go run ./labs/rabbitmq/confirms -n 1000
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/chaugiakhang193/go-mini-feed/labs/rabbitmq/internal/rmqlab"
)

const (
	exchange  = "lab_confirm"
	mainQueue = "lab_confirm_q"
	mainKey   = "all"
	fullQueue = "lab_confirm_full"
	fullKey   = "full"
	noRoute   = "nobody"
)

func main() {
	n := flag.Int("n", 1000, "messages per throughput run")
	flag.Parse()

	conn, err := rmqlab.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// A channel cannot leave confirm mode. The run without confirms gets its
	// own channel.
	plain := openChannel(conn)
	defer plain.Close()
	confirming := openChannel(conn)
	defer confirming.Close()
	if err := confirming.Confirm(false); err != nil {
		log.Fatalf("enable confirm mode: %v", err)
	}
	returns := confirming.NotifyReturn(make(chan amqp.Return, 1))

	setUp(plain)

	fmt.Printf("== throughput, %d messages per run ==\n", *n)
	runNoConfirm(plain, *n)
	runConfirmEach(confirming, *n)
	runConfirmBatch(confirming, *n)
	// The messages of the first run were not confirmed and may still be on
	// their way to the queue.
	depth, err := rmqlab.WaitDepth(plain, mainQueue, 3**n, 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("messages in %s: %d (expected %d)\n", mainQueue, depth, 3**n)

	fmt.Println("\n== unroutable message ==")
	publishAndReport(confirming, returns, noRoute, false)
	publishAndReport(confirming, returns, noRoute, true)

	fmt.Println("\n== full queue (x-max-length=1, x-overflow=reject-publish) ==")
	publishAndReport(confirming, returns, fullKey, false)
	publishAndReport(confirming, returns, fullKey, false)
}

func openChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel: %v", err)
	}
	return ch
}

// setUp deletes the exchange and both queues and declares them again. Every
// run starts with empty queues.
func setUp(ch *amqp.Channel) {
	for _, q := range []string{mainQueue, fullQueue} {
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			log.Fatalf("delete queue %s: %v", q, err)
		}
	}
	if err := ch.ExchangeDelete(exchange, false, false); err != nil {
		log.Fatalf("delete exchange: %v", err)
	}
	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeTopic, false, false, false, false, nil); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}

	declareAndBind(ch, mainQueue, mainKey, nil)
	declareAndBind(ch, fullQueue, fullKey, amqp.Table{
		"x-max-length": int32(1),
		"x-overflow":   "reject-publish",
	})
}

// declareAndBind declares a durable queue and binds it. RabbitMQ 4.3.0 and
// later refuse non-durable, non-exclusive queues by default.
func declareAndBind(ch *amqp.Channel, queue, key string, args amqp.Table) {
	if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
		log.Fatalf("declare queue %s: %v", queue, err)
	}
	if err := ch.QueueBind(queue, key, exchange, false, nil); err != nil {
		log.Fatalf("bind queue %s: %v", queue, err)
	}
}

func runNoConfirm(ch *amqp.Channel, n int) {
	start := time.Now()
	for i := range n {
		if err := ch.PublishWithContext(context.Background(), exchange, mainKey, false, false, message(i)); err != nil {
			log.Fatalf("publish: %v", err)
		}
	}
	report("no confirm", n, 0, 0, time.Since(start))
}

// runConfirmEach waits for the confirm of a message before it sends the
// next one. That is one network round trip per message.
func runConfirmEach(ch *amqp.Channel, n int) {
	var acks, nacks int
	start := time.Now()
	for i := range n {
		dc, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), exchange, mainKey, false, false, message(i))
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		if dc.Wait() {
			acks++
		} else {
			nacks++
		}
	}
	report("confirm, wait each", n, acks, nacks, time.Since(start))
}

// runConfirmBatch sends all n messages and then waits for their confirms.
func runConfirmBatch(ch *amqp.Channel, n int) {
	var acks, nacks int
	pending := make([]*amqp.DeferredConfirmation, 0, n)
	start := time.Now()
	for i := range n {
		dc, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), exchange, mainKey, false, false, message(i))
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		pending = append(pending, dc)
	}
	for _, dc := range pending {
		if dc.Wait() {
			acks++
		} else {
			nacks++
		}
	}
	report("confirm, wait at end", n, acks, nacks, time.Since(start))
}

func report(name string, n, acks, nacks int, elapsed time.Duration) {
	perSec := float64(n) / elapsed.Seconds()
	fmt.Printf("%-22s %8s  %8.0f msg/s  acks=%d nacks=%d\n",
		name, elapsed.Round(time.Millisecond), perSec, acks, nacks)
}

func message(i int) amqp.Publishing {
	return amqp.Publishing{ContentType: "text/plain", Body: fmt.Appendf(nil, "message %d", i)}
}

// publishAndReport sends one message and prints its confirm and its
// basic.return, if there is one. The broker returns a message only when
// mandatory is set and no binding matched.
func publishAndReport(ch *amqp.Channel, returns <-chan amqp.Return, key string, mandatory bool) {
	dc, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, mandatory, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("key " + key)})
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	acked := dc.Wait()

	returned := "none"
	select {
	case r := <-returns:
		returned = fmt.Sprintf("%d %s", r.ReplyCode, r.ReplyText)
	default:
	}

	confirm := "nack"
	if acked {
		confirm = "ack"
	}
	fmt.Printf("key=%-7s mandatory=%-5t -> confirm=%s, basic.return=%s\n", key, mandatory, confirm, returned)
}
