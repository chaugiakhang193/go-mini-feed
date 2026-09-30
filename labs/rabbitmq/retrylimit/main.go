// Command retrylimit shows two ways to stop the redelivery of a message that
// the consumer can never process.
//
// Scenario xdeath uses classic queues. A rejected message is dead-lettered
// to a wait queue, stays there for -ttl and is dead-lettered back to the
// work queue. The consumer reads the x-death header to see how often the
// message was rejected, and parks it on try number -attempts.
//
// Scenario limit uses a quorum queue with x-delivery-limit. It runs twice
// and returns the message with requeue set, first with basic.nack and then
// with basic.reject.
//
// Run from the repository root with the local stack up (make up):
//
//	go run ./labs/rabbitmq/retrylimit -scenario xdeath
//	go run ./labs/rabbitmq/retrylimit -scenario limit
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/chaugiakhang193/go-mini-feed/labs/rabbitmq/internal/rmqlab"
)

const (
	workExchange = "lab_retry"
	deadExchange = "lab_retry_dead"

	workQueue   = "lab_retry_work"
	waitQueue   = "lab_retry_wait"
	parkedQueue = "lab_retry_parked"
	quorumQueue = "lab_retry_quorum"
	limitQueue  = "lab_retry_over_limit"

	deadLetterExchangeArg = "x-dead-letter-exchange"
	deadLetterKeyArg      = "x-dead-letter-routing-key"
	deliveryLimitArg      = "x-delivery-limit"

	reasonRejected = "rejected"
)

func main() {
	scenario := flag.String("scenario", "xdeath", "xdeath or limit")
	attempts := flag.Int("attempts", 4, "xdeath: deliveries after which the message is parked")
	ttl := flag.Duration("ttl", 2*time.Second, "xdeath: wait between two tries")
	limit := flag.Int("limit", 3, "limit: x-delivery-limit of the quorum queue")
	giveUp := flag.Int("give-up", 10, "limit: deliveries after which the run acks the message and stops")
	flag.Parse()

	conn, err := rmqlab.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	admin := openChannel(conn)
	defer admin.Close()
	if err := admin.Confirm(false); err != nil {
		log.Fatalf("enable confirm mode: %v", err)
	}
	b := broker{conn: conn, admin: admin, returns: admin.NotifyReturn(make(chan amqp.Return, 1))}

	switch *scenario {
	case "xdeath":
		runXDeath(b, *attempts, *ttl)
	case "limit":
		runLimit(b, "basic.nack", *limit, *giveUp)
		fmt.Println()
		runLimit(b, "basic.reject", *limit, *giveUp)
	default:
		log.Fatalf("-scenario must be xdeath or limit, got %q", *scenario)
	}
}

// broker holds the connection and the channel that declares, publishes and
// inspects. That channel is in confirm mode. Consumers open their own.
type broker struct {
	conn    *amqp.Connection
	admin   *amqp.Channel
	returns <-chan amqp.Return
}

// runXDeath publishes one message and rejects it until its x-death header
// shows attempts-1 rejections. The next delivery is parked.
//
// The number of tries is read from the header on every delivery. A counter
// kept in the consumer would start from zero after a restart.
func runXDeath(b broker, attempts int, ttl time.Duration) {
	setUpXDeath(b.admin, ttl)
	b.publish(workExchange, workQueue, amqp.Publishing{ContentType: "text/plain", Body: []byte("cannot be parsed")})
	fmt.Printf("== classic queue, wait %s between tries, park at try %d ==\n", ttl, attempts)

	sub := openChannel(b.conn)
	defer sub.Close()
	deliveries := consume(sub, workQueue)

	start := time.Now()
	for n := 1; ; n++ {
		d, ok := next(deliveries, ttl+5*time.Second)
		if !ok {
			log.Fatalf("no delivery %d within %s", n, ttl+5*time.Second)
		}
		deaths := readDeaths(d.Headers)
		fmt.Printf("delivery %d at %5s  redelivered=%-5t x-death=%s\n",
			n, time.Since(start).Round(100*time.Millisecond), d.Redelivered, formatDeaths(deaths))

		rejectedBefore := countDeaths(deaths, workQueue, reasonRejected)
		if rejectedBefore+1 < attempts {
			if err := d.Nack(false, false); err != nil {
				log.Fatalf("nack: %v", err)
			}
			continue
		}

		// Ack once the parked copy is confirmed. If the consumer dies before
		// the ack, the message is still in the work queue.
		b.publish(deadExchange, parkedQueue, amqp.Publishing{ContentType: d.ContentType, Headers: d.Headers, Body: d.Body})
		if err := d.Ack(false); err != nil {
			log.Fatalf("ack: %v", err)
		}
		fmt.Printf("try %d reached the limit: parked and acked\n", rejectedBefore+1)
		break
	}

	for _, queue := range []string{workQueue, waitQueue, parkedQueue} {
		fmt.Printf("%-22s depth=%d\n", queue, depth(b.admin, queue))
	}
	msg, ok, err := b.admin.Get(parkedQueue, true)
	if err != nil || !ok {
		log.Fatalf("get from %s: ok=%t err=%v", parkedQueue, ok, err)
	}
	fmt.Printf("parked message         x-death=%s\n", formatDeaths(readDeaths(msg.Headers)))
}

// setUpXDeath declares the retry loop:
//
//	lab_retry --> lab_retry_work --rejected--> lab_retry_dead --> lab_retry_wait
//	    ^                                                              |
//	    +------------------------- TTL expired ------------------------+
//
// Nothing consumes from the wait queue. A message leaves it when its TTL
// expires and is then dead-lettered to the work exchange.
func setUpXDeath(ch *amqp.Channel, ttl time.Duration) {
	resetTopology(ch)
	declareAndBind(ch, workQueue, workExchange, amqp.Table{
		deadLetterExchangeArg: deadExchange,
		deadLetterKeyArg:      waitQueue,
	})
	declareAndBind(ch, waitQueue, deadExchange, amqp.Table{
		amqp.QueueMessageTTLArg: int32(ttl.Milliseconds()),
		deadLetterExchangeArg:   workExchange,
		deadLetterKeyArg:        workQueue,
	})
	declareAndBind(ch, parkedQueue, deadExchange, nil)
}

// runLimit publishes one message to a quorum queue and returns each delivery
// with requeue set, using method. It stops when no delivery arrives for two
// seconds. Delivery number giveUp is acked instead of returned, which also
// ends the run.
//
// RabbitMQ 4.3 compares x-delivery-limit with the x-delivery-count header.
// Each delivery is printed with that header and with x-acquired-count.
func runLimit(b broker, method string, limit, giveUp int) {
	setUpLimit(b.admin, limit)
	b.publish(workExchange, quorumQueue, amqp.Publishing{ContentType: "text/plain", Body: []byte("cannot be parsed")})
	fmt.Printf("== quorum queue, x-delivery-limit=%d, returned with %s requeue=true ==\n", limit, method)

	sub := openChannel(b.conn)
	defer sub.Close()
	deliveries := consume(sub, quorumQueue)

	for n := 1; ; n++ {
		d, ok := next(deliveries, 2*time.Second)
		if !ok {
			fmt.Printf("no delivery %d within 2s: the queue stopped delivering the message\n", n)
			break
		}
		fmt.Printf("delivery %2d  redelivered=%-5t x-delivery-count=%-5s x-acquired-count=%s\n",
			n, d.Redelivered, headerValue(d.Headers, "x-delivery-count"), headerValue(d.Headers, "x-acquired-count"))

		if n == giveUp {
			if err := d.Ack(false); err != nil {
				log.Fatalf("ack: %v", err)
			}
			fmt.Printf("gave up after %d deliveries: the limit never applied, acked to end the run\n", n)
			break
		}
		if err := returnToQueue(d, method); err != nil {
			log.Fatalf("%s: %v", method, err)
		}
	}

	// Poll for the dead-lettered message, which may not be in the queue yet.
	// After two seconds without one the depth is reported as zero.
	overLimit, _ := rmqlab.WaitDepth(b.admin, limitQueue, 1, 2*time.Second)
	fmt.Printf("%-22s depth=%d\n", quorumQueue, depth(b.admin, quorumQueue))
	fmt.Printf("%-22s depth=%d\n", limitQueue, overLimit)
	if overLimit == 0 {
		return
	}
	msg, ok, err := b.admin.Get(limitQueue, true)
	if err != nil || !ok {
		log.Fatalf("get from %s: ok=%t err=%v", limitQueue, ok, err)
	}
	fmt.Printf("dead-lettered message  x-death=%s\n", formatDeaths(readDeaths(msg.Headers)))
}

func setUpLimit(ch *amqp.Channel, limit int) {
	resetTopology(ch)
	declareAndBind(ch, quorumQueue, workExchange, amqp.Table{
		amqp.QueueTypeArg:     amqp.QueueTypeQuorum,
		deliveryLimitArg:      int32(limit),
		deadLetterExchangeArg: deadExchange,
		deadLetterKeyArg:      limitQueue,
	})
	declareAndBind(ch, limitQueue, deadExchange, nil)
}

func returnToQueue(d amqp.Delivery, method string) error {
	if method == "basic.reject" {
		return d.Reject(true)
	}
	return d.Nack(false, true)
}

// resetTopology deletes the queues and exchanges of the lab and declares the
// two exchanges again. The caller declares the queues it needs, with
// arguments taken from the flags of this run.
func resetTopology(ch *amqp.Channel) {
	for _, queue := range []string{workQueue, waitQueue, parkedQueue, quorumQueue, limitQueue} {
		if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
			log.Fatalf("delete queue %s: %v", queue, err)
		}
	}
	for _, exchange := range []string{workExchange, deadExchange} {
		if err := ch.ExchangeDelete(exchange, false, false); err != nil {
			log.Fatalf("delete exchange %s: %v", exchange, err)
		}
		if err := ch.ExchangeDeclare(exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			log.Fatalf("declare exchange %s: %v", exchange, err)
		}
	}
}

// declareAndBind declares a durable queue and binds it to exchange with the
// queue name as the routing key.
func declareAndBind(ch *amqp.Channel, queue, exchange string, args amqp.Table) {
	if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
		log.Fatalf("declare queue %s: %v", queue, err)
	}
	if err := ch.QueueBind(queue, queue, exchange, false, nil); err != nil {
		log.Fatalf("bind queue %s: %v", queue, err)
	}
}

func openChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel: %v", err)
	}
	return ch
}

// publish sends msg as mandatory and waits for the confirm. It exits the
// program on a nack or a basic.return.
//
// An unroutable message is acked like any other. RabbitMQ sends the
// basic.return before that ack, so when Wait returns the return is already
// in b.returns.
func (b broker) publish(exchange, key string, msg amqp.Publishing) {
	dc, err := b.admin.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, true, false, msg)
	if err != nil {
		log.Fatalf("publish to %s: %v", exchange, err)
	}
	if !dc.Wait() {
		log.Fatalf("publish to %s: broker answered with a nack", exchange)
	}
	select {
	case r := <-b.returns:
		log.Fatalf("publish to %s with key %s: returned %d %s", exchange, key, r.ReplyCode, r.ReplyText)
	default:
	}
}

// consume sets prefetch to 1 and starts a consumer without auto-ack. The
// broker sends the next message after the current one is acked, nacked or
// rejected.
func consume(ch *amqp.Channel, queue string) <-chan amqp.Delivery {
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("set prefetch: %v", err)
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume %s: %v", queue, err)
	}
	return deliveries
}

// next returns the next delivery, or false if none arrives within timeout.
func next(deliveries <-chan amqp.Delivery, timeout time.Duration) (amqp.Delivery, bool) {
	select {
	case d, ok := <-deliveries:
		return d, ok
	case <-time.After(timeout):
		return amqp.Delivery{}, false
	}
}

// headerValue formats a header for printing. A missing header prints as
// "unset".
func headerValue(headers amqp.Table, name string) string {
	value, ok := headers[name]
	if !ok {
		return "unset"
	}
	return fmt.Sprint(value)
}

func depth(ch *amqp.Channel, queue string) int {
	n, err := rmqlab.Depth(ch, queue)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

// death is one entry of the x-death header. count is the number of times
// the message was dead-lettered from queue for reason.
type death struct {
	queue  string
	reason string
	count  int64
}

// readDeaths returns the entries of the x-death header in header order.
// RabbitMQ puts the most recent one first. Entries that are not tables are
// skipped.
func readDeaths(headers amqp.Table) []death {
	entries, _ := headers["x-death"].([]any)
	deaths := make([]death, 0, len(entries))
	for _, entry := range entries {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		queue, _ := table["queue"].(string)
		reason, _ := table["reason"].(string)
		count, _ := table["count"].(int64)
		deaths = append(deaths, death{queue: queue, reason: reason, count: count})
	}
	return deaths
}

func countDeaths(deaths []death, queue, reason string) int {
	for _, d := range deaths {
		if d.queue == queue && d.reason == reason {
			return int(d.count)
		}
	}
	return 0
}

func formatDeaths(deaths []death) string {
	if len(deaths) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(deaths))
	for _, d := range deaths {
		parts = append(parts, fmt.Sprintf("%s/%s x%d", d.queue, d.reason, d.count))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
