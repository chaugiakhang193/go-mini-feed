// Command durability counts which messages survive a broker restart, for
// every combination of queue type (classic, quorum) and delivery mode
// (transient, persistent), plus whether a non-durable exchange survives.
//
// The restart happens outside the program, between two phases. From the
// repository root with the local stack up (make up):
//
//	go run ./labs/rabbitmq/durability -phase publish
//	docker compose -f deployments/docker-compose.yml restart rabbitmq
//	go run ./labs/rabbitmq/durability -phase count
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
	exchange          = "lab_durable"
	transientExchange = "lab_transient"
)

// target is one combination of queue type and delivery mode. Each has its
// own queue, bound under the queue name.
type target struct {
	queue     string
	queueType string
	mode      uint8
}

var targets = []target{
	{queue: "lab_dur_classic_transient", queueType: amqp.QueueTypeClassic, mode: amqp.Transient},
	{queue: "lab_dur_classic_persistent", queueType: amqp.QueueTypeClassic, mode: amqp.Persistent},
	{queue: "lab_dur_quorum_transient", queueType: amqp.QueueTypeQuorum, mode: amqp.Transient},
	{queue: "lab_dur_quorum_persistent", queueType: amqp.QueueTypeQuorum, mode: amqp.Persistent},
}

func main() {
	phase := flag.String("phase", "", "publish or count")
	n := flag.Int("n", 100, "messages per queue")
	flag.Parse()

	conn, err := rmqlab.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel: %v", err)
	}
	defer ch.Close()

	switch *phase {
	case "publish":
		setUp(ch)
		publishAll(ch, *n)
	case "count":
		countAll(conn)
	default:
		log.Fatalf("-phase must be publish or count, got %q", *phase)
	}
}

// setUp deletes both exchanges and the four queues and declares them again.
// The queues and lab_durable are durable, lab_transient is not. On RabbitMQ
// 4.3.6 a passive declare still finds lab_transient after a restart.
func setUp(ch *amqp.Channel) {
	for _, t := range targets {
		if _, err := ch.QueueDelete(t.queue, false, false, false); err != nil {
			log.Fatalf("delete queue %s: %v", t.queue, err)
		}
	}
	for _, name := range []string{exchange, transientExchange} {
		if err := ch.ExchangeDelete(name, false, false); err != nil {
			log.Fatalf("delete exchange %s: %v", name, err)
		}
	}

	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}
	if err := ch.ExchangeDeclare(transientExchange, amqp.ExchangeTopic, false, false, false, false, nil); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}

	for _, t := range targets {
		args := amqp.Table{amqp.QueueTypeArg: t.queueType}
		if _, err := ch.QueueDeclare(t.queue, true, false, false, false, args); err != nil {
			log.Fatalf("declare queue %s: %v", t.queue, err)
		}
		if err := ch.QueueBind(t.queue, t.queue, exchange, false, nil); err != nil {
			log.Fatalf("bind queue %s: %v", t.queue, err)
		}
	}
}

// publishAll publishes n messages to each queue and waits for every
// confirm. The counts before the restart are then known, and a message
// missing afterwards was lost by the restart.
func publishAll(ch *amqp.Channel, n int) {
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirm mode: %v", err)
	}
	for _, t := range targets {
		acks := 0
		for i := range n {
			dc, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), exchange, t.queue, false, false,
				amqp.Publishing{
					ContentType:  "text/plain",
					DeliveryMode: t.mode,
					Body:         fmt.Appendf(nil, "%s %d", t.queue, i),
				})
			if err != nil {
				log.Fatalf("publish: %v", err)
			}
			if dc.Wait() {
				acks++
			}
		}
		depth, err := rmqlab.WaitDepth(ch, t.queue, n, 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-28s acks=%d depth=%d\n", t.queue, acks, depth)
	}
}

// countAll prints the depth of each queue and whether each exchange still
// exists. A passive declare that fails closes its channel. That happens for
// a queue that is still recovering and for a missing exchange, so every
// check opens a new channel.
func countAll(conn *amqp.Connection) {
	for _, t := range targets {
		depth, err := depthAfterRestart(conn, t.queue)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-28s depth=%d\n", t.queue, depth)
	}
	fmt.Printf("%-28s %s\n", exchange, exchangeState(conn, exchange))
	fmt.Printf("%-28s %s\n", transientExchange, exchangeState(conn, transientExchange))
}

// depthAfterRestart retries while the broker is still recovering the queue.
func depthAfterRestart(conn *amqp.Connection, queue string) (int, error) {
	var lastErr error
	for range 20 {
		depth, err := withChannel(conn, func(ch *amqp.Channel) (int, error) {
			return rmqlab.Depth(ch, queue)
		})
		if err == nil {
			return depth, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return 0, lastErr
}

func exchangeState(conn *amqp.Connection, name string) string {
	_, err := withChannel(conn, func(ch *amqp.Channel) (int, error) {
		return 0, ch.ExchangeDeclarePassive(name, amqp.ExchangeTopic, false, false, false, false, nil)
	})
	if err != nil {
		return "missing"
	}
	return "exists"
}

func withChannel(conn *amqp.Connection, fn func(*amqp.Channel) (int, error)) (int, error) {
	ch, err := conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	return fn(ch)
}
