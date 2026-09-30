// Command unroutable publishes to a topic exchange that has no queue bound.
// The publish call returns nil and the broker drops the message. A queue
// bound afterwards does not receive it. Exchanges route messages, they do
// not store them.
//
// Run from the repository root with the local stack up (make up):
//
//	go run ./labs/rabbitmq/unroutable
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/chaugiakhang193/go-mini-feed/labs/rabbitmq/internal/rmqlab"
)

const (
	exchange   = "lab_probe"
	queue      = "lab_probe_q"
	routingKey = "all"

	firstBody  = "message 1: sent before any queue is bound"
	secondBody = "message 2: sent after the bind"
)

func main() {
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

	// Remove what a previous run left on the broker.
	if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
		log.Fatalf("delete queue: %v", err)
	}
	if err := ch.ExchangeDelete(exchange, false, false); err != nil {
		log.Fatalf("delete exchange: %v", err)
	}

	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeTopic, false, false, false, false, nil); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}
	fmt.Printf("declared topic exchange %q, no queue bound\n", exchange)

	err = publish(ch, firstBody)
	fmt.Printf("publish message 1 -> err = %v\n", err)

	// The queue is durable. RabbitMQ 4.3.0 and later refuse a non-durable,
	// non-exclusive queue by default (deprecated feature
	// transient_nonexcl_queues).
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		log.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		log.Fatalf("bind queue: %v", err)
	}
	fmt.Printf("declared queue %q and bound it with key %q\n", queue, routingKey)

	err = publish(ch, secondBody)
	fmt.Printf("publish message 2 -> err = %v\n", err)

	depth, err := rmqlab.WaitDepth(ch, queue, 1, 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("messages in queue: %d\n", depth)

	// If message 1 had been stored it would be at the head of the queue.
	msg, ok, err := ch.Get(queue, true)
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	if !ok || string(msg.Body) != secondBody {
		log.Fatalf("first message in queue is %q, want %q", msg.Body, secondBody)
	}
	fmt.Printf("first message in queue: %q\n", msg.Body)
}

// publish sends body with mandatory=false. The broker does not tell the
// publisher when no binding matches.
func publish(ch *amqp.Channel, body string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}
