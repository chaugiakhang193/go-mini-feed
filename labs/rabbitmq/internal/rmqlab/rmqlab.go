// Package rmqlab has the helpers the RabbitMQ labs share: connecting to the
// broker and reading how many messages a queue holds.
package rmqlab

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

// Vhost is the virtual host every lab runs in.
const Vhost = "/lab"

// Dial loads .env when present, reads the RabbitMQ settings from the
// environment and opens a connection to Vhost.
func Dial() (*amqp.Connection, error) {
	if err := godotenv.Load(); err != nil {
		log.Printf("no .env file, using the process environment: %v", err)
	}
	env, err := config.LoadEnv()
	if err != nil {
		return nil, err
	}
	uri, err := buildURL(env.RabbitMQHost, env.RabbitMQPort, env.RabbitMQUser, env.RabbitMQPassword)
	if err != nil {
		return nil, err
	}
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("dial %s:%d: %w", env.RabbitMQHost, env.RabbitMQPort, err)
	}
	return conn, nil
}

// buildURL returns the AMQP URL for Vhost.
//
// The leading slash belongs to the vhost name and has to be escaped.
// "amqp://host/lab" is vhost "lab"; "amqp://host/%2Flab" is vhost "/lab".
//
// A parse error from net/url quotes the whole URL, password included.
// buildURL parses the URL before amqp.Dial gets it and reports a bad one by
// host and port only.
func buildURL(host string, port int, user, password string) (string, error) {
	userinfo := url.UserPassword(user, password)
	uri := fmt.Sprintf("amqp://%s@%s:%d/%s", userinfo.String(), host, port, url.PathEscape(Vhost))
	if _, err := url.Parse(uri); err != nil {
		return "", fmt.Errorf("RABBITMQ_HOST %q and RABBITMQ_PORT %d do not form a valid URL", host, port)
	}
	return uri, nil
}

// WaitDepth polls the number of ready messages in queue until it is at least
// want or timeout has passed. It returns the last count it read.
//
// An unconfirmed publish may return before the broker has queued the
// message. A count read straight after it can still be the old one.
func WaitDepth(ch *amqp.Channel, queue string, want int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		depth, err := Depth(ch, queue)
		if err != nil {
			return 0, err
		}
		if depth >= want {
			return depth, nil
		}
		if time.Now().After(deadline) {
			return depth, fmt.Errorf("queue %s holds %d messages after %s, want %d", queue, depth, timeout, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Depth returns the number of ready messages in queue. It uses a passive
// declare, which reports on an existing queue and never creates one. The
// durable flag is set because every lab queue is durable.
func Depth(ch *amqp.Channel, queue string) (int, error) {
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect queue %s: %w", queue, err)
	}
	return q.Messages, nil
}
