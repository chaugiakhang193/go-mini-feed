# go-mini-feed

A learning project: a small market-data feed pipeline written in Go. The planned pipeline receives FIX 4.4 messages
over UDP multicast, forwards the raw bytes through RabbitMQ, parses them into JSON and hands the results to
downstream consumers; the status table below shows what exists today. Everything runs locally, and every
instrument, identifier and price is synthetic.

The goal is to practise the concurrency and messaging patterns such a pipeline needs — one writer per ordered
stream, partitioning by key, backpressure, acknowledging only after the work is done, graceful shutdown — and to
measure what happens when they are done wrong.

## Status

| Component | Status |
|---|---|
| Local stack: RabbitMQ (vhosts `/lab`, `/lab-replica`) and Redis | done |
| Configuration: YAML structure + environment values, validated on load | done |
| FIX 4.4 codec: encoding, decoding, BodyLength and CheckSum checks | done |
| RabbitMQ labs: unroutable messages, publisher confirms, durability across a restart, retry limits | done |
| `feed-gateway` | loads and prints its configuration; networking planned |
| `feed-sim` (exchange simulator) | planned |
| `feed-processor` | planned |
| `feed-consumer` | planned |

## Target architecture

Components marked *planned* in the status table do not exist yet; the diagram is the design they are built against.
Sizes come from `configs/config.yaml`.

```
Exchange simulator (feed-sim)
│ UDP multicast, one group per kind of data; no acknowledgements, so packets can be lost, duplicated or reordered
│   239.255.10.1:46001  security definitions   (FIX 35=d)
│   239.255.10.2:46002  market data            (FIX 35=X)
│   239.255.10.3:46003  snapshots              (FIX 35=W)
│   239.255.10.4:46004  heartbeats             (FIX 35=0)
│        ▼  all groups joined on one network interface (SERVER_IP_ADDRESS)
│
┌─ INGEST GATEWAY — process feed-gateway   (one process per market: --multicast=configs/multicast/<market>.yaml)
│
│ start-up: connect to RabbitMQ (optionally wrapped with a replica broker)
│           → create the shared channel → start one publisher goroutine
│           → start one listener goroutine per multicast group
│
│  listener security definitions ─┐
│  listener market data ──────────┤  each listener, in a loop:
│  listener snapshots ────────────┤   read UDP with a 500 ms deadline (wakes up to notice shutdown)
│  listener heartbeats ───────────┘   → copy the bytes out of the reused read buffer
│        │                             → raw log, only if enabled for the group; sent without waiting
│        │                               (a full log queue drops the line)
│        │                             → label with the group's exchange and routing key from the config
│        │                             → never parse the FIX payload
│        ▼ blocking send: when the channel is full the listener waits, stops reading, and the kernel drops packets
│  shared channel  (bounded, 10,000 messages)
│        ▼
│  publisher goroutine  (exactly one: publishes in the order messages were enqueued)
│        │ declares each exchange the first time it sees it
│        ▼ publish through the broker interface
│  gateway → replicating broker ─┬─ RabbitMQ /lab            primary, synchronous, retried with exponential backoff
│                                └─ RabbitMQ /lab-replica    replica, asynchronous worker pool, circuit breaker
└──────────────────────────────────────────────────────────────────────────────────────────
│
RabbitMQ  (vhost /lab)
│ exchange demo_all_raw  (topic)   ◄── body = raw FIX bytes, unchanged
│        │ binding "#"
│        ▼
│ queue LAB_demo_all_messages      (declared by the processor at start-up; messages wait here while it is down)
│        │ pushed to the consumer, at most 1,000 unacknowledged at a time (prefetch)
│        ▼
┌─ PROCESSOR — process feed-processor
│
│ start-up: declare the input queue and output exchanges → connect Redis → register handlers
│           → start 16 partitions, one worker goroutine each → start consuming
│
│  consumer goroutine  (exactly one, manual acknowledgements)
│        │ every delivery carries its own ack / nack, which the worker calls later
│        ▼
│  dispatch: scan tag 48 (SecurityID) in the raw bytes, without a full parse
│        │ partition = FNV-1a(SecurityID) % 16     → one instrument always lands in the same partition
│        │ partition full → nack with requeue      → the consumer never blocks, but order within an instrument can break
│        ▼
│  partition 0   queue[1000] ──► worker 0   ┐
│  partition 1   queue[1000] ──► worker 1   │  one worker per partition, fixed at start-up
│  ...                                      │  parallel across instruments, sequential within one instrument
│  partition 15  queue[1000] ──► worker 15  ┘
│        │ each worker, one message at a time:
│        ▼
│  parse FIX → route by message type (tag 35) → handler      (handlers return a result; they never publish)
│        ├─ d  → cache the security definition: memory + asynchronous Redis write → nothing published
│        ├─ X  order book → resolve the symbol from the cache → JSON quote
│        │                  → exchange demo_bid_offer, routing key per board
│        │     trade      → resolve the symbol, infer the aggressor side → JSON trade
│        │                  → exchange demo_matched, routing key per board
│        └─ W  → skipped
│        ▼
│  the processor publishes the result
│        ▼
│  handled, output published if any → ack      failed → nack with requeue      (at-least-once delivery)
└──────────────────────────────────────────────────────────────────────────────────────────
│
RabbitMQ  (vhost /lab)
│ exchange demo_matched    ◄── JSON trades
│ exchange demo_bid_offer  ◄── JSON quotes
│        ▼
Downstream consumers declare their own queues and bind the routing keys they need
```

## Design notes

- **One publisher per gateway.** All listeners share one bounded channel read by a single goroutine, so messages
  are published in the order they were enqueued. That is not a promise about arrival order across groups:
  listeners race each other to enqueue. The channel is bounded on purpose: when it is full, listeners block instead of
  growing memory without limit, and packets the kernel cannot buffer are lost — a trade-off the project measures.
- **Partition by instrument.** Messages about one instrument must be handled in order (a security definition before
  its prices), while different instruments can run in parallel. Hashing the security ID to a fixed partition gives
  both, the same way keyed partitions work in Kafka. Adding workers to one partition would break the ordering.
- **Acknowledge after the work is done.** The processor acks a message only after handling it succeeds and, when the
  message produces an output, after that output is published. A crash before the ack causes redelivery instead of
  silent loss (at-least-once). Durability end to end also needs publisher confirms and persistent messages. See
  RabbitMQ's
  [consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms).
- **Synthetic, locally scoped data.** Multicast groups live in `239.255.0.0/16`, the IPv4 local scope defined in
  [RFC 2365](https://www.rfc-editor.org/rfc/rfc2365). Messages use [FIX 4.4](https://www.fixtrading.org/standards/fix-4-4/)
  framing and public tags only: FIX 4.4 tags plus three defined from FIX 5.0 SP1 onwards (MarketSegmentID 1300,
  HighLimitPrice 1149, LowLimitPrice 1148).

## Configuration

| Source | Holds | Where |
|---|---|---|
| YAML | structure that is the same on every machine: exchanges, queues, sizes, partitions, Redis DB numbers | `configs/config.yaml` |
| YAML, one file per market | multicast groups; **the file name is the market type** | `configs/multicast/<market>.yaml` |
| Environment | values that differ per machine: hosts, ports, credentials, the NIC address for multicast | `.env` (copy of `.env.example`) |
| Flags | values that differ per run | `--config`, `--multicast`, `--env-file` |

Unknown YAML keys, a second YAML document, non-multicast addresses and duplicate groups are rejected at start-up.
Loading stops at the first file that fails to decode; validating a decoded file reports all of its problems at once. Adding a market means adding `configs/multicast/<market>.yaml` and running
another gateway with `--multicast=configs/multicast/<market>.yaml`; no Go code changes.

## Quick start

Requirements: Linux or WSL2, Go 1.26, Docker with Compose, `make`, and a C compiler for the race detector.

```bash
cp .env.example .env
make up        # RabbitMQ + Redis, waits until healthy
make smoke     # vhosts, the dev user and Redis answer
make config    # load and print the gateway configuration
make race      # unit tests with the race detector
make down
```

RabbitMQ listens on `localhost:5673` with its management UI at `http://localhost:15673`, and Redis on
`localhost:6381`. The `lab` / `lab` user exists for local development only, and every port binds to `127.0.0.1`.

## Labs

Each program under `labs/rabbitmq/` tests one broker behaviour and prints what it measured. They need the local
stack (`make up`) and use only `lab_*` exchanges and queues on vhost `/lab`. Every run that builds an experiment
deletes those objects and declares them again first. The results below are from RabbitMQ 4.3.6.

```bash
go run ./labs/rabbitmq/unroutable
go run ./labs/rabbitmq/confirms -n 1000
go run ./labs/rabbitmq/retrylimit -scenario xdeath
go run ./labs/rabbitmq/retrylimit -scenario limit

# durability runs in two phases with a broker restart in between
go run ./labs/rabbitmq/durability -phase publish
docker compose -f deployments/docker-compose.yml restart rabbitmq
go run ./labs/rabbitmq/durability -phase count
```

| Lab | Result |
|---|---|
| `unroutable` | Publishing to an exchange with no queue bound returns `nil` and the broker drops the message. A queue bound afterwards holds only the message published after the bind. |
| `confirms` | Waiting for each confirm is far slower than collecting the confirms at the end. An unroutable message is still acked; with `mandatory` the broker also returns it with code 312. A full queue set to `reject-publish` answers with a nack. |
| `durability` | In a durable classic queue the transient messages are gone after the restart and the persistent ones remain. A quorum queue keeps both. |
| `retrylimit` | A consumer can stop retrying by counting rejections in the `x-death` header. A quorum queue with `x-delivery-limit` dead-letters a message returned with `basic.reject`, but keeps redelivering one returned with `basic.nack`. |

Background reading: RabbitMQ's guides on [publishers](https://www.rabbitmq.com/docs/publishers),
[confirms](https://www.rabbitmq.com/docs/confirms), [dead lettering](https://www.rabbitmq.com/docs/dlx) and
[quorum queues](https://www.rabbitmq.com/docs/quorum-queues).

## Layout

```
cmd/feed-gateway/          gateway entry point
internal/config/           YAML + environment loading and validation
internal/fix/              FIX tag=value encoding and decoding
labs/rabbitmq/             RabbitMQ experiments, one program each
configs/                   config.yaml and one multicast file per market
deployments/               docker-compose and RabbitMQ definitions
Makefile                   make help lists every target
```
