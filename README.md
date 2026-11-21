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
| FIX 4.4 codec: encoding, decoding, BodyLength and CheckSum checks, repeating groups | done |
| RabbitMQ labs: unroutable messages, publisher confirms, durability across a restart, retry limits, per-key order in a worker pool | done |
| `feed-gateway` | receives every group, keeps the raw feed on disk and counts packets; publishing planned |
| `feed-sim` (exchange simulator) | done |
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
- **Repeating groups are rebuilt from ordered fields.** In a FIX repeating group every entry starts with the same tag,
  and the group has no end marker. The decoder keeps fields in wire order, starts an entry at the group's first tag and
  ends the group at the first tag that is not a member. Parsing into a map keeps one value per tag, and searching for
  `83=` as a substring also matches `283=`. `labs/fixtraps` reproduces both next to the correct decoder. See
  [anatomy of a FIX message](https://www.onixs.biz/insights/anatomy-of-a-fix-message).
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

The simulator needs no containers. It sends each packet kind to the multicast group of the same name in
`configs/multicast/demo.yaml`:

```bash
go run ./cmd/feed-sim -scenario one-tick    # a definition, an order book and a trade
make sim                                    # a stream of prices, snapshots and heartbeats until Ctrl+C
go run ./cmd/feed-sim -scenario stream -count 200 -drop 0.1 -dup 0.05 -reorder 0.05
```

`-drop`, `-dup` and `-reorder` make the network lose, repeat and swap packets after the sequence number (tag 34) is
assigned, so a receiver sees gaps, repeats and out-of-order numbers. `-transport unicast` sends to `127.0.0.1` on the
same ports instead of the groups. At the end it logs one `op=sim_done` line per packet kind with the counts.

The gateway needs no containers yet. Start it first, then the simulator in a second terminal; Ctrl+C stops it:

```bash
make gateway                                          # go run ./cmd/feed-gateway
go run ./cmd/feed-sim -scenario stream -rate 50 -count 100
```

It joins every group on the interface that owns `SERVER_IP_ADDRESS` (empty: the kernel picks one from its routing
table). An address on no interface, or on an interface that is down or has no `MULTICAST` flag, such as a Linux
loopback, stops the start-up: joining a group there succeeds and then receives nothing. Groups with `raw_log: true`
are written to `logs/<UTC date>/<market>/<group>.raw`, one line per packet: the receive time (RFC 3339, UTC), the
payload length and the payload bytes unchanged, SOH and line breaks included. A full raw log queue drops packets and
logs `op=raw_log_dropped` instead of slowing the socket reader. The gateway logs `op=packets` every 10 seconds while
packets arrive and `op=gateway_stopped` with the count per group on exit, as `packets.<group>=n`. Groups may share a port: each socket
receives only the groups joined on it (on Linux, `IP_MULTICAST_ALL` is turned off).

Each socket asks for `read_buffer_bytes` of receive buffer. Linux caps the request at `net.core.rmem_max` without an
error, so the gateway reads the granted size back and logs it:

```
WARN op=read_buffer group=price requested=8388608 granted=4194304 hint=raise net.core.rmem_max
```

The gateway keeps running with the smaller buffer; a burst larger than the granted size is dropped by the kernel.

## Labs

Each program under `labs/rabbitmq/` tests one broker behaviour and prints what it measured. They need the local
stack (`make up`) and use only `lab_*` exchanges and queues on vhost `/lab`. Every run that builds an experiment
deletes those objects and declares them again first. The results below are from RabbitMQ 4.3.6.

```bash
go run ./labs/rabbitmq/unroutable
go run ./labs/rabbitmq/confirms -n 1000
go run ./labs/rabbitmq/retrylimit -scenario xdeath
go run ./labs/rabbitmq/retrylimit -scenario limit

# keyorder seeds the input queue, runs the pool, then audits the results per key
go run ./labs/rabbitmq/keyorder -phase seed
go run ./labs/rabbitmq/keyorder -phase run -mode lanes -lanes 4
go run ./labs/rabbitmq/keyorder -phase audit

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
| `keyorder` | 2000 messages over 4 keys. With 4 lanes (one worker each, the lane chosen by FNV-1a of the key) every key keeps its order. 60 lanes take the same time, since only 4 of them get work. One lane holding 1000 deliveries overflows, the requeued deliveries come back behind newer ones, and 950 to 964 results arrive out of order (the count varies by run). One goroutine per message is several times faster and puts most results out of order. |

Background reading: RabbitMQ's guides on [publishers](https://www.rabbitmq.com/docs/publishers),
[confirms](https://www.rabbitmq.com/docs/confirms), [dead lettering](https://www.rabbitmq.com/docs/dlx) and
[quorum queues](https://www.rabbitmq.com/docs/quorum-queues).

`labs/fixtraps` needs no stack. It is a test package that runs two common FIX parsing mistakes and the decoder in
`internal/fix` on the same bytes:

```bash
go test -v ./labs/fixtraps
```

An order book with six entries parsed into a map keeps one price, the last ask, and every level looks like an ask.
Searching for `83=` in a message without tag 83 returns the value of LocationID (`283`).

`labs/mcastdump` needs no stack either. It joins the four groups of `configs/multicast/demo.yaml` and prints every
datagram on one line with the group, the length, tag 34 and the message, SOH shown as `|`. Start it first, then the
simulator in a second terminal:

```bash
go run ./labs/mcastdump -count 3
go run ./cmd/feed-sim -scenario one-tick
```

Each group has its own reader goroutine. The readers copy their datagrams into one channel with a buffer of 1024 and
a single goroutine prints from it, so lines of different groups never interleave. `-count N` exits after N packets and
`-transport unicast` listens on `127.0.0.1` instead of joining the groups (use the same flag on `feed-sim`).

## Layout

```
cmd/feed-gateway/          gateway: joins the groups, raw log, packet counts, shutdown on Ctrl+C
cmd/feed-sim/              exchange simulator: flags, UDP sockets, shutdown on Ctrl+C
internal/config/           YAML + environment loading and validation
internal/fix/              FIX tag=value encoding, decoding and repeating groups
internal/logger/           raw feed files per day, market and group
internal/multicast/        interface choice, group join, receive buffer check, read loop
internal/sim/              synthetic market: instruments, packets, order books, trades, scenarios, network faults
labs/rabbitmq/             RabbitMQ experiments, one program each
labs/fixtraps/             deliberate FIX parsing mistakes next to the correct decoder
labs/mcastdump/            prints the datagrams of the multicast groups as they arrive
configs/                   config.yaml and one multicast file per market
deployments/               docker-compose and RabbitMQ definitions
Makefile                   make help lists every target
```
