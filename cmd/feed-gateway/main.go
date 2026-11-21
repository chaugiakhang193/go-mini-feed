// Command feed-gateway receives the multicast groups of one market. It joins
// every group on the interface that owns SERVER_IP_ADDRESS, keeps the raw
// packets of the groups marked raw_log, and counts what each group delivers.
// With -print-config it prints the resolved configuration and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"net"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
	"github.com/chaugiakhang193/go-mini-feed/internal/logger"
	"github.com/chaugiakhang193/go-mini-feed/internal/multicast"
)

// countInterval is how often the packet counts are logged while packets keep
// arriving. Logging per packet would flood the log at feed rates.
const countInterval = 10 * time.Second

func main() {
	configPath := flag.String("config", "configs/config.yaml", "application config (structure layer)")
	multicastPath := flag.String("multicast", "configs/multicast/demo.yaml", "multicast group file; its name is the market type")
	envFile := flag.String("env-file", ".env", "optional dotenv file loaded into the environment")
	printOnly := flag.Bool("print-config", false, "print the resolved configuration and exit")
	flag.Parse()

	// Variables already set in the environment win over the file.
	if err := godotenv.Load(*envFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Fatalf("load %s: %v", *envFile, err)
	}

	app, err := config.LoadApp(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	mc, err := config.LoadMulticast(*multicastPath)
	if err != nil {
		log.Fatal(err)
	}
	env, err := config.LoadEnv()
	if err != nil {
		log.Fatalf("environment:\n%v", err)
	}

	printConfig(os.Stdout, app, mc, env)
	if *printOnly {
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ifi, err := multicast.FindInterface(env.ServerIPAddress)
	if err != nil {
		log.Fatalf("op=nic_select_failed server_ip=%s err=%v", env.ServerIPAddress, err)
	}
	nic := "default"
	if ifi != nil {
		nic = ifi.Name
	}
	log.Printf("op=nic_selected nic=%s", nic)

	raw := logger.NewRaw(app.Gateway.RawLog.Dir, mc.Market, app.Gateway.RawLog.QueueSize)
	ls, conns, err := join(mc, ifi, app.Gateway, raw)
	if err != nil {
		raw.Close()
		log.Fatalf("op=join_failed err=%v", err)
	}
	defer closeAll(conns)

	if _, err := run(ctx, ls, app.Gateway.QueueSize, raw); err != nil {
		closeAll(conns)
		log.Fatalf("op=gateway_failed err=%v", err)
	}
}

// join opens one socket per group and logs the receive buffer each one got.
// On an error it closes the sockets already open.
func join(mc *config.Multicast, ifi *net.Interface, gw config.Gateway, raw *logger.Raw) ([]*multicast.Listener, []*net.UDPConn, error) {
	var ls []*multicast.Listener
	var conns []*net.UDPConn
	for _, g := range mc.Groups {
		conn, buf, err := multicast.Join(g, ifi, gw.ReadBufferBytes)
		if err != nil {
			closeAll(conns)
			return nil, nil, err
		}
		switch {
		case buf.Short():
			// The kernel caps the request at net.core.rmem_max without an
			// error. A burst larger than the granted size is dropped by the
			// kernel before the gateway can read it.
			log.Printf("WARN op=read_buffer group=%s requested=%d granted=%d hint=raise net.core.rmem_max",
				g.Name, buf.Requested, buf.Granted)
		case buf.Measured:
			log.Printf("op=read_buffer group=%s requested=%d granted=%d", g.Name, buf.Requested, buf.Granted)
		default:
			log.Printf("op=read_buffer group=%s requested=%d granted=unknown", g.Name, buf.Requested)
		}
		conns = append(conns, conn)
		ls = append(ls, multicast.NewListener(g.Name, conn, gw.ReadDeadline, recorderFor(g, raw)))
	}
	return ls, conns, nil
}

// recorderFor returns raw for a group whose raw_log is on, and a nil
// interface otherwise. Returning a nil *logger.Raw would give a non-nil
// interface, and the listener would call Record on it.
func recorderFor(g config.Group, raw *logger.Raw) multicast.Recorder {
	if !g.RawLogEnabled() {
		return nil
	}
	return raw
}

func closeAll(conns []*net.UDPConn) {
	for _, c := range conns {
		c.Close()
	}
}

// run reads every listener until ctx ends or one listener fails, then stops
// the others, closes raw and returns the packet count of each group. The
// channel is closed only after every listener has returned, since a send on a
// closed channel panics.
func run(ctx context.Context, ls []*multicast.Listener, queueSize int, raw *logger.Raw) (map[string]uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan multicast.Packet, queueSize)
	errs := make(chan error, len(ls))
	var wg sync.WaitGroup
	for _, l := range ls {
		wg.Go(func() {
			if err := l.Run(ctx, out); err != nil {
				errs <- err
				cancel()
			}
		})
	}
	counted := make(chan map[string]uint64, 1)
	go func() { counted <- count(out) }()

	<-ctx.Done()
	stopping := time.Now()
	wg.Wait()
	close(out)
	counts := <-counted
	closeErr := raw.Close()
	log.Printf("op=gateway_stopped %s raw_dropped=%d stop_ms=%d",
		formatCounts(counts), raw.Dropped(), time.Since(stopping).Milliseconds())

	close(errs)
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	if closeErr != nil {
		all = append(all, fmt.Errorf("raw log: %w", closeErr))
	}
	return counts, errors.Join(all...)
}

// count stands in for the publisher: it drains out until the channel is
// closed and logs the counts every countInterval while they grow.
func count(out <-chan multicast.Packet) map[string]uint64 {
	counts := make(map[string]uint64)
	var total, reported uint64
	ticker := time.NewTicker(countInterval)
	defer ticker.Stop()
	for {
		select {
		case p, ok := <-out:
			if !ok {
				return counts
			}
			counts[p.Group]++
			total++
		case <-ticker.C:
			if total != reported {
				log.Printf("op=packets total=%d %s", total, formatCounts(counts))
				reported = total
			}
		}
	}
}

// formatCounts writes the counts as group=n pairs sorted by group name.
func formatCounts(counts map[string]uint64) string {
	var b strings.Builder
	for i, group := range slices.Sorted(maps.Keys(counts)) {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", group, counts[group])
	}
	return b.String()
}

func printConfig(w io.Writer, app *config.App, mc *config.Multicast, env *config.Env) {
	nic := env.ServerIPAddress
	if nic == "" {
		nic = "(OS default interface)"
	}

	fmt.Fprintf(w, "market      %s\n", mc.Market)
	fmt.Fprintf(w, "rabbitmq    %s@%s:%d vhost=%s password=%s\n",
		env.RabbitMQUser, env.RabbitMQHost, env.RabbitMQPort, app.RabbitMQ.VHost, mask(env.RabbitMQPassword))
	if app.Multiplexer.Enabled {
		for _, r := range app.Multiplexer.Replicas {
			fmt.Fprintf(w, "replica     vhost=%s pool=%d\n", r.VHost, app.Multiplexer.PoolSize)
		}
	}
	fmt.Fprintf(w, "nic         %s\n", nic)
	fmt.Fprintf(w, "gateway     queue=%d read_buffer=%d deadline=%s raw_log=%s\n",
		app.Gateway.QueueSize, app.Gateway.ReadBufferBytes, app.Gateway.ReadDeadline, app.Gateway.RawLog.Dir)
	fmt.Fprintf(w, "groups      %d\n", len(mc.Groups))
	for _, g := range mc.Groups {
		fmt.Fprintf(w, "  %-20s %s:%d raw_log=%-5t -> %s [%s]\n",
			g.Name, g.Address, g.Port, g.RawLogEnabled(), g.Exchange, g.RoutingKey)
	}
}

func mask(secret string) string {
	if secret == "" {
		return "(empty)"
	}
	return "******"
}
