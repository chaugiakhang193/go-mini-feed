// Command feed-gateway is the entry point of the multicast gateway. It loads
// the YAML and environment configuration and prints the resolved values.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"

	"github.com/joho/godotenv"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "application config (structure layer)")
	multicastPath := flag.String("multicast", "configs/multicast/demo.yaml", "multicast group file; its name is the market type")
	envFile := flag.String("env-file", ".env", "optional dotenv file loaded into the environment")
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
