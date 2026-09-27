// Package config loads the two configuration layers of the feed binaries:
// YAML files describe the structure (groups, exchanges, sizes) and the
// environment supplies deployment values (hosts, credentials, NIC address).
package config

import "time"

// App is the structure layer shared by the gateway and the processor.
type App struct {
	RabbitMQ    RabbitMQ    `yaml:"rabbitmq"`
	Multiplexer Multiplexer `yaml:"multiplexer"`
	Redis       Redis       `yaml:"redis"`
	Gateway     Gateway     `yaml:"gateway"`
	Processor   Processor   `yaml:"processor"`
}

type RabbitMQ struct {
	VHost      string    `yaml:"vhost"`
	Exchanges  Exchanges `yaml:"exchanges"`
	InputQueue string    `yaml:"input_queue"`
	Prefetch   int       `yaml:"prefetch"`
}

type Exchanges struct {
	Raw      string `yaml:"raw"`
	Matched  string `yaml:"matched"`
	BidOffer string `yaml:"bid_offer"`
	Index    string `yaml:"index"`
}

type Multiplexer struct {
	Enabled  bool      `yaml:"enabled"`
	PoolSize int       `yaml:"pool_size"`
	Replicas []Replica `yaml:"replicas"`
}

type Replica struct {
	VHost string `yaml:"vhost"`
}

type Redis struct {
	SecDefDB    int `yaml:"secdef_db"`
	StockInfoDB int `yaml:"stock_info_db"`
	PubSubDB    int `yaml:"pubsub_db"`
}

type Gateway struct {
	QueueSize       int           `yaml:"queue_size"`
	ReadBufferBytes int           `yaml:"read_buffer_bytes"`
	ReadDeadline    time.Duration `yaml:"read_deadline"`
	RawLog          RawLog        `yaml:"raw_log"`
}

type RawLog struct {
	Dir       string `yaml:"dir"`
	QueueSize int    `yaml:"queue_size"`
}

type Processor struct {
	Partitions      int `yaml:"partitions"`
	PartitionBuffer int `yaml:"partition_buffer"`
}

// Multicast is one market's group list. Market is not read from YAML:
// it comes from the file name (configs/multicast/demo.yaml -> "demo").
type Multicast struct {
	Market string  `yaml:"-"`
	Groups []Group `yaml:"groups"`
}

type Group struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
	// RawLog is a pointer so that a missing key (nil) can be told apart from
	// an explicit false; Validate requires it to be set.
	RawLog     *bool  `yaml:"raw_log"`
	Exchange   string `yaml:"exchange"`
	RoutingKey string `yaml:"routing_key"`
}

// RawLogEnabled reports whether raw packets of this group are written to disk.
func (g Group) RawLogEnabled() bool {
	return g.RawLog != nil && *g.RawLog
}

// Env is the environment layer. Password fields must never be printed as is.
type Env struct {
	RabbitMQHost     string
	RabbitMQPort     int
	RabbitMQUser     string
	RabbitMQPassword string
	RedisAddr        string
	RedisPassword    string
	ServerIPAddress  string
}
