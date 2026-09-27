package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strconv"
)

// LoadEnv reads the environment layer from the process environment.
// Loading a .env file into the environment is the caller's job (cmd/*).
func LoadEnv() (*Env, error) {
	var errs []error

	required := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", key))
		}
		return v
	}

	env := &Env{
		RabbitMQHost:     required("RABBITMQ_HOST"),
		RabbitMQUser:     required("RABBITMQ_USER"),
		RabbitMQPassword: required("RABBITMQ_PASSWORD"),
		RedisAddr:        required("REDIS_ADDR"),
		RedisPassword:    os.Getenv("REDIS_PASSWORD"),
		ServerIPAddress:  os.Getenv("SERVER_IP_ADDRESS"),
	}

	if raw := required("RABBITMQ_PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			errs = append(errs, fmt.Errorf("RABBITMQ_PORT %q is not a valid port", raw))
		} else {
			env.RabbitMQPort = port
		}
	}

	if ip := env.ServerIPAddress; ip != "" {
		addr, err := netip.ParseAddr(ip)
		if err != nil || !addr.Is4() {
			errs = append(errs, fmt.Errorf("SERVER_IP_ADDRESS %q is not an IPv4 address", ip))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return env, nil
}
