package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

func TestPrintConfigNeverPrintsThePassword(t *testing.T) {
	app, err := config.LoadApp("../../configs/config.yaml")
	require.NoError(t, err)
	mc, err := config.LoadMulticast("../../configs/multicast/demo.yaml")
	require.NoError(t, err)
	env := &config.Env{
		RabbitMQHost:     "localhost",
		RabbitMQPort:     5672,
		RabbitMQUser:     "lab",
		RabbitMQPassword: "s3cr3t-value",
		RedisAddr:        "localhost:6379",
	}

	var out bytes.Buffer
	printConfig(&out, app, mc, env)

	assert.NotContains(t, out.String(), "s3cr3t-value")
	assert.Contains(t, out.String(), "lab@localhost:5672 vhost=/lab password=******")
	assert.Contains(t, out.String(), "security_definition  239.255.10.1:46001 raw_log=true")
}

func TestMask(t *testing.T) {
	assert.Equal(t, "(empty)", mask(""))
	assert.Equal(t, "******", mask("x"))
	assert.Equal(t, "******", mask("a much longer secret"))
}
