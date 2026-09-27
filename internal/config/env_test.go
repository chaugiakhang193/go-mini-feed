package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RABBITMQ_HOST", "localhost")
	t.Setenv("RABBITMQ_PORT", "5672")
	t.Setenv("RABBITMQ_USER", "lab")
	t.Setenv("RABBITMQ_PASSWORD", "lab")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("SERVER_IP_ADDRESS", "")
}

func TestLoadEnv(t *testing.T) {
	tests := []struct {
		name    string
		set     map[string]string
		wantErr []string
	}{
		{name: "valid, default interface"},
		{name: "valid, explicit NIC", set: map[string]string{"SERVER_IP_ADDRESS": "192.0.2.10"}},
		{
			name:    "missing credentials are all reported",
			set:     map[string]string{"RABBITMQ_USER": "", "RABBITMQ_PASSWORD": ""},
			wantErr: []string{"RABBITMQ_USER is required", "RABBITMQ_PASSWORD is required"},
		},
		{
			name:    "port is not a number",
			set:     map[string]string{"RABBITMQ_PORT": "56x2"},
			wantErr: []string{`RABBITMQ_PORT "56x2" is not a valid port`},
		},
		{
			name:    "NIC address must be IPv4",
			set:     map[string]string{"SERVER_IP_ADDRESS": "wifi0"},
			wantErr: []string{`SERVER_IP_ADDRESS "wifi0" is not an IPv4 address`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setValidEnv(t)
			for k, v := range tt.set {
				t.Setenv(k, v)
			}

			env, err := LoadEnv()
			if tt.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, 5672, env.RabbitMQPort)
				return
			}
			require.Error(t, err)
			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}
