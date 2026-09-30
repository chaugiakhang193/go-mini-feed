package rmqlab

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildURL(t *testing.T) {
	uri, err := buildURL("localhost", 5673, "lab", "p@ss/word")
	require.NoError(t, err)
	assert.Equal(t, "amqp://lab:p%40ss%2Fword@localhost:5673/%2Flab", uri)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	password, _ := parsed.User.Password()
	assert.Equal(t, "p@ss/word", password)
}

func TestBuildURLErrorHidesPassword(t *testing.T) {
	const password = "synthetic-secret"

	_, err := buildURL("bad%host", 5673, "lab", password)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), password)
	assert.Contains(t, err.Error(), `"bad%host"`)
}
