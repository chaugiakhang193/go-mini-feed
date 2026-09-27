package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadApp reads and validates the structure layer.
func LoadApp(path string) (*App, error) {
	var app App
	if err := decodeFile(path, &app); err != nil {
		return nil, err
	}
	if err := app.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return &app, nil
}

// LoadMulticast reads and validates one market's group file.
// The market type is the file name without its extension.
func LoadMulticast(path string) (*Multicast, error) {
	var mc Multicast
	if err := decodeFile(path, &mc); err != nil {
		return nil, err
	}
	mc.Market = MarketFromPath(path)
	if err := mc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return &mc, nil
}

// MarketFromPath maps "configs/multicast/demo.yaml" to "demo".
func MarketFromPath(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// decodeFile rejects unknown keys, so a typo such as "prefech" fails loudly
// instead of leaving the field at its zero value. It also rejects a second
// YAML document: Decode reads one document per call, so anything after a
// "---" separator would otherwise be ignored without an error.
func decodeFile(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("decode %s: file is empty", path)
		}
		return fmt.Errorf("decode %s: %w", path, err)
	}

	var extra yaml.Node
	switch err := dec.Decode(&extra); {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("decode %s: %w", path, err)
	default:
		return fmt.Errorf("decode %s: only one YAML document is allowed, found another at line %d", path, extra.Line)
	}
}
