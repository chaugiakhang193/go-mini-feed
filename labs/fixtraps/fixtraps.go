// Package fixtraps reproduces two common FIX parsing mistakes next to the
// decoder in internal/fix, on the same bytes. The functions here are wrong on
// purpose. Nothing outside this package calls them.
package fixtraps

import (
	"bytes"
	"strconv"

	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

// MapParse stores every field in a map keyed by tag. A map keeps one value
// per key. A tag that appears in several entries of a repeating group keeps
// only its value from the last of those entries.
func MapParse(raw []byte) map[int]string {
	fields := make(map[int]string)
	for _, field := range bytes.Split(raw, []byte{fix.SOH}) {
		tagText, value, found := bytes.Cut(field, []byte("="))
		if !found {
			continue
		}
		tag, err := strconv.Atoi(string(tagText))
		if err != nil {
			continue
		}
		fields[tag] = string(value)
	}
	return fields
}

// FindTag looks a tag up by searching for "<tag>=" anywhere in the message.
// The match is not required to start right after a SOH. The pattern also
// matches the tail of a longer tag, and "83=" is found inside "283=".
func FindTag(raw []byte, tag int) (string, bool) {
	key := []byte(strconv.Itoa(tag) + "=")
	at := bytes.Index(raw, key)
	if at < 0 {
		return "", false
	}
	value, _, _ := bytes.Cut(raw[at+len(key):], []byte{fix.SOH})
	return string(value), true
}
