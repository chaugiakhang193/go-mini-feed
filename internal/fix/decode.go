package fix

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	// ErrMalformed marks input that is not a sequence of tag=value fields, or
	// whose framing fields are missing from their fixed positions or repeated
	// elsewhere.
	ErrMalformed = errors.New("malformed FIX message")
	// ErrBodyLength marks a tag 9 that disagrees with the bytes received.
	ErrBodyLength = errors.New("FIX body length mismatch")
	// ErrChecksum marks a tag 10 that disagrees with the bytes received.
	ErrChecksum = errors.New("FIX checksum mismatch")
)

// Message is a decoded message: every field in wire order, framing included.
type Message struct {
	fields []Field
}

// Split tokenizes raw into fields in wire order, keeping repeated tags. It
// checks only the tag=value<SOH> syntax; Decode adds the framing checks.
func Split(raw []byte) ([]Field, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrMalformed)
	}
	if raw[len(raw)-1] != SOH {
		return nil, fmt.Errorf("%w: last field is not terminated by SOH", ErrMalformed)
	}

	var fields []Field
	for pos := 0; pos < len(raw); {
		// Always found: the last byte is a SOH.
		end := pos + bytes.IndexByte(raw[pos:], SOH)
		f, err := parseField(raw[pos:end])
		if err != nil {
			return nil, fmt.Errorf("%w: field at byte %d: %w", ErrMalformed, pos, err)
		}
		fields = append(fields, f)
		pos = end + 1
	}
	return fields, nil
}

func parseField(b []byte) (Field, error) {
	tagText, value, found := bytes.Cut(b, []byte("="))
	if !found {
		return Field{}, fmt.Errorf("missing '=' in %q", b)
	}
	tag, ok := parseTag(string(tagText))
	if !ok {
		return Field{}, fmt.Errorf("invalid tag %q", tagText)
	}
	if len(value) == 0 {
		return Field{}, fmt.Errorf("tag %d has an empty value", tag)
	}
	return Field{Tag: tag, Value: string(value)}, nil
}

// Decode splits raw and checks its framing: tags 8, 9 and 35 as the first
// three fields, tag 10 as the last and nowhere else, and BodyLength and
// CheckSum matching the bytes actually received.
func Decode(raw []byte) (Message, error) {
	fields, err := Split(raw)
	if err != nil {
		return Message{}, err
	}
	if len(fields) < 4 {
		return Message{}, fmt.Errorf("%w: %d fields, need at least tags 8, 9, 35 and 10", ErrMalformed, len(fields))
	}
	for i, want := range []int{TagBeginString, TagBodyLength, TagMsgType} {
		if fields[i].Tag != want {
			return Message{}, fmt.Errorf("%w: field %d is tag %d, want %d", ErrMalformed, i+1, fields[i].Tag, want)
		}
	}
	last := fields[len(fields)-1]
	if last.Tag != TagCheckSum {
		return Message{}, fmt.Errorf("%w: last field is tag %d, want %d", ErrMalformed, last.Tag, TagCheckSum)
	}
	// A framing tag repeated inside the body would pass the length and
	// checksum checks yet shadow the real one: Get(TagCheckSum) would return
	// a value that was never compared with the bytes.
	for i := 3; i < len(fields)-1; i++ {
		if isFramingTag(fields[i].Tag) {
			return Message{}, fmt.Errorf("%w: framing tag %d repeated at field %d", ErrMalformed, fields[i].Tag, i+1)
		}
	}
	if fields[0].Value != BeginString {
		return Message{}, fmt.Errorf("%w: BeginString %q, want %q", ErrMalformed, fields[0].Value, BeginString)
	}

	// BodyLength counts from the byte after the SOH that ends tag 9 through
	// the SOH before "10="; CheckSum covers every byte before "10=".
	bodyStart := wireLen(fields[0]) + wireLen(fields[1])
	trailerStart := len(raw) - wireLen(last)
	declared, ok := parseCount(fields[1].Value)
	if !ok {
		return Message{}, fmt.Errorf("%w: BodyLength %q is not a valid byte count", ErrMalformed, fields[1].Value)
	}
	if counted := trailerStart - bodyStart; declared != counted {
		return Message{}, fmt.Errorf("%w: declared %d, counted %d", ErrBodyLength, declared, counted)
	}
	if computed := checksum(raw[:trailerStart]); last.Value != computed {
		return Message{}, fmt.Errorf("%w: declared %s, computed %s", ErrChecksum, last.Value, computed)
	}
	return Message{fields: fields}, nil
}

// MsgType returns tag 35.
func (m Message) MsgType() string {
	v, _ := m.Get(TagMsgType)
	return v
}

// Get returns the value of the first field with the given tag. A tag inside a
// repeating group appears once per entry, and Get sees only the first of them.
func (m Message) Get(tag int) (string, bool) {
	return lookup(m.fields, tag)
}

func lookup(fields []Field, tag int) (string, bool) {
	for _, f := range fields {
		if f.Tag == tag {
			return f.Value, true
		}
	}
	return "", false
}
