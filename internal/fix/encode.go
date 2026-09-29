package fix

import (
	"fmt"
	"strconv"
	"strings"
)

// Encode builds a complete message: tags 8, 9 and 35 first, then body in the
// given order, then tag 10. Encode computes BodyLength and CheckSum itself, so
// body must not contain any of the four framing tags. The caller lays out
// repeating groups: the count tag first, then each entry starting with its
// delimiter tag.
func Encode(msgType string, body []Field) ([]byte, error) {
	if err := checkValue(TagMsgType, msgType); err != nil {
		return nil, err
	}

	// payload is exactly what BodyLength counts: from "35=" through the SOH
	// that precedes "10=".
	payload := appendField(nil, TagMsgType, msgType)
	for i, f := range body {
		if err := checkBodyField(f); err != nil {
			return nil, fmt.Errorf("body[%d]: %w", i, err)
		}
		payload = appendField(payload, f.Tag, f.Value)
	}

	msg := appendField(nil, TagBeginString, BeginString)
	msg = appendField(msg, TagBodyLength, strconv.Itoa(len(payload)))
	msg = append(msg, payload...)
	return appendField(msg, TagCheckSum, checksum(msg)), nil
}

func checkBodyField(f Field) error {
	if isFramingTag(f.Tag) {
		return fmt.Errorf("tag %d is written by Encode", f.Tag)
	}
	if f.Tag <= 0 {
		return fmt.Errorf("tag must be positive, got %d", f.Tag)
	}
	return checkValue(f.Tag, f.Value)
}

// checkValue rejects values the receiver could not split back into the same
// field. Outside length-prefixed data fields, which this package does not
// support, FIX has no escaping: an embedded SOH ends the field early.
func checkValue(tag int, value string) error {
	if value == "" {
		return fmt.Errorf("tag %d has an empty value", tag)
	}
	if strings.IndexByte(value, SOH) >= 0 {
		return fmt.Errorf("tag %d value contains SOH", tag)
	}
	return nil
}
