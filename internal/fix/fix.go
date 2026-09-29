// Package fix encodes and decodes FIX tag=value messages with the FIX 4.4
// framing: BeginString, BodyLength and CheckSum.
package fix

import (
	"fmt"
	"strconv"
)

// SOH (0x01) terminates every field, the last one included.
const SOH byte = 0x01

// BeginString is the protocol version Encode writes to tag 8 and Decode
// requires there.
const BeginString = "FIX.4.4"

// Field is one tag=value pair. Value is a string, so a decoded field owns its
// bytes and never aliases a read buffer that is reused for the next packet.
type Field struct {
	Tag   int
	Value string
}

func appendField(dst []byte, tag int, value string) []byte {
	dst = strconv.AppendInt(dst, int64(tag), 10)
	dst = append(dst, '=')
	dst = append(dst, value...)
	return append(dst, SOH)
}

// isFramingTag reports whether tag is one of the four tags whose position the
// framing fixes: 8, 9 and 35 first, 10 last.
func isFramingTag(tag int) bool {
	switch tag {
	case TagBeginString, TagBodyLength, TagMsgType, TagCheckSum:
		return true
	}
	return false
}

// wireLen is the number of bytes f occupies on the wire. It is exact for
// decoded fields because Split accepts a tag number only without sign or
// leading zero, so formatting the tag again gives back the original bytes.
func wireLen(f Field) int {
	return len(strconv.Itoa(f.Tag)) + len("=") + len(f.Value) + 1
}

// checksum is the FIX CheckSum of b: the sum of its bytes modulo 256, written
// as exactly three digits.
func checksum(b []byte) string {
	var sum int
	for _, c := range b {
		sum += int(c)
	}
	return fmt.Sprintf("%03d", sum%256)
}

// parseTag reads a tag number. FIX 4.4 defines TagNum as positive and without
// leading zeros; strconv.Atoi alone also accepts "+35", "-35" and "035".
func parseTag(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	return n, err == nil && n > 0 && strconv.Itoa(n) == s
}

// parseCount reads a byte length or an entry count: ASCII digits only. FIX 4.4
// allows leading zeros in int fields ("0120" is 120), so they are accepted;
// a sign is not, since neither value can be negative.
func parseCount(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}
