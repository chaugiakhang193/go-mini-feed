// Package sim builds the packets of a synthetic market and decides when to
// send them. It holds no sockets: bytes leave through a Sender, so every rule
// here runs in a plain unit test.
package sim

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
)

// priceStep is the tick size. Prices are whole currency units held in int64,
// so every comparison and sum is exact.
const priceStep = 100

// securityIDPrefix starts every security ID. With a three-letter symbol and
// the check digit it makes 12 characters, the width of an ISIN.
const securityIDPrefix = "XX000000"

// Instrument is one tradable symbol and its daily price band.
type Instrument struct {
	Symbol     string
	SecurityID string
	RefPrice   int64
	High       int64 // upper price limit of the day
	Low        int64 // lower price limit of the day
}

// Instruments returns n instruments named AAA, BBB, and so on. Reference
// prices come from rng, so one seed gives one list.
func Instruments(n int, rng *rand.Rand) ([]Instrument, error) {
	if n < 1 || n > 26 {
		return nil, fmt.Errorf("instrument count %d out of range 1..26", n)
	}
	list := make([]Instrument, n)
	for i := range list {
		symbol := strings.Repeat(string(rune('A'+i)), 3)
		ref := (rng.Int64N(900) + 100) * priceStep
		high, low := priceLimits(ref)
		list[i] = Instrument{
			Symbol:     symbol,
			SecurityID: securityID(symbol),
			RefPrice:   ref,
			High:       high,
			Low:        low,
		}
	}
	return list, nil
}

// priceLimits returns the band of +7% and -7% around ref. Both limits stay on
// the tick grid: the upper one rounds down and the lower one rounds up, so the
// band never extends past 7%. The arithmetic is integer, so the limits do not
// depend on float rounding.
func priceLimits(ref int64) (high, low int64) {
	high = ref * 107 / 100 / priceStep * priceStep
	low = (ref*93/100 + priceStep - 1) / priceStep * priceStep
	return high, low
}

// securityID appends a Luhn check digit to the prefix and the symbol, giving
// the layout of an ISIN.
func securityID(symbol string) string {
	payload := securityIDPrefix + symbol
	return payload + strconv.Itoa(luhnCheckDigit(payload))
}

// luhnCheckDigit computes the Luhn check digit of an alphanumeric payload.
// Digits stand for themselves and a letter for its two-digit value, A=10
// through Z=35, which is how ISO 6166 prepares an ISIN for the algorithm.
func luhnCheckDigit(payload string) int {
	var digits []int
	for _, c := range payload {
		switch {
		case c >= '0' && c <= '9':
			digits = append(digits, int(c-'0'))
		case c >= 'A' && c <= 'Z':
			v := int(c-'A') + 10
			digits = append(digits, v/10, v%10)
		}
	}

	// The digit next to the check digit is doubled, then every second one.
	sum := 0
	double := true
	for i := len(digits) - 1; i >= 0; i-- {
		d := digits[i]
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return (10 - sum%10) % 10
}
