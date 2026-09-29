package fix

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeMatchesGolden(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		body    []Field
		want    string
	}{
		{"security definition", "d", securityDefinitionBody(), goldenSecurityDefinition},
		{"heartbeat with CheckSum 000", "0", heartbeatBody(), goldenHeartbeat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.msgType, tc.body)
			require.NoError(t, err)
			assert.Equal(t, tc.want, readable(got))
		})
	}
}

func TestEncodeRejects(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		body    []Field
		wantErr string
	}{
		{
			name:    "empty MsgType",
			msgType: "",
			wantErr: "tag 35 has an empty value",
		},
		{
			name:    "SOH inside MsgType",
			msgType: "d\x01",
			wantErr: "tag 35 value contains SOH",
		},
		{
			name:    "BeginString supplied by the caller",
			msgType: "d",
			body:    []Field{{TagBeginString, "FIX.4.2"}},
			wantErr: "body[0]: tag 8 is written by Encode",
		},
		{
			name:    "BodyLength supplied by the caller",
			msgType: "d",
			body:    []Field{{TagSymbol, "AAA"}, {TagBodyLength, "12"}},
			wantErr: "body[1]: tag 9 is written by Encode",
		},
		{
			name:    "CheckSum supplied by the caller",
			msgType: "d",
			body:    []Field{{TagCheckSum, "000"}},
			wantErr: "body[0]: tag 10 is written by Encode",
		},
		{
			name:    "MsgType repeated in the body",
			msgType: "d",
			body:    []Field{{TagMsgType, "X"}},
			wantErr: "body[0]: tag 35 is written by Encode",
		},
		{
			name:    "tag zero",
			msgType: "d",
			body:    []Field{{0, "AAA"}},
			wantErr: "body[0]: tag must be positive, got 0",
		},
		{
			name:    "negative tag",
			msgType: "d",
			body:    []Field{{-55, "AAA"}},
			wantErr: "body[0]: tag must be positive, got -55",
		},
		{
			name:    "empty value",
			msgType: "d",
			body:    []Field{{TagSymbol, "AAA"}, {TagSecurityID, ""}},
			wantErr: "body[1]: tag 48 has an empty value",
		},
		{
			name:    "SOH inside a value",
			msgType: "d",
			body:    []Field{{TagSymbol, "AA\x01A"}},
			wantErr: "body[0]: tag 55 value contains SOH",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.msgType, tc.body)
			assert.Nil(t, got)
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}
