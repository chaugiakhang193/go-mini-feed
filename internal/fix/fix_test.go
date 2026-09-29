package fix

import "strings"

// wire turns a readable message, "|" standing for SOH, into wire bytes.
func wire(s string) []byte {
	return []byte(strings.ReplaceAll(s, "|", string(SOH)))
}

// readable is the inverse of wire, so failures print legibly.
func readable(b []byte) string {
	return strings.ReplaceAll(string(b), string(SOH), "|")
}

// goldenSecurityDefinition is a security definition (35=d) for the synthetic
// symbol AAA, the encoding of securityDefinitionBody. Tags 9 and 10 were
// computed outside Go, so comparing against it does not check the package
// against itself.
const goldenSecurityDefinition = "8=FIX.4.4|9=120|35=d|49=DEMO|56=LAB|34=1|52=20260928-01:30:00.000|" +
	"55=AAA|48=XX000000AAA9|22=4|1300=MAIN|140=25000|1149=26700|1148=23300|10=208|"

// goldenHeartbeat is a heartbeat (35=0) whose CheckSum is 000, the case
// where the three-digit padding matters most. Also computed outside Go.
const goldenHeartbeat = "8=FIX.4.4|9=51|35=0|49=DEMO|56=LAB|34=59|52=20260928-02:00:59.000|10=000|"

func heartbeatBody() []Field {
	return []Field{
		{TagSenderCompID, "DEMO"},
		{TagTargetCompID, "LAB"},
		{TagMsgSeqNum, "59"},
		{TagSendingTime, "20260928-02:00:59.000"},
	}
}

func securityDefinitionBody() []Field {
	return []Field{
		{TagSenderCompID, "DEMO"},
		{TagTargetCompID, "LAB"},
		{TagMsgSeqNum, "1"},
		{TagSendingTime, "20260928-01:30:00.000"},
		{TagSymbol, "AAA"},
		{TagSecurityID, "XX000000AAA9"},
		{TagSecurityIDSource, "4"},
		{TagMarketSegmentID, "MAIN"},
		{TagPrevClosePx, "25000"},
		{TagHighLimitPrice, "26700"},
		{TagLowLimitPrice, "23300"},
	}
}

// orderBookBody is an incremental refresh (35=X) carrying three bid levels
// and three ask levels, so tags 279, 269, 290, 270 and 271 repeat six times.
func orderBookBody() []Field {
	return []Field{
		{TagSenderCompID, "DEMO"},
		{TagTargetCompID, "LAB"},
		{TagMsgSeqNum, "2"},
		{TagSendingTime, "20260928-02:15:00.123"},
		{TagSecurityID, "XX000000AAA9"},
		{TagMarketSegmentID, "MAIN"},
		{TagTradingSessionID, "CONT"},
		{TagTransactTime, "20260928-02:15:00.120"},
		{TagNoMDEntries, "6"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPositionNo, "1"}, {TagMDEntryPx, "24900"}, {TagMDEntrySize, "1000"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPositionNo, "2"}, {TagMDEntryPx, "24800"}, {TagMDEntrySize, "2500"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPositionNo, "3"}, {TagMDEntryPx, "24700"}, {TagMDEntrySize, "500"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "1"}, {TagMDEntryPositionNo, "1"}, {TagMDEntryPx, "25000"}, {TagMDEntrySize, "1200"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "1"}, {TagMDEntryPositionNo, "2"}, {TagMDEntryPx, "25100"}, {TagMDEntrySize, "800"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "1"}, {TagMDEntryPositionNo, "3"}, {TagMDEntryPx, "25200"}, {TagMDEntrySize, "3000"},
	}
}
