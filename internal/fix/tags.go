package fix

// Tag numbers from the public FIX dictionary. Every tag exists in FIX 4.4
// unless its comment says otherwise.
const (
	TagBeginString  = 8
	TagBodyLength   = 9
	TagCheckSum     = 10
	TagMsgSeqNum    = 34
	TagMsgType      = 35
	TagSenderCompID = 49
	TagSendingTime  = 52
	TagTargetCompID = 56

	TagSecurityIDSource  = 22
	TagSecurityID        = 48
	TagSymbol            = 55
	TagTransactTime      = 60
	TagPrevClosePx       = 140
	TagNoMDEntries       = 268
	TagMDEntryType       = 269
	TagMDEntryPx         = 270
	TagMDEntrySize       = 271
	TagMDUpdateAction    = 279
	TagLocationID        = 283
	TagMDEntryPositionNo = 290
	TagTradingSessionID  = 336
	TagGrossTradeAmt     = 381
	TagTotalVolumeTraded = 387

	// Not in FIX 4.4 or FIX 5.0; defined from FIX 5.0 SP1 onwards.
	TagLowLimitPrice   = 1148
	TagHighLimitPrice  = 1149
	TagMarketSegmentID = 1300
)
