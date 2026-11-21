package fix

import (
	"errors"
	"fmt"
	"slices"
)

// ErrGroup marks a repeating group whose layout contradicts its count field
// or its spec.
var ErrGroup = errors.New("invalid FIX repeating group")

// GroupSpec describes one repeating group. FIX puts no end marker after a
// group. An entry starts at DelimiterTag, and the group ends at the first tag
// that is not in Members. Without the member list the end of the group cannot
// be found.
type GroupSpec struct {
	CountTag     int
	DelimiterTag int
	Members      []int // includes DelimiterTag
}

// IncrementalEntries is NoMDEntries in MarketDataIncrementalRefresh (35=X).
// In FIX 4.4 each entry starts with MDUpdateAction (279). Members lists the
// subset of the FIX 4.4 group that this module reads.
var IncrementalEntries = GroupSpec{
	CountTag:     TagNoMDEntries,
	DelimiterTag: TagMDUpdateAction,
	Members: []int{
		TagMDUpdateAction, TagMDEntryType, TagMDEntryPx, TagMDEntrySize,
		TagLocationID, TagMDEntryPositionNo,
	},
}

// SnapshotEntries is NoMDEntries in MarketDataSnapshotFullRefresh (35=W).
// In FIX 4.4 each entry starts with MDEntryType (269).
var SnapshotEntries = GroupSpec{
	CountTag:     TagNoMDEntries,
	DelimiterTag: TagMDEntryType,
	Members: []int{
		TagMDEntryType, TagMDEntryPx, TagMDEntrySize, TagMDEntryPositionNo,
	},
}

// Entry is one entry of a repeating group, fields in wire order.
type Entry []Field

// Get returns the value of tag within this entry.
func (e Entry) Get(tag int) (string, bool) {
	return lookup(e, tag)
}

// Entries returns the entries of the group described by spec, rebuilt from
// the ordered fields. A message without spec.CountTag has no such group and
// yields nil, nil.
func (m Message) Entries(spec GroupSpec) ([]Entry, error) {
	countAt := slices.IndexFunc(m.fields, func(f Field) bool { return f.Tag == spec.CountTag })
	if countAt < 0 {
		return nil, nil
	}
	declared, ok := parseCount(m.fields[countAt].Value)
	if !ok {
		return nil, fmt.Errorf("%w: tag %d count %q is not a number", ErrGroup, spec.CountTag, m.fields[countAt].Value)
	}

	var entries []Entry
	for _, f := range m.fields[countAt+1:] {
		if f.Tag == spec.DelimiterTag {
			entries = append(entries, Entry{f})
			continue
		}
		if !slices.Contains(spec.Members, f.Tag) {
			break
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("%w: tag %d must be followed by tag %d, got %d",
				ErrGroup, spec.CountTag, spec.DelimiterTag, f.Tag)
		}
		current := &entries[len(entries)-1]
		// A member seen twice in one entry means the next entry's delimiter
		// is missing. Merging the two entries would pair the wrong fields.
		if _, seen := current.Get(f.Tag); seen {
			return nil, fmt.Errorf("%w: entry %d repeats tag %d, delimiter %d missing",
				ErrGroup, len(entries), f.Tag, spec.DelimiterTag)
		}
		*current = append(*current, f)
	}

	if len(entries) != declared {
		return nil, fmt.Errorf("%w: tag %d declares %d entries, found %d",
			ErrGroup, spec.CountTag, declared, len(entries))
	}
	return entries, nil
}
