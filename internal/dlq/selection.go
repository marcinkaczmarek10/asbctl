// Package dlq selects, snapshots, resends and purges dead-lettered messages.
package dlq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// Filter picks messages by payload type and dead-letter reason. An empty field matches everything.
type Filter struct {
	PayloadTypeID string
	Reason        string
}

// Entry is what the selection keeps of a message: enough to find it again and to count it.
type Entry struct {
	SequenceNumber int64
	MessageID      string
	PayloadTypeID  string
	Reason         string
}

// Matches reports whether the entry passes the filter.
func (f Filter) Matches(entry Entry) bool {
	return (f.PayloadTypeID == "" || entry.PayloadTypeID == f.PayloadTypeID) &&
		(f.Reason == "" || entry.Reason == f.Reason)
}

// EntryFromMessage reads the fields that selection needs from a received or peeked message.
func EntryFromMessage(message *azservicebus.ReceivedMessage) Entry {
	entry := Entry{MessageID: message.MessageID}
	if message.SequenceNumber != nil {
		entry.SequenceNumber = *message.SequenceNumber
	}
	entry.PayloadTypeID, _ = message.ApplicationProperties[export.PayloadTypeIDProperty].(string)
	if message.DeadLetterReason != nil {
		entry.Reason = *message.DeadLetterReason
	}
	return entry
}

// Selection is the set of dead-lettered messages that a command will act on.
type Selection struct {
	Entries []Entry
	// Peeked counts every message looked at, selected or not.
	Peeked int
}

// Group is one row of a plan: how many selected messages share a payload type and a reason.
type Group struct {
	PayloadTypeID    string `json:"payload_type_id"`
	DeadLetterReason string `json:"dead_letter_reason"`
	Count            int    `json:"count"`
}

// MessageIDs returns the selected entries grouped by MessageId. A MessageId can appear more than once on a
// dead-letter queue (the same message dead-lettered twice), so the value is a slice.
func (s Selection) MessageIDs() map[string][]Entry {
	ids := make(map[string][]Entry, len(s.Entries))
	for _, entry := range s.Entries {
		ids[entry.MessageID] = append(ids[entry.MessageID], entry)
	}
	return ids
}

// DuplicateIDs returns the MessageIds that occur more than once in the selection. Empty ids do not count.
func (s Selection) DuplicateIDs() map[string]struct{} {
	seen := make(map[string]int, len(s.Entries))
	for _, entry := range s.Entries {
		if entry.MessageID != "" {
			seen[entry.MessageID]++
		}
	}
	duplicates := make(map[string]struct{})
	for id, count := range seen {
		if count > 1 {
			duplicates[id] = struct{}{}
		}
	}
	return duplicates
}

// Groups counts the selection by payload type and reason, largest group first.
func (s Selection) Groups() []Group {
	counts := make(map[Group]int)
	for _, entry := range s.Entries {
		counts[Group{PayloadTypeID: entry.PayloadTypeID, DeadLetterReason: entry.Reason}]++
	}
	groups := make([]Group, 0, len(counts))
	for group, count := range counts {
		group.Count = count
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(a, b Group) int {
		return cmp.Or(
			cmp.Compare(b.Count, a.Count),
			cmp.Compare(a.PayloadTypeID, b.PayloadTypeID),
			cmp.Compare(a.DeadLetterReason, b.DeadLetterReason),
		)
	})
	return groups
}

var errSelectionFull = errors.New("selection is full")

// Select peeks the dead-letter queue and keeps the messages that pass the filter, up to maxSelected (zero means
// no limit). onSelected sees each kept message in full, which is how the snapshot gets written.
func Select(
	ctx context.Context,
	peeker bus.Peeker,
	filter Filter,
	maxSelected int,
	onSelected func(*azservicebus.ReceivedMessage) error,
) (Selection, error) {
	var selection Selection
	_, err := bus.Peek(ctx, peeker, bus.PeekOptions{Max: math.MaxInt}, func(page []*azservicebus.ReceivedMessage) error {
		for _, message := range page {
			selection.Peeked++
			entry := EntryFromMessage(message)
			if !filter.Matches(entry) {
				continue
			}
			if err := onSelected(message); err != nil {
				return err
			}
			selection.Entries = append(selection.Entries, entry)
			if maxSelected > 0 && len(selection.Entries) >= maxSelected {
				return errSelectionFull
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errSelectionFull) {
		return Selection{}, err
	}
	return selection, nil
}

// SelectWithSnapshot is Select that writes each selected message to a snapshot file first. The file is renamed
// into place only when the whole selection succeeded, so a snapshot that exists is complete. An existing file is an error.
func SelectWithSnapshot(
	ctx context.Context,
	peeker bus.Peeker,
	filter Filter,
	maxSelected int,
	snapshotPath string,
) (Selection, error) {
	var selection Selection
	err := export.WriteFile(snapshotPath, false, func(w io.Writer) error {
		writer := export.NewWriter(w, export.Options{Format: export.FormatJSON})
		var err error
		selection, err = Select(ctx, peeker, filter, maxSelected, func(message *azservicebus.ReceivedMessage) error {
			_, err := writer.WriteMessages([]*azservicebus.ReceivedMessage{message})
			return err
		})
		return err
	})
	if err != nil {
		return Selection{}, fmt.Errorf("snapshot: %w", err)
	}
	return selection, nil
}

// DefaultSnapshotPath names a snapshot after the entity and the time, in the working directory.
func DefaultSnapshotPath(target bus.Target, now time.Time) string {
	entity := target.Queue
	if entity == "" {
		entity = target.Topic + "-" + target.Subscription
	}
	entity = strings.NewReplacer("/", "_", "\\", "_").Replace(entity)
	return fmt.Sprintf("asbctl-snapshot-%s-%s.ndjson", entity, now.UTC().Format("20060102T150405Z"))
}
