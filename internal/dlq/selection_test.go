package dlq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

func TestFilterMatches(t *testing.T) {
	entry := Entry{PayloadTypeID: "Order", Reason: "Poison"}
	tests := []struct {
		name   string
		filter Filter
		want   bool
	}{
		{name: "empty filter matches", filter: Filter{}, want: true},
		{name: "payload type match", filter: Filter{PayloadTypeID: "Order"}, want: true},
		{name: "payload type mismatch", filter: Filter{PayloadTypeID: "Other"}, want: false},
		{name: "reason match", filter: Filter{Reason: "Poison"}, want: true},
		{name: "reason mismatch", filter: Filter{Reason: "Other"}, want: false},
		{name: "both match", filter: Filter{PayloadTypeID: "Order", Reason: "Poison"}, want: true},
		{name: "one of two mismatches", filter: Filter{PayloadTypeID: "Order", Reason: "Other"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Matches(entry); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
	if (Filter{PayloadTypeID: "Order"}).Matches(Entry{}) {
		t.Error("a payload type filter matched a message with no PayloadTypeId")
	}
}

func TestEntryFromMessage(t *testing.T) {
	got := EntryFromMessage(dlMessage(7, "m-7", "Order", "Poison"))

	want := Entry{SequenceNumber: 7, MessageID: "m-7", PayloadTypeID: "Order", Reason: "Poison"}
	if got != want {
		t.Errorf("EntryFromMessage() = %+v, want %+v", got, want)
	}
	if bare := EntryFromMessage(&azservicebus.ReceivedMessage{}); bare != (Entry{}) {
		t.Errorf("EntryFromMessage(empty) = %+v, want the zero entry", bare)
	}
}

func TestSelectKeepsMatchesUpToMax(t *testing.T) {
	queue := newFakeQueue(
		dlMessage(1, "a", "Order", "r"), dlMessage(2, "b", "Other", "r"), dlMessage(3, "c", "Order", "r"),
		dlMessage(4, "d", "Order", "r"), dlMessage(5, "e", "Order", "r"),
	)
	tests := []struct {
		name       string
		max        int
		wantIDs    []string
		wantPeeked int
	}{
		{name: "no limit", max: 0, wantIDs: []string{"a", "c", "d", "e"}, wantPeeked: 5},
		{name: "limit stops the peek early", max: 2, wantIDs: []string{"a", "c"}, wantPeeked: 3},
		{name: "limit of one", max: 1, wantIDs: []string{"a"}, wantPeeked: 1},
		{name: "limit above the matches", max: 10, wantIDs: []string{"a", "c", "d", "e"}, wantPeeked: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selection, err := Select(context.Background(), queue, Filter{PayloadTypeID: "Order"}, tc.max, func(*azservicebus.ReceivedMessage) error { return nil })

			if err != nil {
				t.Fatalf("Select() error = %v", err)
			}
			var ids []string
			for _, entry := range selection.Entries {
				ids = append(ids, entry.MessageID)
			}
			if !reflect.DeepEqual(ids, tc.wantIDs) || selection.Peeked != tc.wantPeeked {
				t.Errorf("Select() = ids %v, peeked %d, want %v, %d", ids, selection.Peeked, tc.wantIDs, tc.wantPeeked)
			}
		})
	}
}

func TestSelectReturnsTheCallbackError(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))

	_, err := Select(context.Background(), queue, Filter{}, 0, func(*azservicebus.ReceivedMessage) error { return errBoom })

	if !errors.Is(err, errBoom) {
		t.Errorf("Select() error = %v, want %v", err, errBoom)
	}
}

func TestSelectionSets(t *testing.T) {
	selection := Selection{Entries: []Entry{
		{SequenceNumber: 1, MessageID: "a"}, {SequenceNumber: 2, MessageID: "a"}, {SequenceNumber: 3, MessageID: "b"},
		{SequenceNumber: 4, MessageID: ""}, {SequenceNumber: 5, MessageID: ""},
	}}

	ids := selection.MessageIDs()
	duplicates := selection.DuplicateIDs()

	if len(ids["a"]) != 2 || len(ids["b"]) != 1 || len(ids[""]) != 2 {
		t.Errorf("MessageIDs() = %v, want a:2, b:1, empty:2", ids)
	}
	if !reflect.DeepEqual(duplicates, map[string]struct{}{"a": {}}) {
		t.Errorf("DuplicateIDs() = %v, want only a: b occurs once and empty ids do not count", duplicates)
	}
}

func TestSelectionGroupsAreSortedByCountThenKey(t *testing.T) {
	selection := Selection{Entries: []Entry{
		{PayloadTypeID: "B", Reason: "r"}, {PayloadTypeID: "A", Reason: "r2"}, {PayloadTypeID: "A", Reason: "r1"},
		{PayloadTypeID: "C", Reason: "r"}, {PayloadTypeID: "C", Reason: "r"}, {PayloadTypeID: "", Reason: ""},
	}}

	got := selection.Groups()

	want := []Group{
		{"C", "r", 2}, {"", "", 1}, {"A", "r1", 1}, {"A", "r2", 1}, {"B", "r", 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Groups() = %v, want %v", got, want)
	}
}

func TestDefaultSnapshotPath(t *testing.T) {
	now := time.Date(2026, 11, 5, 14, 30, 5, 0, time.FixedZone("CET", 3600))
	tests := []struct {
		name   string
		target bus.Target
		want   string
	}{
		{name: "queue", target: bus.Target{Queue: "orders"}, want: "asbctl-snapshot-orders-20261105T133005Z.ndjson"},
		{name: "subscription", target: bus.Target{Topic: "events", Subscription: "billing"}, want: "asbctl-snapshot-events-billing-20261105T133005Z.ndjson"},
		{name: "slashes in a name", target: bus.Target{Queue: "team/orders"}, want: "asbctl-snapshot-team_orders-20261105T133005Z.ndjson"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DefaultSnapshotPath(tc.target, now); got != tc.want {
				t.Errorf("DefaultSnapshotPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSelectWithSnapshotWritesTheSelectedMessagesBeforeReturning(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "Order", "r"), dlMessage(2, "b", "Other", "r"), dlMessage(3, "c", "Order", "r"))
	path := filepath.Join(t.TempDir(), "snap.ndjson")

	selection, err := SelectWithSnapshot(context.Background(), queue, Filter{PayloadTypeID: "Order"}, 0, path)

	if err != nil || len(selection.Entries) != 2 {
		t.Fatalf("SelectWithSnapshot() = %d entries, %v, want 2 and no error", len(selection.Entries), err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 2 {
		t.Errorf("snapshot has %d lines, want 2", lines)
	}
	for _, id := range []string{`"message_id":"a"`, `"message_id":"c"`} {
		if !strings.Contains(string(data), id) {
			t.Errorf("snapshot = %s, want it to contain %s", data, id)
		}
	}
	if strings.Contains(string(data), `"message_id":"b"`) {
		t.Errorf("snapshot holds a message that the filter rejected: %s", data)
	}

	var parsed []string
	for outgoing, err := range export.Outgoings(strings.NewReader(string(data)), nil) {
		if err != nil {
			t.Fatalf("the snapshot is not readable by send: %v", err)
		}
		parsed = append(parsed, outgoing.MessageID)
	}
	if !reflect.DeepEqual(parsed, []string{"a", "c"}) {
		t.Errorf("snapshot parsed as %v, want [a c]", parsed)
	}
}

func TestSelectWithSnapshotRefusesAnExistingFileAndLeavesNoFileOnAFailure(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "exists.ndjson")
	if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))

	_, err := SelectWithSnapshot(context.Background(), queue, Filter{}, 0, existing)
	if !errors.Is(err, export.ErrFileExists) {
		t.Errorf("SelectWithSnapshot() over an existing file error = %v, want ErrFileExists", err)
	}

	failing := filepath.Join(dir, "failing.ndjson")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SelectWithSnapshot(cancelled, queue, Filter{}, 0, failing); err == nil {
		t.Error("SelectWithSnapshot() with a cancelled context error = nil, want an error")
	}
	if _, statErr := os.Stat(failing); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("a failed selection left %s behind: %v", failing, statErr)
	}
}
