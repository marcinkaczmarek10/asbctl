package export

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

func dlqMessage(id, payloadTypeID, reason, description string, deliveryCount uint32, enqueuedAt string) *azservicebus.ReceivedMessage {
	enqueued, err := time.Parse(time.RFC3339Nano, enqueuedAt)
	if err != nil {
		panic(err)
	}
	m := &azservicebus.ReceivedMessage{
		MessageID: id, DeliveryCount: deliveryCount, EnqueuedTime: &enqueued,
		DeadLetterReason: ptr(reason), DeadLetterErrorDescription: ptr(description),
	}
	if payloadTypeID != "" {
		m.ApplicationProperties = map[string]any{"PayloadTypeId": payloadTypeID}
	}
	return m
}

func summarise(entity bus.EntityFacts, maxMessages int, messages ...*azservicebus.ReceivedMessage) Summary {
	aggregator := NewAggregator()
	aggregator.Add(messages)
	return aggregator.Summary(entity, maxMessages)
}

func TestSummaryGroupsByPayloadTypeAndReason(t *testing.T) {
	got := summarise(bus.EntityFacts{}, 100,
		dlqMessage("a1", "OrderPlaced", "MaxDeliveryCountExceeded", "boom", 10, "2026-11-05T10:00:00Z"),
		dlqMessage("a2", "OrderPlaced", "MaxDeliveryCountExceeded", "boom", 7, "2026-11-05T09:00:00Z"),
		dlqMessage("a3", "OrderPlaced", "MaxDeliveryCountExceeded", "other", 3, "2026-11-05T11:00:00Z"),
		dlqMessage("b1", "OrderPlaced", "BadPayload", "x", 1, "2026-11-05T12:00:00Z"),
		dlqMessage("c1", "", "BadPayload", "y", 2, "2026-11-05T13:00:00Z"),
	)

	want := []Group{
		{PayloadTypeID: "OrderPlaced", DeadLetterReason: "MaxDeliveryCountExceeded", Count: 3, FirstEnqueuedAt: "2026-11-05T09:00:00Z", LastEnqueuedAt: "2026-11-05T11:00:00Z", MaxDeliveryCount: 10, TopErrorDescription: "boom", SampleMessageID: "a1"},
		{PayloadTypeID: "", DeadLetterReason: "BadPayload", Count: 1, FirstEnqueuedAt: "2026-11-05T13:00:00Z", LastEnqueuedAt: "2026-11-05T13:00:00Z", MaxDeliveryCount: 2, TopErrorDescription: "y", SampleMessageID: "c1"},
		{PayloadTypeID: "OrderPlaced", DeadLetterReason: "BadPayload", Count: 1, FirstEnqueuedAt: "2026-11-05T12:00:00Z", LastEnqueuedAt: "2026-11-05T12:00:00Z", MaxDeliveryCount: 1, TopErrorDescription: "x", SampleMessageID: "b1"},
	}
	if !reflect.DeepEqual(got.Groups, want) {
		t.Errorf("Groups =\n%+v\nwant\n%+v", got.Groups, want)
	}
	if got.Peeked != 5 {
		t.Errorf("Peeked = %d, want 5", got.Peeked)
	}
}

func TestSummarySortsByCountThenPayloadTypeThenReason(t *testing.T) {
	got := summarise(bus.EntityFacts{}, 100,
		dlqMessage("1", "B", "r1", "", 1, "2026-11-05T10:00:00Z"),
		dlqMessage("2", "A", "r2", "", 1, "2026-11-05T10:00:00Z"),
		dlqMessage("3", "A", "r1", "", 1, "2026-11-05T10:00:00Z"),
		dlqMessage("4", "C", "r1", "", 1, "2026-11-05T10:00:00Z"),
		dlqMessage("5", "C", "r1", "", 1, "2026-11-05T10:00:00Z"),
	)

	var order []string
	for _, group := range got.Groups {
		order = append(order, group.PayloadTypeID+"/"+group.DeadLetterReason)
	}
	want := []string{"C/r1", "A/r1", "A/r2", "B/r1"}
	if !reflect.DeepEqual(order, want) {
		t.Errorf("group order = %v, want %v", order, want)
	}
}

func TestSummaryTopDescription(t *testing.T) {
	tests := []struct {
		name         string
		descriptions []string
		want         string
	}{
		{name: "most frequent wins", descriptions: []string{"a", "b", "b", "c"}, want: "b"},
		{name: "a tie goes to the smaller text", descriptions: []string{"b", "a", "b", "a"}, want: "a"},
		{name: "empty descriptions are ignored", descriptions: []string{"", "", "x"}, want: "x"},
		{name: "only empty descriptions leave none", descriptions: []string{"", ""}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var messages []*azservicebus.ReceivedMessage
			for _, description := range tc.descriptions {
				messages = append(messages, dlqMessage("m", "T", "r", description, 1, "2026-11-05T10:00:00Z"))
			}

			got := summarise(bus.EntityFacts{}, 100, messages...).Groups[0].TopErrorDescription

			if got != tc.want {
				t.Errorf("TopErrorDescription = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSummaryComparesEnqueuedTimesAsTimesAndNotAsText(t *testing.T) {
	got := summarise(bus.EntityFacts{}, 100,
		dlqMessage("1", "T", "r", "", 1, "2026-11-05T10:00:00Z"),
		dlqMessage("2", "T", "r", "", 1, "2026-11-05T10:00:00.5Z"),
	).Groups[0]

	if got.FirstEnqueuedAt != "2026-11-05T10:00:00Z" || got.LastEnqueuedAt != "2026-11-05T10:00:00.5Z" {
		t.Errorf("first/last = %s / %s, want 10:00:00Z / 10:00:00.5Z", got.FirstEnqueuedAt, got.LastEnqueuedAt)
	}
}

func TestSummaryOfAnEmptyDeadLetterQueue(t *testing.T) {
	got := summarise(bus.EntityFacts{}, 100)

	if got.Peeked != 0 || got.Partial || got.Groups == nil || len(got.Groups) != 0 {
		t.Errorf("Summary() = %+v, want peeked 0, not partial and an empty non-nil group list", got)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"groups":[]`) {
		t.Errorf("json = %s, want groups to be an empty array", encoded)
	}
}

func TestIsPartial(t *testing.T) {
	known := func(count int64) bus.EntityFacts { return bus.EntityFacts{Available: true, DeadLetterMessages: &count} }
	tests := []struct {
		name   string
		entity bus.EntityFacts
		peeked int
		max    int
		want   bool
	}{
		{name: "known count above peeked is partial", entity: known(500), peeked: 100, max: 100, want: true},
		{name: "known count equal to peeked is complete", entity: known(100), peeked: 100, max: 100, want: false},
		{name: "known count below peeked is complete", entity: known(90), peeked: 100, max: 100, want: false},
		{name: "known count wins over a peek that filled max", entity: known(100), peeked: 100, max: 100, want: false},
		{name: "unknown count with peeked at max is partial", entity: bus.EntityFacts{}, peeked: 100, max: 100, want: true},
		{name: "unknown count with peeked below max is complete", entity: bus.EntityFacts{}, peeked: 99, max: 100, want: false},
		{name: "unavailable facts are unknown even with a stale count", entity: bus.EntityFacts{Available: false, DeadLetterMessages: &[]int64{500}[0]}, peeked: 10, max: 100, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPartial(tc.entity, tc.peeked, tc.max); got != tc.want {
				t.Errorf("isPartial(peeked %d, max %d) = %v, want %v", tc.peeked, tc.max, got, tc.want)
			}
		})
	}
}

func TestWriteSummaryTable(t *testing.T) {
	count := int64(250)
	entity := bus.EntityFacts{
		Queue: "orders", Available: true, ActiveMessages: ptr(int64(1)), DeadLetterMessages: &count,
		ScheduledMessages: ptr(int64(0)), TransferDeadLetterMessages: ptr(int64(0)),
		MaxDeliveryCount: ptr(int32(10)), LockDuration: "PT1M", RequiresDuplicateDetection: ptr(true), DuplicateDetectionWindow: "PT10M",
	}
	long := strings.Repeat("d", 130)
	summary := summarise(entity, 2,
		dlqMessage("a", "", "", long, 4, "2026-11-05T10:00:00Z"),
		dlqMessage("b", "OrderPlaced", "Poison", "bad\tjson", 1, "2026-11-05T11:00:00Z"),
	)
	var out bytes.Buffer

	if err := WriteSummary(&out, FormatTable, summary); err != nil {
		t.Fatalf("WriteSummary() error = %v", err)
	}

	want := "entity:                queue orders\n" +
		"active:                1\n" +
		"dead-letter:           250\n" +
		"scheduled:             0\n" +
		"transfer dead-letter:  0\n" +
		"max delivery count:    10\n" +
		"lock duration:         PT1M\n" +
		"duplicate detection:   on, window PT10M\n" +
		"Partial: the summary covers 2 of 250 messages.\n" +
		"\n" +
		"COUNT  PAYLOAD_TYPE  REASON  MAX_DELIVERY  FIRST                 LAST                  DESCRIPTION\n" +
		"1      (none)        (none)  4             2026-11-05T10:00:00Z  2026-11-05T10:00:00Z  " + strings.Repeat("d", 120) + "\n" +
		"1      OrderPlaced   Poison  1             2026-11-05T11:00:00Z  2026-11-05T11:00:00Z  bad json\n"
	if out.String() != want {
		t.Errorf("table =\n%s\nwant\n%s", out.String(), want)
	}
}

func TestWriteSummaryTableWhenEntityFactsAreUnavailable(t *testing.T) {
	var out bytes.Buffer

	err := WriteSummary(&out, FormatTable, summarise(bus.EntityFacts{Topic: "t", Subscription: "s"}, 1,
		dlqMessage("a", "T", "r", "d", 1, "2026-11-05T10:00:00Z")))

	if err != nil {
		t.Fatalf("WriteSummary() error = %v", err)
	}
	for _, want := range []string{"topic t, subscription s", "unavailable", "Partial: the summary covers the first 1 messages"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("table = %q, want it to contain %q", out.String(), want)
		}
	}
}

func TestWriteSummaryTableOfAnEmptyDeadLetterQueue(t *testing.T) {
	var out bytes.Buffer

	_ = WriteSummary(&out, FormatTable, summarise(bus.EntityFacts{Queue: "q"}, 10))

	if !strings.Contains(out.String(), "No dead-lettered messages were peeked.") || strings.Contains(out.String(), "COUNT") {
		t.Errorf("table = %q, want the empty note and no group table", out.String())
	}
}

func TestWriteSummaryJSONIsOneObjectWithFullDescriptions(t *testing.T) {
	long := strings.Repeat("d", 130)
	summary := summarise(bus.EntityFacts{Queue: "q"}, 10, dlqMessage("a", "", "r", long, 2, "2026-11-05T10:00:00Z"))
	var out bytes.Buffer

	if err := WriteSummary(&out, FormatJSON, summary); err != nil {
		t.Fatalf("WriteSummary() error = %v", err)
	}

	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("json = %q, want exactly one line", out.String())
	}
	var decoded Summary
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("output is not json: %v", err)
	}
	if decoded.Schema != 1 || decoded.Peeked != 1 || decoded.Entity.Queue != "q" || decoded.Entity.Available {
		t.Errorf("decoded = %+v, want schema 1, peeked 1, entity queue q and unavailable", decoded)
	}
	if got := decoded.Groups[0]; got.TopErrorDescription != long || got.PayloadTypeID != "" {
		t.Errorf("group = %+v, want the full description and an empty payload type id", got)
	}
	if !strings.Contains(out.String(), `"payload_type_id":""`) {
		t.Errorf("json = %s, want payload_type_id present even when empty", out.String())
	}
}
