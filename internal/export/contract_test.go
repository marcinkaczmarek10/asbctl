package export

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// The files in testdata/contract are the output shapes that the Python adapter parses. This test pins them
// to what the writers produce. Run it with UPDATE_CONTRACT=1 to rewrite them after an intended change.
const contractDir = "../../testdata/contract"

func contractMessages() []*azservicebus.ReceivedMessage {
	enqueued := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	withText := &azservicebus.ReceivedMessage{
		SequenceNumber: ptr(int64(1001)),
		MessageID:      "msg-1",
		EnqueuedTime:   &enqueued,
		ContentType:    ptr("application/json"),
		CorrelationID:  ptr("corr-1"),
		Subject:        ptr("order.placed"),
		PartitionKey:   ptr("pk-1"),
		To:             ptr("orders"),
		ReplyTo:        ptr("replies"),
		TimeToLive:     ptr(time.Hour),
		DeliveryCount:  2,
		ApplicationProperties: map[string]any{
			"PayloadTypeId": "OrderPlaced",
			"retries":       int64(3),
			"urgent":        true,
			"ratio":         1.5,
			"created":       time.Date(2026, 11, 5, 9, 59, 0, 500_000_000, time.UTC),
			"signature":     []byte{0xde, 0xad},
			"trace":         amqp.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},
		Body: []byte(`{"id":42}`),
	}
	deadLettered := &azservicebus.ReceivedMessage{
		SequenceNumber:             ptr(int64(1002)),
		MessageID:                  "msg-2",
		EnqueuedTime:               &enqueued,
		DeliveryCount:              10,
		DeadLetterReason:           ptr("MaxDeliveryCountExceeded"),
		DeadLetterErrorDescription: ptr("Message could not be consumed after 10 delivery attempts."),
		DeadLetterSource:           ptr("orders"),
		Body:                       []byte{0xff, 0xfe, 0x00},
	}
	valueBody := &azservicebus.ReceivedMessage{
		SequenceNumber: ptr(int64(1003)),
		MessageID:      "msg-3",
		EnqueuedTime:   &enqueued,
		RawAMQPMessage: &azservicebus.AMQPAnnotatedMessage{Body: azservicebus.AMQPAnnotatedMessageBody{
			Value: map[any]any{"id": int64(7), "tags": []any{"a", "b"}},
		}},
	}
	emptyBody := &azservicebus.ReceivedMessage{SequenceNumber: ptr(int64(1004)), MessageID: "msg-4", EnqueuedTime: &enqueued}

	return []*azservicebus.ReceivedMessage{withText, deadLettered, valueBody, emptyBody}
}

func contractSummary(entity bus.EntityFacts, maxMessages int) Summary {
	aggregator := NewAggregator()
	aggregator.Add([]*azservicebus.ReceivedMessage{
		dlqMessage("a1", "OrderPlaced", "MaxDeliveryCountExceeded", "consumer timed out", 10, "2026-11-05T10:00:00Z"),
		dlqMessage("a2", "OrderPlaced", "MaxDeliveryCountExceeded", "consumer timed out", 10, "2026-11-05T10:05:00.5Z"),
		dlqMessage("a3", "OrderPlaced", "MaxDeliveryCountExceeded", "database unavailable", 7, "2026-11-05T10:10:00Z"),
		dlqMessage("b1", "", "BadPayload", "unexpected token", 1, "2026-11-05T11:00:00Z"),
	})
	return aggregator.Summary(entity, maxMessages)
}

func TestContractFilesMatchTheWriters(t *testing.T) {
	var messages bytes.Buffer
	if _, err := NewWriter(&messages, Options{Format: FormatJSON}).WriteMessages(contractMessages()); err != nil {
		t.Fatalf("WriteMessages() error = %v", err)
	}

	var summary, unavailable, failure bytes.Buffer
	available := bus.EntityFacts{
		Queue: "orders", Available: true,
		ActiveMessages: ptr(int64(12)), DeadLetterMessages: ptr(int64(40)),
		ScheduledMessages: ptr(int64(0)), TransferDeadLetterMessages: ptr(int64(0)),
		MaxDeliveryCount: ptr(int32(10)), LockDuration: "PT1M",
		RequiresDuplicateDetection: ptr(true), DuplicateDetectionWindow: "PT10M",
	}
	if err := WriteSummary(&summary, FormatJSON, contractSummary(available, 10000)); err != nil {
		t.Fatalf("WriteSummary() error = %v", err)
	}
	if err := WriteSummary(&unavailable, FormatJSON, contractSummary(bus.EntityFacts{Topic: "events", Subscription: "billing"}, 4)); err != nil {
		t.Fatalf("WriteSummary() error = %v", err)
	}
	WriteError(&failure, FormatJSON, "peek_failed", errors.New("service unavailable"))

	var plan, results, exported bytes.Buffer
	if err := WriteSendPlan(&plan, FormatJSON, NewSendPlan(SendTarget{Queue: "orders"}, map[string]int{"OrderPlaced": 2, "": 1}, false)); err != nil {
		t.Fatalf("WriteSendPlan() error = %v", err)
	}
	for _, result := range []bus.SendResult{
		{Line: 1, MessageID: "msg-1"},
		{Line: 2, MessageID: "msg-2", Err: errors.New("the message is too large")},
		{Line: 3},
	} {
		if err := WriteSendResult(&results, FormatJSON, result); err != nil {
			t.Fatalf("WriteSendResult() error = %v", err)
		}
	}
	if err := WriteExportResult(&exported, FormatJSON, ExportResult{Schema: Schema, Exported: 4, File: "orders.ndjson", Partial: true}); err != nil {
		t.Fatalf("WriteExportResult() error = %v", err)
	}

	files := map[string]string{
		"messages.ndjson":          messages.String(),
		"summary.json":             summary.String(),
		"summary_unavailable.json": unavailable.String(),
		"error.json":               failure.String(),
		"send_plan.json":           plan.String(),
		"send_result.ndjson":       results.String(),
		"export_result.json":       exported.String(),
	}
	for name, got := range files {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(contractDir, name)
			if os.Getenv("UPDATE_CONTRACT") != "" {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if got != string(want) {
				t.Errorf("%s differs from the writer output.\ngot:\n%s\nwant:\n%s", name, got, want)
			}
		})
	}
}
