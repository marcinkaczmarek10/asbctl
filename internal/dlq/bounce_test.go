package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

func TestBounceID(t *testing.T) {
	since := runStart
	late, early := runStart.Add(time.Minute), runStart.Add(-time.Minute)
	resent := map[string]struct{}{"sent-id": {}, "original-id": {}}
	withOriginal := func(original string) map[string]any {
		return map[string]any{export.OriginalMessageIDProperty: original}
	}
	tests := []struct {
		name    string
		message *azservicebus.ReceivedMessage
		wantID  string
		wantOK  bool
	}{
		{name: "same message id after the run began", message: &azservicebus.ReceivedMessage{MessageID: "sent-id", EnqueuedTime: &late}, wantID: "sent-id", wantOK: true},
		{name: "original id property", message: &azservicebus.ReceivedMessage{MessageID: "new", EnqueuedTime: &late, ApplicationProperties: withOriginal("original-id")}, wantID: "original-id", wantOK: true},
		{name: "enqueued exactly at the start counts", message: &azservicebus.ReceivedMessage{MessageID: "sent-id", EnqueuedTime: &since}, wantID: "sent-id", wantOK: true},
		{name: "enqueued before the run", message: &azservicebus.ReceivedMessage{MessageID: "sent-id", EnqueuedTime: &early}},
		{name: "no enqueued time", message: &azservicebus.ReceivedMessage{MessageID: "sent-id"}},
		{name: "unknown id", message: &azservicebus.ReceivedMessage{MessageID: "stranger", EnqueuedTime: &late}},
		{name: "unknown original id", message: &azservicebus.ReceivedMessage{MessageID: "x", EnqueuedTime: &late, ApplicationProperties: withOriginal("stranger")}},
		{name: "empty original id does not match an empty entry", message: &azservicebus.ReceivedMessage{MessageID: "x", EnqueuedTime: &late, ApplicationProperties: withOriginal("")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotID, gotOK := bounceID(tc.message, resent, since)

			if gotID != tc.wantID || gotOK != tc.wantOK {
				t.Errorf("bounceID() = %q, %v, want %q, %v", gotID, gotOK, tc.wantID, tc.wantOK)
			}
		})
	}
}

func TestBounceShareAndShouldStop(t *testing.T) {
	tests := []struct {
		name            string
		percent         float64
		bounced, resent int
		wantShare       float64
		wantStop        bool
	}{
		{name: "nothing resent", percent: 5, bounced: 0, resent: 0, wantShare: 0, wantStop: false},
		{name: "below the threshold", percent: 5, bounced: 4, resent: 100, wantShare: 4, wantStop: false},
		{name: "at the threshold", percent: 5, bounced: 5, resent: 100, wantShare: 5, wantStop: true},
		{name: "above the threshold", percent: 5, bounced: 50, resent: 100, wantShare: 50, wantStop: true},
		{name: "disabled", percent: 0, bounced: 100, resent: 100, wantShare: 100, wantStop: false},
		{name: "one in three", percent: 33, bounced: 1, resent: 3, wantShare: 100.0 / 3, wantStop: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := BounceShare(tc.bounced, tc.resent); got != tc.wantShare {
				t.Errorf("BounceShare(%d, %d) = %v, want %v", tc.bounced, tc.resent, got, tc.wantShare)
			}
			if got := ShouldStopOnBounce(tc.percent, tc.bounced, tc.resent); got != tc.wantStop {
				t.Errorf("ShouldStopOnBounce(%v, %d, %d) = %v, want %v", tc.percent, tc.bounced, tc.resent, got, tc.wantStop)
			}
		})
	}
}

func TestBounceScannerReadsForwardFromItsCursor(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "old", "", "r"), dlMessage(2, "old2", "", "r"))
	scanner := NewBounceScanner(queue, runStart, 0)
	scanner.Track("sent", "orig")

	first, err := scanner.Scan(context.Background())
	if err != nil || first != 0 {
		t.Fatalf("first Scan() = %d, %v, want 0 bounces: the queue only holds old messages", first, err)
	}

	queue.add(bounceOf(10, "sent"))
	queue.add(bounceOf(11, "unrelated"))
	second, _ := scanner.Scan(context.Background())
	third, _ := scanner.Scan(context.Background())

	if second != 1 || third != 0 || scanner.Bounced() != 1 {
		t.Errorf("scans = %d then %d, Bounced() = %d, want 1 then 0 and 1 in total", second, third, scanner.Bounced())
	}
	if scanner.cursor != 12 {
		t.Errorf("cursor = %d, want 12: past the last message read", scanner.cursor)
	}
}

func TestBounceScannerCountsAMessageOnlyOnce(t *testing.T) {
	queue := newFakeQueue()
	scanner := NewBounceScanner(queue, runStart, 0)
	scanner.Track("sent", "")
	queue.add(bounceOf(10, "sent"))
	queue.add(bounceOf(11, "sent"))

	got, _ := scanner.Scan(context.Background())

	if got != 1 {
		t.Errorf("Scan() = %d, want 1: one id bounced twice is one resent message that came back", got)
	}
}

func TestBounceScannerAllowsForClockSkew(t *testing.T) {
	queue := newFakeQueue()
	scanner := NewBounceScanner(queue, runStart, 0)
	scanner.Track("sent", "")
	skewed := bounceOf(10, "sent")
	enqueued := runStart.Add(-30 * time.Second)
	skewed.EnqueuedTime = &enqueued
	queue.add(skewed)

	if got, _ := scanner.Scan(context.Background()); got != 1 {
		t.Errorf("Scan() = %d, want 1: a message 30 seconds before the start is inside the skew allowance", got)
	}
}

func TestBounceScannerIgnoresEmptyIDs(t *testing.T) {
	scanner := NewBounceScanner(newFakeQueue(), runStart, 0)

	scanner.Track("", "")

	if len(scanner.resent) != 0 {
		t.Errorf("resent = %v, want empty ids not tracked", scanner.resent)
	}
}
