package bus

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

// fakeEntityReader returns canned responses and errors, one field per admin call.
type fakeEntityReader struct {
	queueRuntime *admin.GetQueueRuntimePropertiesResponse
	queue        *admin.GetQueueResponse
	subRuntime   *admin.GetSubscriptionRuntimePropertiesResponse
	subscription *admin.GetSubscriptionResponse
	topic        *admin.GetTopicResponse
	err          error
}

func (f fakeEntityReader) GetQueueRuntimeProperties(context.Context, string, *admin.GetQueueRuntimePropertiesOptions) (*admin.GetQueueRuntimePropertiesResponse, error) {
	return f.queueRuntime, f.err
}

func (f fakeEntityReader) GetQueue(context.Context, string, *admin.GetQueueOptions) (*admin.GetQueueResponse, error) {
	return f.queue, f.err
}

func (f fakeEntityReader) GetSubscriptionRuntimeProperties(context.Context, string, string, *admin.GetSubscriptionRuntimePropertiesOptions) (*admin.GetSubscriptionRuntimePropertiesResponse, error) {
	return f.subRuntime, f.err
}

func (f fakeEntityReader) GetSubscription(context.Context, string, string, *admin.GetSubscriptionOptions) (*admin.GetSubscriptionResponse, error) {
	return f.subscription, f.err
}

func (f fakeEntityReader) GetTopic(context.Context, string, *admin.GetTopicOptions) (*admin.GetTopicResponse, error) {
	return f.topic, f.err
}

func TestReadEntityFactsForAQueue(t *testing.T) {
	reader := fakeEntityReader{
		queueRuntime: &admin.GetQueueRuntimePropertiesResponse{QueueRuntimeProperties: admin.QueueRuntimeProperties{
			ActiveMessageCount: 12, DeadLetterMessageCount: 850, ScheduledMessageCount: 3, TransferDeadLetterMessageCount: 1,
		}},
		queue: &admin.GetQueueResponse{QueueProperties: admin.QueueProperties{
			MaxDeliveryCount: ptr(int32(10)), LockDuration: ptr("PT1M"),
			RequiresDuplicateDetection: ptr(true), DuplicateDetectionHistoryTimeWindow: ptr("PT10M"),
		}},
	}

	got, err := ReadEntityFacts(context.Background(), reader, Target{Queue: "orders", DeadLetter: true})

	if err != nil {
		t.Fatalf("ReadEntityFacts() error = %v", err)
	}
	want := EntityFacts{
		Queue: "orders", Available: true,
		ActiveMessages: ptr(int64(12)), DeadLetterMessages: ptr(int64(850)), ScheduledMessages: ptr(int64(3)), TransferDeadLetterMessages: ptr(int64(1)),
		MaxDeliveryCount: ptr(int32(10)), LockDuration: "PT1M", RequiresDuplicateDetection: ptr(true), DuplicateDetectionWindow: "PT10M",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadEntityFacts() = %+v, want %+v", got, want)
	}
}

func TestReadEntityFactsForASubscriptionTakesDuplicateDetectionFromTheTopic(t *testing.T) {
	reader := fakeEntityReader{
		subRuntime: &admin.GetSubscriptionRuntimePropertiesResponse{SubscriptionRuntimeProperties: admin.SubscriptionRuntimeProperties{
			ActiveMessageCount: 5, DeadLetterMessageCount: 7, TransferDeadLetterMessageCount: 2,
		}},
		subscription: &admin.GetSubscriptionResponse{SubscriptionProperties: admin.SubscriptionProperties{MaxDeliveryCount: ptr(int32(5)), LockDuration: ptr("PT30S")}},
		topic:        &admin.GetTopicResponse{TopicProperties: admin.TopicProperties{RequiresDuplicateDetection: ptr(true), DuplicateDetectionHistoryTimeWindow: ptr("PT5M")}},
	}

	got, err := ReadEntityFacts(context.Background(), reader, Target{Topic: "events", Subscription: "billing", DeadLetter: true})

	if err != nil {
		t.Fatalf("ReadEntityFacts() error = %v", err)
	}
	want := EntityFacts{
		Topic: "events", Subscription: "billing", Available: true,
		ActiveMessages: ptr(int64(5)), DeadLetterMessages: ptr(int64(7)), TransferDeadLetterMessages: ptr(int64(2)),
		MaxDeliveryCount: ptr(int32(5)), LockDuration: "PT30S", RequiresDuplicateDetection: ptr(true), DuplicateDetectionWindow: "PT5M",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadEntityFacts() = %+v, want %+v", got, want)
	}
}

func TestReadEntityFactsErrors(t *testing.T) {
	tests := []struct {
		name          string
		reader        fakeEntityReader
		target        Target
		wantForbidden bool
		wantErr       bool
		wantFacts     EntityFacts
	}{
		{
			name:          "401 on a queue is unavailable and keeps the identity",
			reader:        fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusUnauthorized}},
			target:        Target{Queue: "orders"},
			wantForbidden: true, wantErr: true, wantFacts: EntityFacts{Queue: "orders"},
		},
		{
			name:          "403 on a subscription is unavailable and keeps the identity",
			reader:        fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusForbidden}},
			target:        Target{Topic: "t", Subscription: "s"},
			wantForbidden: true, wantErr: true, wantFacts: EntityFacts{Topic: "t", Subscription: "s"},
		},
		{
			name:    "a server error is a failure and not an unavailable entity",
			reader:  fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusInternalServerError}},
			target:  Target{Queue: "orders"},
			wantErr: true,
		},
		{
			name:    "a plain error is a failure",
			reader:  fakeEntityReader{err: errors.New("network down")},
			target:  Target{Queue: "orders"},
			wantErr: true,
		},
		{name: "a missing queue is a failure", reader: fakeEntityReader{}, target: Target{Queue: "gone"}, wantErr: true},
		{name: "a missing subscription is a failure", reader: fakeEntityReader{}, target: Target{Topic: "t", Subscription: "gone"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReadEntityFacts(context.Background(), tc.reader, tc.target)

			if (err != nil) != tc.wantErr {
				t.Fatalf("ReadEntityFacts() error = %v, want error %v", err, tc.wantErr)
			}
			if forbidden := errors.Is(err, ErrEntityFactsForbidden); forbidden != tc.wantForbidden {
				t.Errorf("errors.Is(err, ErrEntityFactsForbidden) = %v, want %v", forbidden, tc.wantForbidden)
			}
			if !reflect.DeepEqual(got, tc.wantFacts) {
				t.Errorf("ReadEntityFacts() facts = %+v, want %+v", got, tc.wantFacts)
			}
		})
	}
}
