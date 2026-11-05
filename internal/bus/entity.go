package bus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

const adminTimeout = 30 * time.Second

// ErrEntityFactsForbidden marks an admin read that the caller has no rights for.
var ErrEntityFactsForbidden = errors.New("no permission to read entity properties")

// EntityReader is the set of admin client reads that ReadEntityFacts calls.
type EntityReader interface {
	GetQueue(ctx context.Context, queueName string, options *admin.GetQueueOptions) (*admin.GetQueueResponse, error)
	GetQueueRuntimeProperties(ctx context.Context, queueName string, options *admin.GetQueueRuntimePropertiesOptions) (*admin.GetQueueRuntimePropertiesResponse, error)
	GetSubscription(ctx context.Context, topicName string, subscriptionName string, options *admin.GetSubscriptionOptions) (*admin.GetSubscriptionResponse, error)
	GetSubscriptionRuntimeProperties(ctx context.Context, topicName string, subscriptionName string, options *admin.GetSubscriptionRuntimePropertiesOptions) (*admin.GetSubscriptionRuntimePropertiesResponse, error)
	GetTopic(ctx context.Context, topicName string, options *admin.GetTopicOptions) (*admin.GetTopicResponse, error)
}

var _ EntityReader = (*admin.Client)(nil)

// EntityFacts is the entity block of the dead-letter summary. Counts and settings are omitted when Available
// is false, because reading them needs Manage rights that the caller may not have.
type EntityFacts struct {
	Queue                      string `json:"queue,omitempty"`
	Topic                      string `json:"topic,omitempty"`
	Subscription               string `json:"subscription,omitempty"`
	Available                  bool   `json:"available"`
	ActiveMessages             *int64 `json:"active_messages,omitempty"`
	DeadLetterMessages         *int64 `json:"dead_letter_messages,omitempty"`
	ScheduledMessages          *int64 `json:"scheduled_messages,omitempty"`
	TransferDeadLetterMessages *int64 `json:"transfer_dead_letter_messages,omitempty"`
	MaxDeliveryCount           *int32 `json:"max_delivery_count,omitempty"`
	LockDuration               string `json:"lock_duration,omitempty"`
	RequiresDuplicateDetection *bool  `json:"requires_duplicate_detection,omitempty"`
	DuplicateDetectionWindow   string `json:"duplicate_detection_window,omitempty"`
}

// ReadEntityFacts reads counts and settings for the target. A permission error returns facts with Available
// false and an error that wraps ErrEntityFactsForbidden, so the caller can carry on without them.
func ReadEntityFacts(ctx context.Context, reader EntityReader, target Target) (EntityFacts, error) {
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	var facts EntityFacts
	var err error
	if target.Queue != "" {
		facts, err = readQueueFacts(ctx, reader, target.Queue)
	} else {
		facts, err = readSubscriptionFacts(ctx, reader, target.Topic, target.Subscription)
	}

	if isForbidden(err) {
		return EntityFacts{Queue: target.Queue, Topic: target.Topic, Subscription: target.Subscription}, fmt.Errorf("%w: %w", ErrEntityFactsForbidden, err)
	}
	if err != nil {
		return EntityFacts{}, err
	}
	return facts, nil
}

func readQueueFacts(ctx context.Context, reader EntityReader, queue string) (EntityFacts, error) {
	runtime, err := reader.GetQueueRuntimeProperties(ctx, queue, nil)
	if err != nil {
		return EntityFacts{}, fmt.Errorf("read queue runtime properties: %w", err)
	}
	settings, err := reader.GetQueue(ctx, queue, nil)
	if err != nil {
		return EntityFacts{}, fmt.Errorf("read queue: %w", err)
	}
	if runtime == nil || settings == nil {
		return EntityFacts{}, fmt.Errorf("queue %q not found", queue)
	}

	return EntityFacts{
		Queue:                      queue,
		Available:                  true,
		ActiveMessages:             ptr(int64(runtime.ActiveMessageCount)),
		DeadLetterMessages:         ptr(int64(runtime.DeadLetterMessageCount)),
		ScheduledMessages:          ptr(int64(runtime.ScheduledMessageCount)),
		TransferDeadLetterMessages: ptr(int64(runtime.TransferDeadLetterMessageCount)),
		MaxDeliveryCount:           settings.MaxDeliveryCount,
		LockDuration:               valueOf(settings.LockDuration),
		RequiresDuplicateDetection: settings.RequiresDuplicateDetection,
		DuplicateDetectionWindow:   valueOf(settings.DuplicateDetectionHistoryTimeWindow),
	}, nil
}

// readSubscriptionFacts takes duplicate detection from the topic, because a subscription has none of its own.
func readSubscriptionFacts(ctx context.Context, reader EntityReader, topic, subscription string) (EntityFacts, error) {
	runtime, err := reader.GetSubscriptionRuntimeProperties(ctx, topic, subscription, nil)
	if err != nil {
		return EntityFacts{}, fmt.Errorf("read subscription runtime properties: %w", err)
	}
	settings, err := reader.GetSubscription(ctx, topic, subscription, nil)
	if err != nil {
		return EntityFacts{}, fmt.Errorf("read subscription: %w", err)
	}
	topicSettings, err := reader.GetTopic(ctx, topic, nil)
	if err != nil {
		return EntityFacts{}, fmt.Errorf("read topic: %w", err)
	}
	if runtime == nil || settings == nil || topicSettings == nil {
		return EntityFacts{}, fmt.Errorf("subscription %q of topic %q not found", subscription, topic)
	}

	// Subscription runtime properties have no scheduled count, so ScheduledMessages stays unset.
	return EntityFacts{
		Topic:                      topic,
		Subscription:               subscription,
		Available:                  true,
		ActiveMessages:             ptr(int64(runtime.ActiveMessageCount)),
		DeadLetterMessages:         ptr(int64(runtime.DeadLetterMessageCount)),
		TransferDeadLetterMessages: ptr(int64(runtime.TransferDeadLetterMessageCount)),
		MaxDeliveryCount:           settings.MaxDeliveryCount,
		LockDuration:               valueOf(settings.LockDuration),
		RequiresDuplicateDetection: topicSettings.RequiresDuplicateDetection,
		DuplicateDetectionWindow:   valueOf(topicSettings.DuplicateDetectionHistoryTimeWindow),
	}, nil
}

func isForbidden(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) &&
		(responseErr.StatusCode == http.StatusUnauthorized || responseErr.StatusCode == http.StatusForbidden)
}

func ptr[T any](v T) *T { return &v }

func valueOf(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
