package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	// DefaultLockDuration is assumed when the lock duration of an entity cannot be read.
	DefaultLockDuration = 30 * time.Second
	// AssumedDuplicateWindow is assumed when the duplicate detection settings of an entity cannot be read.
	AssumedDuplicateWindow = 10 * time.Minute

	receiveTimeout = 10 * time.Second
)

// SettlingReceiver is the set of peek-lock receiver calls that resend and purge make.
type SettlingReceiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.AbandonMessageOptions) error
}

var _ SettlingReceiver = (*azservicebus.Receiver)(nil)

// ReceiveBatch receives up to count messages. A receive that times out with nothing in it means the entity
// has nothing left to give, so it returns no messages and no error.
func ReceiveBatch(ctx context.Context, receiver SettlingReceiver, count int) ([]*azservicebus.ReceivedMessage, error) {
	receiveCtx, cancel := context.WithTimeout(ctx, receiveTimeout)
	defer cancel()

	messages, err := receiver.ReceiveMessages(receiveCtx, count, nil)
	if err != nil && len(messages) == 0 && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return nil, nil
	}
	if err != nil && len(messages) == 0 {
		return nil, fmt.Errorf("receive messages: %w", err)
	}
	return messages, nil
}

// ReceiveBatchSize returns how many messages to receive at once: as many as can be sent in half the lock
// duration at the given rate, so that no message loses its lock while it waits. Without a rate it is the largest batch.
func ReceiveBatchSize(lockDuration time.Duration, rate int) int {
	if rate <= 0 {
		return MaxBatchSize
	}
	sendable := int(float64(rate) * lockDuration.Seconds() / 2)
	return max(1, min(MaxBatchSize, sendable))
}

// LockDurationFrom returns the lock duration in the entity facts, or DefaultLockDuration when they are unavailable.
func LockDurationFrom(facts EntityFacts) time.Duration {
	if !facts.Available {
		return DefaultLockDuration
	}
	lock, err := ParseISODuration(facts.LockDuration)
	if err != nil || lock <= 0 {
		return DefaultLockDuration
	}
	return lock
}

// DuplicateDetection says whether a send target drops a repeated MessageId inside a time window.
type DuplicateDetection struct {
	Enabled bool
	Window  time.Duration
	// Assumed is true when the settings could not be read and the safe answer was taken instead.
	Assumed bool
}

// AssumedDuplicateDetection is the answer when the settings are unreadable: on, for AssumedDuplicateWindow.
func AssumedDuplicateDetection() DuplicateDetection {
	return DuplicateDetection{Enabled: true, Window: AssumedDuplicateWindow, Assumed: true}
}

// DuplicateDetectionFrom reads the two settings as the admin API returns them.
func DuplicateDetectionFrom(required *bool, window string) (DuplicateDetection, error) {
	if required == nil || !*required {
		return DuplicateDetection{}, nil
	}
	parsed, err := ParseISODuration(window)
	if err != nil {
		return DuplicateDetection{}, fmt.Errorf("duplicate detection window: %w", err)
	}
	return DuplicateDetection{Enabled: true, Window: parsed}, nil
}

// ReadSendTarget reads the duplicate detection settings of the queue or topic that messages are sent to. A
// permission error returns the assumed settings with an error that wraps ErrEntityFactsForbidden.
func ReadSendTarget(ctx context.Context, reader EntityReader, name string, isTopic bool) (DuplicateDetection, error) {
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	var required *bool
	var window *string
	if isTopic {
		topic, err := reader.GetTopic(ctx, name, nil)
		if err != nil {
			return forbiddenOr(err, "read topic")
		}
		if topic == nil {
			return DuplicateDetection{}, fmt.Errorf("topic %q not found", name)
		}
		required, window = topic.RequiresDuplicateDetection, topic.DuplicateDetectionHistoryTimeWindow
	} else {
		queue, err := reader.GetQueue(ctx, name, nil)
		if err != nil {
			return forbiddenOr(err, "read queue")
		}
		if queue == nil {
			return DuplicateDetection{}, fmt.Errorf("queue %q not found", name)
		}
		required, window = queue.RequiresDuplicateDetection, queue.DuplicateDetectionHistoryTimeWindow
	}
	return DuplicateDetectionFrom(required, valueOf(window))
}

func forbiddenOr(err error, what string) (DuplicateDetection, error) {
	if isForbidden(err) {
		return AssumedDuplicateDetection(), fmt.Errorf("%w: %w", ErrEntityFactsForbidden, err)
	}
	return DuplicateDetection{}, fmt.Errorf("%s: %w", what, err)
}

var isoDuration = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// ParseISODuration reads the ISO 8601 durations that the admin API returns, such as PT1M or P0DT0H10M0S.
// A value too large for a time.Duration, such as the service maximum, is clamped to the largest Duration.
func ParseISODuration(text string) (time.Duration, error) {
	match := isoDuration.FindStringSubmatch(text)
	if match == nil || text == "P" || strings.HasSuffix(text, "T") {
		return 0, fmt.Errorf("%q is not an ISO 8601 duration", text)
	}

	var seconds float64
	for i, unit := range []float64{86400, 3600, 60, 1} {
		if match[i+1] == "" {
			continue
		}
		value, err := strconv.ParseFloat(match[i+1], 64)
		if err != nil {
			return 0, fmt.Errorf("%q is not an ISO 8601 duration: %w", text, err)
		}
		seconds += value * unit
	}
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64), nil
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
