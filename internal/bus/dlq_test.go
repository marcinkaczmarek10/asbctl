package bus

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

func TestParseISODuration(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "PT1M", want: time.Minute},
		{in: "PT30S", want: 30 * time.Second},
		{in: "PT10M", want: 10 * time.Minute},
		{in: "PT1H30M", want: 90 * time.Minute},
		{in: "P1D", want: 24 * time.Hour},
		{in: "P0DT0H1M0S", want: time.Minute},
		{in: "P1DT2H3M4S", want: 26*time.Hour + 3*time.Minute + 4*time.Second},
		{in: "PT1.5S", want: 1500 * time.Millisecond},
		{in: "P10675199DT2H48M5.4775807S", want: time.Duration(1<<63 - 1)},
		{in: "", wantErr: true},
		{in: "P", wantErr: true},
		{in: "PT", wantErr: true},
		{in: "10M", wantErr: true},
		{in: "PT1X", wantErr: true},
		{in: "1 minute", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseISODuration(tc.in)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("ParseISODuration(%q) = %v, %v, want %v, error %v", tc.in, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestReceiveBatchSize(t *testing.T) {
	tests := []struct {
		name string
		lock time.Duration
		rate int
		want int
	}{
		{name: "no rate gives the largest batch", lock: time.Minute, rate: 0, want: 100},
		{name: "half the lock at the rate", lock: time.Minute, rate: 2, want: 60},
		{name: "capped at the largest batch", lock: time.Minute, rate: 50, want: 100},
		{name: "never below one", lock: time.Second, rate: 1, want: 1},
		{name: "a rate of one over a long lock", lock: 5 * time.Minute, rate: 1, want: 100},
		{name: "a slow rate over the default lock", lock: DefaultLockDuration, rate: 3, want: 45},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReceiveBatchSize(tc.lock, tc.rate); got != tc.want {
				t.Errorf("ReceiveBatchSize(%v, %d) = %d, want %d", tc.lock, tc.rate, got, tc.want)
			}
		})
	}
}

func TestLockDurationFrom(t *testing.T) {
	tests := []struct {
		name  string
		facts EntityFacts
		want  time.Duration
	}{
		{name: "available facts", facts: EntityFacts{Available: true, LockDuration: "PT2M"}, want: 2 * time.Minute},
		{name: "unavailable facts", facts: EntityFacts{LockDuration: "PT2M"}, want: DefaultLockDuration},
		{name: "unparsable duration", facts: EntityFacts{Available: true, LockDuration: "soon"}, want: DefaultLockDuration},
		{name: "empty duration", facts: EntityFacts{Available: true}, want: DefaultLockDuration},
		{name: "zero duration", facts: EntityFacts{Available: true, LockDuration: "PT0S"}, want: DefaultLockDuration},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LockDurationFrom(tc.facts); got != tc.want {
				t.Errorf("LockDurationFrom() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDuplicateDetectionFrom(t *testing.T) {
	yes, no := true, false
	tests := []struct {
		name     string
		required *bool
		window   string
		want     DuplicateDetection
		wantErr  bool
	}{
		{name: "on with a window", required: &yes, window: "PT10M", want: DuplicateDetection{Enabled: true, Window: 10 * time.Minute}},
		{name: "off", required: &no, window: "PT10M", want: DuplicateDetection{}},
		{name: "unset is off", required: nil, want: DuplicateDetection{}},
		{name: "on with a bad window", required: &yes, window: "ten", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DuplicateDetectionFrom(tc.required, tc.window)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("DuplicateDetectionFrom() = %+v, %v, want %+v, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestAssumedDuplicateDetectionIsOnForTenMinutes(t *testing.T) {
	want := DuplicateDetection{Enabled: true, Window: 10 * time.Minute, Assumed: true}
	if got := AssumedDuplicateDetection(); got != want {
		t.Errorf("AssumedDuplicateDetection() = %+v, want %+v", got, want)
	}
}

func TestReadSendTarget(t *testing.T) {
	yes := true
	queueReader := fakeEntityReader{queue: &admin.GetQueueResponse{QueueProperties: admin.QueueProperties{
		RequiresDuplicateDetection: &yes, DuplicateDetectionHistoryTimeWindow: ptr("PT5M"),
	}}}
	topicReader := fakeEntityReader{topic: &admin.GetTopicResponse{TopicProperties: admin.TopicProperties{
		RequiresDuplicateDetection: &yes, DuplicateDetectionHistoryTimeWindow: ptr("PT20M"),
	}}}
	tests := []struct {
		name          string
		reader        fakeEntityReader
		isTopic       bool
		want          DuplicateDetection
		wantForbidden bool
		wantErr       bool
	}{
		{name: "queue", reader: queueReader, want: DuplicateDetection{Enabled: true, Window: 5 * time.Minute}},
		{name: "topic", reader: topicReader, isTopic: true, want: DuplicateDetection{Enabled: true, Window: 20 * time.Minute}},
		{name: "queue without permission", reader: fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusForbidden}}, want: AssumedDuplicateDetection(), wantForbidden: true, wantErr: true},
		{name: "topic without permission", reader: fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusUnauthorized}}, isTopic: true, want: AssumedDuplicateDetection(), wantForbidden: true, wantErr: true},
		{name: "server error", reader: fakeEntityReader{err: &azcore.ResponseError{StatusCode: http.StatusInternalServerError}}, wantErr: true},
		{name: "missing queue", reader: fakeEntityReader{}, wantErr: true},
		{name: "missing topic", reader: fakeEntityReader{}, isTopic: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReadSendTarget(context.Background(), tc.reader, "entity", tc.isTopic)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("ReadSendTarget() = %+v, %v, want %+v, error %v", got, err, tc.want, tc.wantErr)
			}
			if forbidden := errors.Is(err, ErrEntityFactsForbidden); forbidden != tc.wantForbidden {
				t.Errorf("errors.Is(err, ErrEntityFactsForbidden) = %v, want %v", forbidden, tc.wantForbidden)
			}
		})
	}
}

type fakeSettlingReceiver struct {
	messages []*azservicebus.ReceivedMessage
	err      error
}

func (f fakeSettlingReceiver) ReceiveMessages(ctx context.Context, _ int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	return f.messages, f.err
}

func (fakeSettlingReceiver) CompleteMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.CompleteMessageOptions) error {
	return nil
}

func (fakeSettlingReceiver) AbandonMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.AbandonMessageOptions) error {
	return nil
}

func TestReceiveBatch(t *testing.T) {
	one := []*azservicebus.ReceivedMessage{{MessageID: "m"}}
	boom := errors.New("link detached")
	tests := []struct {
		name     string
		receiver fakeSettlingReceiver
		wantLen  int
		wantErr  error
	}{
		{name: "messages", receiver: fakeSettlingReceiver{messages: one}, wantLen: 1},
		{name: "a receive that timed out empty is an empty batch", receiver: fakeSettlingReceiver{err: context.DeadlineExceeded}},
		{name: "an error with nothing received is an error", receiver: fakeSettlingReceiver{err: boom}, wantErr: boom},
		{name: "messages that arrived before the error are kept", receiver: fakeSettlingReceiver{messages: one, err: boom}, wantLen: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReceiveBatch(context.Background(), tc.receiver, 10)

			if !errors.Is(err, tc.wantErr) || len(got) != tc.wantLen {
				t.Errorf("ReceiveBatch() = %d messages, %v, want %d, %v", len(got), err, tc.wantLen, tc.wantErr)
			}
		})
	}
}

func TestReceiveBatchReportsATimeoutOfTheCallersOwnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ReceiveBatch(ctx, fakeSettlingReceiver{err: context.DeadlineExceeded}, 10)

	if err == nil {
		t.Error("ReceiveBatch() error = nil, want the error when the caller's context is already done")
	}
}
