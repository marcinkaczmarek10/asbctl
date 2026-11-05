package export

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// numberedPeeker serves messages numbered 1..total, alternating payload types A and B.
type numberedPeeker struct{ total int64 }

func (p numberedPeeker) PeekMessages(_ context.Context, count int, options *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	start := int64(1)
	if options.FromSequenceNumber != nil {
		start = *options.FromSequenceNumber
	}
	var page []*azservicebus.ReceivedMessage
	for seq := start; seq <= p.total && len(page) < count; seq++ {
		payloadType := "A"
		if seq%2 == 0 {
			payloadType = "B"
		}
		page = append(page, &azservicebus.ReceivedMessage{
			SequenceNumber: &seq, MessageID: "m", ApplicationProperties: map[string]any{"PayloadTypeId": payloadType},
		})
	}
	return page, nil
}

func TestMessagesWritesOneRecordPerLineAndReportsProgress(t *testing.T) {
	var out bytes.Buffer
	var progress [][2]int

	result, err := Messages(context.Background(), numberedPeeker{total: 250}, MessagesOptions{Max: 250}, &out,
		func(peeked, exported int) { progress = append(progress, [2]int{peeked, exported}) })

	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}
	if result != (MessagesResult{Peeked: 250, Exported: 250, Partial: true}) {
		t.Errorf("result = %+v, want 250 peeked, 250 exported, partial because Max was reached", result)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 250 {
		t.Errorf("lines = %d, want 250", lines)
	}
	wantProgress := [][2]int{{100, 100}, {200, 200}, {250, 250}}
	if len(progress) != 3 || progress[0] != wantProgress[0] || progress[1] != wantProgress[1] || progress[2] != wantProgress[2] {
		t.Errorf("progress = %v, want %v", progress, wantProgress)
	}
}

func TestMessagesFilterDropsOutputButNotPeekCount(t *testing.T) {
	var out bytes.Buffer

	result, err := Messages(context.Background(), numberedPeeker{total: 10}, MessagesOptions{Max: 100, PayloadTypeID: "B"}, &out, func(int, int) {})

	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}
	if result != (MessagesResult{Peeked: 10, Exported: 5, Partial: false}) {
		t.Errorf("result = %+v, want 10 peeked, 5 exported, not partial", result)
	}
	if strings.Contains(out.String(), `"PayloadTypeId":"A"`) {
		t.Errorf("output holds a payload type A record: %s", out.String())
	}
}

func TestMessagesIsNotPartialWhenTheEntityHoldsLessThanMax(t *testing.T) {
	result, err := Messages(context.Background(), numberedPeeker{total: 3}, MessagesOptions{Max: 4}, &bytes.Buffer{}, func(int, int) {})

	if err != nil || result.Partial || result.Peeked != 3 {
		t.Errorf("Messages() = %+v, %v, want 3 peeked and not partial", result, err)
	}
}

func TestMessagesIsPartialWhenPeekFillsMax(t *testing.T) {
	result, _ := Messages(context.Background(), numberedPeeker{total: 3}, MessagesOptions{Max: 3}, &bytes.Buffer{}, func(int, int) {})

	if !result.Partial {
		t.Errorf("result = %+v, want partial when peeked equals Max", result)
	}
}

type failingPeeker struct{ err error }

func (p failingPeeker) PeekMessages(context.Context, int, *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	return nil, p.err
}

func TestMessagesReturnsThePeekError(t *testing.T) {
	boom := errors.New("service unavailable")

	_, err := Messages(context.Background(), failingPeeker{err: boom}, MessagesOptions{Max: 10}, &bytes.Buffer{}, func(int, int) {})

	if !errors.Is(err, boom) {
		t.Errorf("Messages() error = %v, want %v", err, boom)
	}
}

func TestWriteExportResult(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		result ExportResult
		want   string
	}{
		{name: "json", format: FormatJSON, result: ExportResult{Schema: 1, Exported: 3, File: "out.ndjson"}, want: `{"schema":1,"exported":3,"file":"out.ndjson","partial":false}` + "\n"},
		{name: "table", format: FormatTable, result: ExportResult{Exported: 3, File: "out.ndjson"}, want: "Exported 3 messages to out.ndjson.\n"},
		{name: "table partial", format: FormatTable, result: ExportResult{Exported: 3, File: "out.ndjson", Partial: true}, want: "Exported 3 messages to out.ndjson. The entity may hold more than --max.\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			if err := WriteExportResult(&out, tc.format, tc.result); err != nil || out.String() != tc.want {
				t.Errorf("WriteExportResult() = %q, %v, want %q", out.String(), err, tc.want)
			}
		})
	}
}

func TestWriteExportResultReturnsTheWriteError(t *testing.T) {
	if err := WriteExportResult(failingWriter{}, FormatJSON, ExportResult{}); err == nil {
		t.Error("WriteExportResult() error = nil, want the write error")
	}
}
