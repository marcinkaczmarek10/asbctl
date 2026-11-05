package export

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

func ptr[T any](v T) *T { return &v }

func TestRecordFromReceivedBody(t *testing.T) {
	tests := []struct {
		name         string
		body         []byte
		wantBody     string
		wantBodyText *string
	}{
		{name: "utf-8 body keeps its text", body: []byte("héllo"), wantBody: "aMOpbGxv", wantBodyText: ptr("héllo")},
		{name: "binary body has no text", body: []byte{0xff, 0xfe, 0x00}, wantBody: "//4A", wantBodyText: nil},
		{name: "empty body has empty text", body: nil, wantBody: "", wantBodyText: ptr("")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := recordFromReceived(&azservicebus.ReceivedMessage{Body: tc.body})

			if got.Body != tc.wantBody {
				t.Errorf("Body = %q, want %q", got.Body, tc.wantBody)
			}
			if !reflect.DeepEqual(got.BodyText, tc.wantBodyText) {
				t.Errorf("BodyText = %v, want %v", got.BodyText, tc.wantBodyText)
			}
		})
	}
}

func TestRecordFromReceivedProperties(t *testing.T) {
	enqueued := time.Date(2026, 11, 5, 14, 30, 0, 500, time.FixedZone("CET", 3600))
	uuid := amqp.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	tests := []struct {
		name       string
		properties map[string]any
		want       map[string]any
	}{
		{name: "no properties are omitted", properties: nil, want: nil},
		{
			name:       "json native types stay as they are",
			properties: map[string]any{"s": "x", "b": true, "i": int64(7), "f": 1.5},
			want:       map[string]any{"s": "x", "b": true, "i": int64(7), "f": 1.5},
		},
		{
			name:       "timestamp becomes a typed utc value",
			properties: map[string]any{"t": enqueued},
			want:       map[string]any{"t": typedValue{Type: "timestamp", Value: "2026-11-05T13:30:00.0000005Z"}},
		},
		{
			name:       "binary becomes base64",
			properties: map[string]any{"b": []byte{0xff, 0x00}},
			want:       map[string]any{"b": typedValue{Type: "binary", Value: "/wA="}},
		},
		{
			name:       "uuid becomes its string form",
			properties: map[string]any{"u": uuid},
			want:       map[string]any{"u": typedValue{Type: "uuid", Value: "01020304-0506-0708-090a-0b0c0d0e0f10"}},
		},
		{
			name:       "symbol and duration are typed",
			properties: map[string]any{"y": amqp.Symbol("sym"), "d": 90 * time.Second},
			want:       map[string]any{"y": typedValue{Type: "symbol", Value: "sym"}, "d": typedValue{Type: "duration", Value: "1m30s"}},
		},
		{
			name:       "non-finite float is typed because json cannot hold it",
			properties: map[string]any{"n": math.NaN()},
			want:       map[string]any{"n": typedValue{Type: "double", Value: "NaN"}},
		},
		{
			name:       "float32 follows the same rule as float64",
			properties: map[string]any{"f": float32(1.5), "n": float32(math.NaN())},
			want:       map[string]any{"f": float32(1.5), "n": typedValue{Type: "float", Value: "NaN"}},
		},
		{
			name:       "an unknown type keeps its go type name",
			properties: map[string]any{"l": []any{"a", 1}},
			want:       map[string]any{"l": typedValue{Type: "[]interface {}", Value: "[a 1]"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := recordFromReceived(&azservicebus.ReceivedMessage{ApplicationProperties: tc.properties}).ApplicationProperties

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ApplicationProperties = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestRecordFromReceivedFields(t *testing.T) {
	enqueued := time.Date(2026, 11, 5, 14, 30, 0, 0, time.FixedZone("CET", 3600))
	message := &azservicebus.ReceivedMessage{
		SequenceNumber:             ptr(int64(42)),
		MessageID:                  "m-1",
		EnqueuedTime:               &enqueued,
		ContentType:                ptr("application/json"),
		Subject:                    ptr("subject"),
		TimeToLive:                 ptr(90 * time.Second),
		DeliveryCount:              3,
		DeadLetterReason:           ptr("MaxDeliveryCountExceeded"),
		DeadLetterErrorDescription: ptr("too many"),
		DeadLetterSource:           ptr("queue"),
	}

	got := recordFromReceived(message)

	want := record{
		Schema:                     1,
		SequenceNumber:             42,
		MessageID:                  "m-1",
		EnqueuedAt:                 "2026-11-05T13:30:00Z",
		ContentType:                "application/json",
		Subject:                    "subject",
		TimeToLiveSeconds:          90,
		DeliveryCount:              3,
		DeadLetterReason:           "MaxDeliveryCountExceeded",
		DeadLetterErrorDescription: "too many",
		DeadLetterSource:           "queue",
		BodyText:                   ptr(""),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recordFromReceived() = %+v, want %+v", got, want)
	}
}

func TestRecordJSONOmitsEmptyFieldsButKeepsBody(t *testing.T) {
	got, err := json.Marshal(recordFromReceived(&azservicebus.ReceivedMessage{MessageID: "m", Body: []byte{0xff}}))
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	want := `{"schema":1,"message_id":"m","body":"/w=="}`
	if string(got) != want {
		t.Errorf("json = %s, want %s", got, want)
	}
}

func TestPayloadTypeID(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]any
		want       string
	}{
		{name: "present", properties: map[string]any{"PayloadTypeId": "OrderPlaced"}, want: "OrderPlaced"},
		{name: "absent", properties: map[string]any{"Other": "x"}, want: ""},
		{name: "no properties", properties: nil, want: ""},
		{name: "not a string", properties: map[string]any{"PayloadTypeId": int64(5)}, want: ""},
		{name: "name is case sensitive", properties: map[string]any{"payloadtypeid": "OrderPlaced"}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := recordFromReceived(&azservicebus.ReceivedMessage{ApplicationProperties: tc.properties}).payloadTypeID()

			if got != tc.want {
				t.Errorf("payloadTypeID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMatchesPayloadType(t *testing.T) {
	order := recordFromReceived(&azservicebus.ReceivedMessage{ApplicationProperties: map[string]any{"PayloadTypeId": "OrderPlaced"}})
	untyped := recordFromReceived(&azservicebus.ReceivedMessage{})
	tests := []struct {
		name string
		rec  record
		id   string
		want bool
	}{
		{name: "empty filter keeps everything", rec: order, id: "", want: true},
		{name: "empty filter keeps untyped messages", rec: untyped, id: "", want: true},
		{name: "equal id matches", rec: order, id: "OrderPlaced", want: true},
		{name: "other id is dropped", rec: order, id: "OrderShipped", want: false},
		{name: "untyped message is dropped by a filter", rec: untyped, id: "OrderPlaced", want: false},
		{name: "id match is case sensitive", rec: order, id: "orderplaced", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesPayloadType(tc.rec, tc.id); got != tc.want {
				t.Errorf("matchesPayloadType(id %q) = %v, want %v", tc.id, got, tc.want)
			}
		})
	}
}

func TestBodyCell(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want string
	}{
		{name: "short text is kept", body: []byte("hello"), want: "hello"},
		{name: "text is cut at 60 characters", body: []byte(strings.Repeat("a", 61)), want: strings.Repeat("a", 60)},
		{name: "the cut counts characters and not bytes", body: []byte(strings.Repeat("ł", 61)), want: strings.Repeat("ł", 60)},
		{name: "line breaks and tabs become spaces", body: []byte("a\nb\tc\r"), want: "a b c "},
		{name: "binary body shows its size", body: []byte{0xff, 0xfe, 0xfd}, want: "<3 bytes binary>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := bodyCell(recordFromReceived(&azservicebus.ReceivedMessage{Body: tc.body})); got != tc.want {
				t.Errorf("bodyCell(%q) = %q, want %q", tc.body, got, tc.want)
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		in      string
		want    Format
		wantErr bool
	}{
		{in: "table", want: FormatTable},
		{in: "json", want: FormatJSON},
		{in: "yaml", wantErr: true},
		{in: "", wantErr: true},
		{in: "JSON", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseFormat(tc.in)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("ParseFormat(%q) = %q, %v, want %q, error %v", tc.in, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func message(seq int64, payloadTypeID, subject string, body []byte) *azservicebus.ReceivedMessage {
	enqueued := time.Date(2026, 11, 5, 13, 30, 0, 0, time.UTC)
	m := &azservicebus.ReceivedMessage{
		SequenceNumber: &seq, MessageID: "id-" + subject, EnqueuedTime: &enqueued, Subject: &subject, Body: body,
		DeadLetterReason: ptr("Poison"),
	}
	if payloadTypeID != "" {
		m.ApplicationProperties = map[string]any{"PayloadTypeId": payloadTypeID}
	}
	return m
}

func TestWriterTable(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{
			name:    "table has the header and one row per message",
			options: Options{Format: FormatTable},
			want: "SEQ  ENQUEUED              MESSAGE_ID  PAYLOAD_TYPE  SUBJECT  BODY\n" +
				"1    2026-11-05T13:30:00Z  id-one      Order         one      hello\n" +
				"2    2026-11-05T13:30:00Z  id-two                    two      <1 bytes binary>\n",
		},
		{
			name:    "dead-letter table adds the reason column",
			options: Options{Format: FormatTable, DeadLetter: true},
			want: "SEQ  ENQUEUED              MESSAGE_ID  PAYLOAD_TYPE  SUBJECT  DLQ_REASON  BODY\n" +
				"1    2026-11-05T13:30:00Z  id-one      Order         one      Poison      hello\n" +
				"2    2026-11-05T13:30:00Z  id-two                    two      Poison      <1 bytes binary>\n",
		},
		{
			name:    "payload filter drops other messages",
			options: Options{Format: FormatTable, PayloadTypeID: "Order"},
			want: "SEQ  ENQUEUED              MESSAGE_ID  PAYLOAD_TYPE  SUBJECT  BODY\n" +
				"1    2026-11-05T13:30:00Z  id-one      Order         one      hello\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			_, err := NewWriter(&out, tc.options).WriteMessages([]*azservicebus.ReceivedMessage{
				message(1, "Order", "one", []byte("hello")),
				message(2, "", "two", []byte{0xff}),
			})

			if err != nil {
				t.Fatalf("WriteMessages() error = %v", err)
			}
			if out.String() != tc.want {
				t.Errorf("table =\n%s\nwant\n%s", out.String(), tc.want)
			}
		})
	}
}

func TestTableHeaderIsWrittenOnceAndNotForEmptyPages(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out, Options{Format: FormatTable, PayloadTypeID: "Order"})

	_, _ = writer.WriteMessages([]*azservicebus.ReceivedMessage{message(1, "Other", "skip", nil)})
	if out.Len() != 0 {
		t.Fatalf("output after a fully filtered page = %q, want empty", out.String())
	}
	_, _ = writer.WriteMessages([]*azservicebus.ReceivedMessage{message(2, "Order", "a", []byte("x"))})
	_, _ = writer.WriteMessages([]*azservicebus.ReceivedMessage{message(3, "Order", "b", []byte("y"))})

	if got := strings.Count(out.String(), "SEQ"); got != 1 {
		t.Errorf("header count = %d, want 1 in\n%s", got, out.String())
	}
	if got := strings.Count(out.String(), "\n"); got != 3 {
		t.Errorf("line count = %d, want 3 in\n%s", got, out.String())
	}
}

func TestWriterJSONStreamsOneObjectPerLine(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out, Options{Format: FormatJSON, PayloadTypeID: "Order"})

	written, err := writer.WriteMessages([]*azservicebus.ReceivedMessage{
		message(1, "Order", "<one>", []byte("a")),
		message(2, "Other", "two", []byte("b")),
		message(3, "Order", "three", []byte("c")),
	})

	if err != nil {
		t.Fatalf("WriteMessages() error = %v", err)
	}
	if written != 2 {
		t.Errorf("written = %d, want 2", written)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("json lines = %d, want 2 in\n%s", len(lines), out.String())
	}
	var first record
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 1 is not json: %v", err)
	}
	if first.SequenceNumber != 1 || first.Subject != "<one>" {
		t.Errorf("line 1 = %+v, want sequence 1 and subject <one>", first)
	}
	if !strings.Contains(lines[0], `"<one>"`) {
		t.Errorf("line 1 = %s, want < and > written unescaped", lines[0])
	}
}

func TestWriteError(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		want   string
	}{
		{name: "json mode writes the error object", format: FormatJSON, want: `{"schema":1,"error":{"code":"peek_failed","message":"boom"}}` + "\n"},
		{name: "table mode writes a plain line", format: FormatTable, want: "asbctl: boom\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			WriteError(&out, tc.format, "peek_failed", errors.New("boom"))

			if out.String() != tc.want {
				t.Errorf("WriteError() = %q, want %q", out.String(), tc.want)
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestWriterReturnsTheWriteError(t *testing.T) {
	for _, format := range []Format{FormatTable, FormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			_, err := NewWriter(failingWriter{}, Options{Format: format}).WriteMessages([]*azservicebus.ReceivedMessage{message(1, "", "one", nil)})

			if err == nil || !strings.Contains(err.Error(), "broken pipe") {
				t.Errorf("WriteMessages() error = %v, want it to wrap the write error", err)
			}
		})
	}
}
