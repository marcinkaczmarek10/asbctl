package export

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

func parseAll(t *testing.T, input string, newMessageID func() string) ([]bus.Outgoing, error) {
	t.Helper()
	var outgoings []bus.Outgoing
	for outgoing, err := range Outgoings(strings.NewReader(input), newMessageID) {
		if err != nil {
			return outgoings, err
		}
		outgoings = append(outgoings, outgoing)
	}
	return outgoings, nil
}

func TestOutgoingsRestoresTheContractMessages(t *testing.T) {
	input, err := os.ReadFile(filepath.Join(contractDir, "messages.ndjson"))
	if err != nil {
		t.Fatalf("read contract file: %v", err)
	}

	outgoings, err := parseAll(t, string(input), nil)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	if len(outgoings) != 4 {
		t.Fatalf("Outgoings() = %d messages, want 4", len(outgoings))
	}

	first := outgoings[0]
	if first.Line != 1 || first.MessageID != "msg-1" || first.PayloadTypeID != "OrderPlaced" || first.Message == nil || first.Annotated != nil {
		t.Fatalf("first = %+v, want line 1, msg-1, OrderPlaced and a data message", first)
	}
	message := first.Message
	if string(message.Body) != `{"id":42}` || *message.ContentType != "application/json" || *message.CorrelationID != "corr-1" ||
		*message.Subject != "order.placed" || *message.PartitionKey != "pk-1" || *message.To != "orders" ||
		*message.ReplyTo != "replies" || *message.MessageID != "msg-1" || *message.TimeToLive != time.Hour {
		t.Errorf("message = %+v, want the record fields carried over", message)
	}
	wantProperties := map[string]any{
		"PayloadTypeId": "OrderPlaced",
		"retries":       int64(3),
		"urgent":        true,
		"ratio":         1.5,
		"created":       time.Date(2026, 11, 5, 9, 59, 0, 500_000_000, time.UTC),
		"signature":     []byte{0xde, 0xad},
		"trace":         amqp.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	if !reflect.DeepEqual(message.ApplicationProperties, wantProperties) {
		t.Errorf("ApplicationProperties = %#v, want %#v", message.ApplicationProperties, wantProperties)
	}

	if !reflect.DeepEqual(outgoings[1].Message.Body, []byte{0xff, 0xfe, 0x00}) {
		t.Errorf("binary body = %v, want ff fe 00", outgoings[1].Message.Body)
	}
	if outgoings[1].Message.ApplicationProperties != nil {
		t.Errorf("dead-letter fields became properties: %v", outgoings[1].Message.ApplicationProperties)
	}

	value := outgoings[2]
	if value.Annotated == nil || value.Message != nil {
		t.Fatalf("value body = %+v, want an annotated message", value)
	}
	wantValue := map[string]any{"id": int64(7), "tags": []any{"a", "b"}}
	if !reflect.DeepEqual(value.Annotated.Body.Value, wantValue) {
		t.Errorf("Body.Value = %#v, want %#v", value.Annotated.Body.Value, wantValue)
	}

	if outgoings[3].Message == nil || len(outgoings[3].Message.Body) != 0 {
		t.Errorf("empty body = %+v, want a data message with no body", outgoings[3])
	}
}

func TestOutgoingsKeepsBrokerSetFieldsOutOfTheMessage(t *testing.T) {
	line := `{"schema":1,"sequence_number":9,"message_id":"m","enqueued_at":"2026-11-05T10:00:00Z","delivery_count":10,` +
		`"dead_letter_reason":"r","dead_letter_error_description":"d","dead_letter_source":"s","body":""}`

	outgoings, err := parseAll(t, line, nil)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	message := outgoings[0].Message
	if message.ApplicationProperties != nil || message.Subject != nil || message.ContentType != nil || message.TimeToLive != nil {
		t.Errorf("message = %+v, want only the message id carried over", message)
	}
}

func TestOutgoingsNewMessageID(t *testing.T) {
	counter := 0
	newID := func() string {
		counter++
		return "new-" + string(rune('0'+counter))
	}
	input := `{"schema":1,"message_id":"old-1","application_properties":{"PayloadTypeId":"T"},"body":""}` + "\n" +
		`{"schema":1,"message_id":"old-2","application_properties":{"asbctl-original-message-id":"first"},"body":""}` + "\n" +
		`{"schema":1,"body":""}` + "\n"

	outgoings, err := parseAll(t, input, newID)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	wantIDs := []string{"new-1", "new-2", "new-3"}
	for i, outgoing := range outgoings {
		if outgoing.MessageID != wantIDs[i] || *outgoing.Message.MessageID != wantIDs[i] {
			t.Errorf("message %d id = %q / %q, want %q", i, outgoing.MessageID, *outgoing.Message.MessageID, wantIDs[i])
		}
	}
	wantOriginals := []any{"old-1", "first", nil}
	for i, outgoing := range outgoings {
		if got := outgoing.Message.ApplicationProperties[OriginalMessageIDProperty]; got != wantOriginals[i] {
			t.Errorf("message %d original id property = %v, want %v", i, got, wantOriginals[i])
		}
	}
	if outgoings[0].Message.ApplicationProperties["PayloadTypeId"] != "T" || outgoings[0].PayloadTypeID != "T" {
		t.Errorf("payload type id was lost: %+v", outgoings[0].Message.ApplicationProperties)
	}
}

func TestOutgoingsPropertyTypes(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		check func(t *testing.T, got any)
	}{
		{name: "integer is int64", json: `7`, check: expect(int64(7))},
		{name: "big integer is int64", json: `9007199254740993`, check: expect(int64(9007199254740993))},
		{name: "negative integer", json: `-3`, check: expect(int64(-3))},
		{name: "fraction is float64", json: `1.5`, check: expect(1.5)},
		{name: "exponent is float64", json: `1e3`, check: expect(1000.0)},
		{name: "bool", json: `true`, check: expect(true)},
		{name: "string", json: `"x"`, check: expect("x")},
		{name: "timestamp", json: `{"type":"timestamp","value":"2026-11-05T09:59:00.5Z"}`, check: expect(time.Date(2026, 11, 5, 9, 59, 0, 500_000_000, time.UTC))},
		{name: "binary", json: `{"type":"binary","value":"/wA="}`, check: expect([]byte{0xff, 0x00})},
		{name: "symbol", json: `{"type":"symbol","value":"sym"}`, check: expect(amqp.Symbol("sym"))},
		{name: "double infinity", json: `{"type":"double","value":"+Inf"}`, check: func(t *testing.T, got any) {
			if f, ok := got.(float64); !ok || !math.IsInf(f, 1) {
				t.Errorf("got %#v, want +Inf float64", got)
			}
		}},
		{name: "float NaN", json: `{"type":"float","value":"NaN"}`, check: func(t *testing.T, got any) {
			if f, ok := got.(float32); !ok || f == f {
				t.Errorf("got %#v, want NaN float32", got)
			}
		}},
		{name: "float keeps its width", json: `{"type":"float","value":"1.5"}`, check: expect(float32(1.5))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			line := `{"schema":1,"application_properties":{"p":` + tc.json + `},"body":""}`

			outgoings, err := parseAll(t, line, nil)

			if err != nil {
				t.Fatalf("Outgoings() error = %v", err)
			}
			tc.check(t, outgoings[0].Message.ApplicationProperties["p"])
		})
	}
}

func expect(want any) func(*testing.T, any) {
	return func(t *testing.T, got any) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	}
}

func TestOutgoingsRestoredValuesAreEncodableAsAMQP(t *testing.T) {
	line := `{"schema":1,"application_properties":{"s":"x","i":7,"f":1.5,"b":true,` +
		`"t":{"type":"timestamp","value":"2026-11-05T09:59:00Z"},"bin":{"type":"binary","value":"/wA="},` +
		`"u":{"type":"uuid","value":"01020304-0506-0708-090a-0b0c0d0e0f10"},"y":{"type":"symbol","value":"s"},` +
		`"fl":{"type":"float","value":"1.5"},"db":{"type":"double","value":"NaN"}},"body":""}` + "\n" +
		`{"schema":1,"body_type":"value","body":"","body_text":"{\"a\":[1,{\"type\":\"binary\",\"value\":\"/wA=\"}]}"}` + "\n" +
		`{"schema":1,"body_type":"sequence","body":"","body_text":"[[\"a\",1],[true]]"}` + "\n"

	outgoings, err := parseAll(t, line, nil)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	encode := func(msg *amqp.Message) {
		t.Helper()
		if _, err := msg.MarshalBinary(); err != nil {
			t.Errorf("AMQP cannot encode %#v: %v", msg, err)
		}
	}
	encode(&amqp.Message{ApplicationProperties: outgoings[0].Message.ApplicationProperties, Data: [][]byte{nil}})
	encode(&amqp.Message{Value: outgoings[1].Annotated.Body.Value})
	encode(&amqp.Message{Sequence: outgoings[2].Annotated.Body.Sequence})
}

func TestOutgoingsBodyValues(t *testing.T) {
	tests := []struct {
		name         string
		record       string
		wantValue    any
		wantSequence [][]any
	}{
		{name: "string value", record: `{"schema":1,"body_type":"value","body":"","body_text":"\"hello\""}`, wantValue: "hello"},
		{name: "number value is int64", record: `{"schema":1,"body_type":"value","body":"","body_text":"5"}`, wantValue: int64(5)},
		{name: "typed member inside a value", record: `{"schema":1,"body_type":"value","body":"","body_text":"{\"k\":{\"type\":\"symbol\",\"value\":\"s\"}}"}`, wantValue: map[string]any{"k": amqp.Symbol("s")}},
		{name: "unknown typed shape stays a map", record: `{"schema":1,"body_type":"value","body":"","body_text":"{\"type\":\"custom\",\"value\":\"v\"}"}`, wantValue: map[string]any{"type": "custom", "value": "v"}},
		{name: "sequence", record: `{"schema":1,"body_type":"sequence","body":"","body_text":"[[\"a\",1],[]]"}`, wantSequence: [][]any{{"a", int64(1)}, {}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outgoings, err := parseAll(t, tc.record, nil)

			if err != nil {
				t.Fatalf("Outgoings() error = %v", err)
			}
			body := outgoings[0].Annotated.Body
			if !reflect.DeepEqual(body.Value, tc.wantValue) || !reflect.DeepEqual(body.Sequence, tc.wantSequence) {
				t.Errorf("body = value %#v, sequence %#v, want value %#v, sequence %#v", body.Value, body.Sequence, tc.wantValue, tc.wantSequence)
			}
		})
	}
}

func TestAnnotatedMessageCarriesTheRecordFields(t *testing.T) {
	record := `{"schema":1,"message_id":"m","content_type":"c","correlation_id":"corr","subject":"s","session_id":"sess",` +
		`"partition_key":"pk","to":"t","reply_to":"r","time_to_live_seconds":30,` +
		`"application_properties":{"PayloadTypeId":"T"},"body_type":"value","body":"","body_text":"1"}`

	outgoings, err := parseAll(t, record, nil)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	message := outgoings[0].Annotated
	properties := message.Properties
	if properties.MessageID != "m" || *properties.ContentType != "c" || properties.CorrelationID != "corr" || *properties.Subject != "s" ||
		*properties.GroupID != "sess" || *properties.To != "t" || *properties.ReplyTo != "r" {
		t.Errorf("Properties = %+v, want the record fields", properties)
	}
	if message.Header == nil || message.Header.TTL != 30*time.Second {
		t.Errorf("Header = %+v, want a 30s TTL", message.Header)
	}
	if message.MessageAnnotations["x-opt-partition-key"] != "pk" {
		t.Errorf("MessageAnnotations = %v, want the partition key", message.MessageAnnotations)
	}
	if message.ApplicationProperties["PayloadTypeId"] != "T" || outgoings[0].PayloadTypeID != "T" {
		t.Errorf("ApplicationProperties = %v, want PayloadTypeId T", message.ApplicationProperties)
	}
}

func TestOutgoingsNamesTheBadLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLine int
		wantText string
	}{
		{name: "not json", input: `{"schema":1,"body":""}` + "\n" + `nope` + "\n", wantLine: 2, wantText: "not a message record"},
		{name: "blank lines still count", input: "\n\n" + `{"schema":2,"body":""}`, wantLine: 3, wantText: "schema is 2"},
		{name: "missing schema", input: `{"body":""}`, wantLine: 1, wantText: "schema is 0"},
		{name: "two values on a line", input: `{"schema":1,"body":""} {"schema":1}`, wantLine: 1, wantText: "more than one"},
		{name: "body not base64", input: `{"schema":1,"body":"a b"}`, wantLine: 1, wantText: "base64"},
		{name: "unknown body type", input: `{"schema":1,"body_type":"stream","body":""}`, wantLine: 1, wantText: `unknown body_type "stream"`},
		{name: "value body without text", input: `{"schema":1,"body_type":"value","body":""}`, wantLine: 1, wantText: "needs body_text"},
		{name: "value body that is null", input: `{"schema":1,"body_type":"value","body":"","body_text":"null"}`, wantLine: 1, wantText: "null"},
		{name: "body text not json", input: `{"schema":1,"body_type":"value","body":"","body_text":"{"}`, wantLine: 1, wantText: "not valid JSON"},
		{name: "sequence that is not arrays", input: `{"schema":1,"body_type":"sequence","body":"","body_text":"[1]"}`, wantLine: 1, wantText: "array of arrays"},
		{name: "sequence that is not a list", input: `{"schema":1,"body_type":"sequence","body":"","body_text":"1"}`, wantLine: 1, wantText: "array of arrays"},
		{name: "property that is an untyped object", input: `{"schema":1,"application_properties":{"p":{"a":1}},"body":""}`, wantLine: 1, wantText: `property "p"`},
		{name: "property that is a list", input: `{"schema":1,"application_properties":{"p":[1]},"body":""}`, wantLine: 1, wantText: "list"},
		{name: "typed value with a bad timestamp", input: `{"schema":1,"application_properties":{"p":{"type":"timestamp","value":"yesterday"}},"body":""}`, wantLine: 1, wantText: "timestamp"},
		{name: "typed value with a bad uuid", input: `{"schema":1,"application_properties":{"p":{"type":"uuid","value":"abc"}},"body":""}`, wantLine: 1, wantText: "uuid"},
		{name: "typed value with a non-hex uuid", input: `{"schema":1,"application_properties":{"p":{"type":"uuid","value":"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"}},"body":""}`, wantLine: 1, wantText: "uuid"},
		{name: "integer too large", input: `{"schema":1,"application_properties":{"p":99999999999999999999},"body":""}`, wantLine: 1, wantText: "does not fit"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseAll(t, tc.input, nil)

			var lineErr *LineError
			if !errors.As(err, &lineErr) {
				t.Fatalf("Outgoings() error = %v, want a *LineError", err)
			}
			if lineErr.Line != tc.wantLine || !strings.Contains(lineErr.Error(), tc.wantText) {
				t.Errorf("error = %q, want line %d containing %q", lineErr, tc.wantLine, tc.wantText)
			}
		})
	}
}

func TestOutgoingsStopsAfterTheFirstBadLine(t *testing.T) {
	var yielded int
	for _, err := range Outgoings(strings.NewReader("bad\n"+`{"schema":1,"body":""}`+"\n"), nil) {
		yielded++
		if err == nil {
			t.Fatal("first yield has no error, want the bad line")
		}
	}

	if yielded != 1 {
		t.Errorf("yields = %d, want 1: the sequence ends at the first error", yielded)
	}
}

func TestOutgoingsReadsALineWithoutATrailingNewlineAndLongLines(t *testing.T) {
	long := strings.Repeat("A", 1<<20)
	input := `{"schema":1,"body":"` + long + `"}` + "\n" + `{"schema":1,"body":""}`

	outgoings, err := parseAll(t, input, nil)

	if err != nil || len(outgoings) != 2 || len(outgoings[0].Message.Body) != 1<<20*3/4 {
		t.Errorf("Outgoings() = %d messages, error %v, want 2 messages and a 786432-byte first body", len(outgoings), err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("disk gone") }

func TestOutgoingsReportsAReadError(t *testing.T) {
	var got error
	for _, err := range Outgoings(failingReader{}, nil) {
		got = err
	}

	if got == nil || !strings.Contains(got.Error(), "disk gone") {
		t.Errorf("error = %v, want the read error", got)
	}
}

func TestOutgoingsCanBeStoppedByTheConsumer(t *testing.T) {
	count := 0
	for range Outgoings(strings.NewReader(strings.Repeat(`{"schema":1,"body":""}`+"\n", 5)), nil) {
		count++
		break
	}

	if count != 1 {
		t.Errorf("iterations = %d, want 1", count)
	}
}

func TestOutgoingsKeepsANullProperty(t *testing.T) {
	outgoings, err := parseAll(t, `{"schema":1,"application_properties":{"p":null},"body":""}`, nil)

	if err != nil {
		t.Fatalf("Outgoings() error = %v", err)
	}
	if value, present := outgoings[0].Message.ApplicationProperties["p"]; !present || value != nil {
		t.Errorf("property p = %v, present %v, want a nil value that is present", value, present)
	}
}
