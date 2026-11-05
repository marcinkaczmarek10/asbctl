package export

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// OriginalMessageIDProperty holds the MessageId that a message had before --new-message-id replaced it.
const OriginalMessageIDProperty = "asbctl-original-message-id"

// LineError names the input line that could not be turned into a message.
type LineError struct {
	Line int
	Err  error
}

func (e *LineError) Error() string { return fmt.Sprintf("line %d: %v", e.Line, e.Err) }

func (e *LineError) Unwrap() error { return e.Err }

// Outgoings reads NDJSON records, the format that export writes, and yields each as an outgoing message.
// Blank lines are skipped. A line that does not parse yields a *LineError and ends the sequence. A newMessageID
// that is not nil replaces every MessageId and keeps the first one in OriginalMessageIDProperty.
func Outgoings(input io.Reader, newMessageID func() string) iter.Seq2[bus.Outgoing, error] {
	return func(yield func(bus.Outgoing, error) bool) {
		reader := bufio.NewReaderSize(input, 64*1024)
		for lineNumber := 1; ; lineNumber++ {
			line, readErr := reader.ReadBytes('\n')
			if len(bytes.TrimSpace(line)) > 0 {
				outgoing, err := outgoingFromLine(lineNumber, line, newMessageID)
				if err != nil {
					yield(bus.Outgoing{}, &LineError{Line: lineNumber, Err: err})
					return
				}
				if !yield(outgoing, nil) {
					return
				}
			}
			if errors.Is(readErr, io.EOF) {
				return
			}
			if readErr != nil {
				yield(bus.Outgoing{}, fmt.Errorf("read input: %w", readErr))
				return
			}
		}
	}
}

func outgoingFromLine(lineNumber int, line []byte, newMessageID func() string) (bus.Outgoing, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber() // keeps 3 as an integer and 3.5 as a float, which float64 decoding would blur.

	var rec record
	if err := decoder.Decode(&rec); err != nil {
		return bus.Outgoing{}, fmt.Errorf("not a message record: %w", err)
	}
	if decoder.More() {
		return bus.Outgoing{}, errors.New("more than one JSON value on the line")
	}
	if rec.Schema != Schema {
		return bus.Outgoing{}, fmt.Errorf("schema is %d, want %d", rec.Schema, Schema)
	}

	properties, err := propertiesForSend(rec.ApplicationProperties)
	if err != nil {
		return bus.Outgoing{}, err
	}
	if newMessageID != nil {
		properties = withOriginalMessageID(properties, rec.MessageID)
		rec.MessageID = newMessageID()
	}

	outgoing := bus.Outgoing{Line: lineNumber, MessageID: rec.MessageID}
	outgoing.PayloadTypeID, _ = properties[PayloadTypeIDProperty].(string)

	switch rec.BodyType {
	case "", "data":
		body, err := base64.StdEncoding.DecodeString(rec.Body)
		if err != nil {
			return bus.Outgoing{}, fmt.Errorf("body is not base64: %w", err)
		}
		outgoing.Message = messageForSend(rec, properties, body)
	case "value", "sequence":
		body, err := annotatedBody(rec)
		if err != nil {
			return bus.Outgoing{}, err
		}
		outgoing.Annotated = annotatedForSend(rec, properties, body)
	default:
		return bus.Outgoing{}, fmt.Errorf("unknown body_type %q", rec.BodyType)
	}
	return outgoing, nil
}

func withOriginalMessageID(properties map[string]any, originalID string) map[string]any {
	if _, kept := properties[OriginalMessageIDProperty]; kept || originalID == "" {
		return properties
	}
	updated := make(map[string]any, len(properties)+1)
	for name, value := range properties {
		updated[name] = value
	}
	updated[OriginalMessageIDProperty] = originalID
	return updated
}

func messageForSend(rec record, properties map[string]any, body []byte) *azservicebus.Message {
	message := &azservicebus.Message{
		Body:                  body,
		ApplicationProperties: properties,
		ContentType:           stringIfSet(rec.ContentType),
		CorrelationID:         stringIfSet(rec.CorrelationID),
		MessageID:             stringIfSet(rec.MessageID),
		PartitionKey:          stringIfSet(rec.PartitionKey),
		ReplyTo:               stringIfSet(rec.ReplyTo),
		SessionID:             stringIfSet(rec.SessionID),
		Subject:               stringIfSet(rec.Subject),
		To:                    stringIfSet(rec.To),
	}
	if rec.TimeToLiveSeconds > 0 {
		ttl := time.Duration(rec.TimeToLiveSeconds) * time.Second
		message.TimeToLive = &ttl
	}
	return message
}

// partitionKeyAnnotation is where Service Bus keeps the partition key of an AMQP message.
const partitionKeyAnnotation = "x-opt-partition-key"

func annotatedForSend(rec record, properties map[string]any, body azservicebus.AMQPAnnotatedMessageBody) *azservicebus.AMQPAnnotatedMessage {
	message := &azservicebus.AMQPAnnotatedMessage{
		ApplicationProperties: properties,
		Body:                  body,
		Properties: &azservicebus.AMQPAnnotatedMessageProperties{
			ContentType: stringIfSet(rec.ContentType),
			GroupID:     stringIfSet(rec.SessionID),
			ReplyTo:     stringIfSet(rec.ReplyTo),
			Subject:     stringIfSet(rec.Subject),
			To:          stringIfSet(rec.To),
		},
	}
	if rec.MessageID != "" {
		message.Properties.MessageID = rec.MessageID
	}
	if rec.CorrelationID != "" {
		message.Properties.CorrelationID = rec.CorrelationID
	}
	if rec.TimeToLiveSeconds > 0 {
		message.Header = &azservicebus.AMQPAnnotatedMessageHeader{TTL: time.Duration(rec.TimeToLiveSeconds) * time.Second}
	}
	if rec.PartitionKey != "" {
		message.MessageAnnotations = map[any]any{partitionKeyAnnotation: rec.PartitionKey}
	}
	return message
}

func stringIfSet(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// annotatedBody restores an AMQP value or sequence body from the JSON text that export wrote in body_text.
func annotatedBody(rec record) (azservicebus.AMQPAnnotatedMessageBody, error) {
	if rec.BodyText == nil {
		return azservicebus.AMQPAnnotatedMessageBody{}, fmt.Errorf("body_type %q needs body_text", rec.BodyType)
	}
	decoded, err := decodeJSONValue(*rec.BodyText)
	if err != nil {
		return azservicebus.AMQPAnnotatedMessageBody{}, fmt.Errorf("body_text is not valid JSON: %w", err)
	}

	value, err := valueFromJSON(decoded, false)
	if err != nil {
		return azservicebus.AMQPAnnotatedMessageBody{}, fmt.Errorf("body_text: %w", err)
	}
	if rec.BodyType == "value" {
		if value == nil {
			return azservicebus.AMQPAnnotatedMessageBody{}, errors.New("body_text is null, a value body needs a value")
		}
		return azservicebus.AMQPAnnotatedMessageBody{Value: value}, nil
	}

	sections, ok := value.([]any)
	if !ok {
		return azservicebus.AMQPAnnotatedMessageBody{}, errors.New("a sequence body_text must be a JSON array of arrays")
	}
	sequence := make([][]any, len(sections))
	for i, section := range sections {
		items, ok := section.([]any)
		if !ok {
			return azservicebus.AMQPAnnotatedMessageBody{}, errors.New("a sequence body_text must be a JSON array of arrays")
		}
		sequence[i] = items
	}
	return azservicebus.AMQPAnnotatedMessageBody{Sequence: sequence}, nil
}

func decodeJSONValue(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("more than one JSON value")
	}
	return value, nil
}

func propertiesForSend(properties map[string]any) (map[string]any, error) {
	if len(properties) == 0 {
		return nil, nil
	}
	restored := make(map[string]any, len(properties))
	for name, raw := range properties {
		value, err := valueFromJSON(raw, true)
		if err != nil {
			return nil, fmt.Errorf("application property %q: %w", name, err)
		}
		restored[name] = value
	}
	return restored, nil
}

// valueFromJSON turns a decoded JSON value into the AMQP value it stood for. A typed object becomes its AMQP
// type. Any other object is an error for a property, because a property cannot hold a map, and a map for a body.
func valueFromJSON(raw any, isProperty bool) (any, error) {
	switch v := raw.(type) {
	case nil, string, bool:
		return v, nil
	case json.Number:
		return numberFromJSON(v)
	case []any:
		if isProperty {
			return nil, errors.New("a list is not a valid property value")
		}
		items := make([]any, len(v))
		for i, item := range v {
			restored, err := valueFromJSON(item, false)
			if err != nil {
				return nil, err
			}
			items[i] = restored
		}
		return items, nil
	case map[string]any:
		typed, isTyped, err := typedFromObject(v)
		if err != nil {
			return nil, err
		}
		if isTyped {
			return typed, nil
		}
		if isProperty {
			return nil, errors.New("an object that is not a typed value is not a valid property value")
		}
		entries := make(map[string]any, len(v))
		for key, item := range v {
			restored, err := valueFromJSON(item, false)
			if err != nil {
				return nil, err
			}
			entries[key] = restored
		}
		return entries, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value of type %T", raw)
	}
}

// numberFromJSON reads integers as int64 and everything else as float64. The AMQP width is not kept.
func numberFromJSON(number json.Number) (any, error) {
	if integer, err := number.Int64(); err == nil {
		return integer, nil
	}
	if strings.ContainsAny(number.String(), ".eE") {
		return number.Float64()
	}
	return nil, fmt.Errorf("integer %s does not fit in int64", number)
}

// typedFromObject restores {"type": ..., "value": ...}. It reports false for an object of another shape, or of a
// type it does not know, and an error for a known type whose value is not valid.
func typedFromObject(object map[string]any) (value any, isTyped bool, err error) {
	typeName, typeOK := object["type"].(string)
	text, valueOK := object["value"].(string)
	if len(object) != 2 || !typeOK || !valueOK {
		return nil, false, nil
	}

	switch typeName {
	case "timestamp":
		value, err = time.Parse(time.RFC3339Nano, text)
	case "binary":
		value, err = base64.StdEncoding.DecodeString(text)
	case "uuid":
		value, err = uuidFromText(text)
	case "symbol":
		value = amqp.Symbol(text)
	case "float":
		var f float64
		f, err = strconv.ParseFloat(text, 32)
		value = float32(f)
	case "double":
		value, err = strconv.ParseFloat(text, 64)
	default:
		return nil, false, nil
	}
	if err != nil {
		return nil, true, fmt.Errorf("typed value %q: %w", typeName, err)
	}
	return value, true, nil
}

func uuidFromText(text string) (amqp.UUID, error) {
	var id amqp.UUID
	compact := strings.ReplaceAll(text, "-", "")
	if len(compact) != 2*len(id) {
		return id, fmt.Errorf("%q is not a uuid", text)
	}
	if _, err := hex.Decode(id[:], []byte(compact)); err != nil {
		return id, fmt.Errorf("%q is not a uuid: %w", text, err)
	}
	return id, nil
}
