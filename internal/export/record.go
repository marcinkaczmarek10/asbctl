// Package export turns received Service Bus messages into the asbctl record and writes it as a table or NDJSON.
package export

import (
	"encoding/base64"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

// Schema is the record schema version written in every JSON object.
const Schema = 1

// PayloadTypeIDProperty is the application property that names the handler a message is routed to.
const PayloadTypeIDProperty = "PayloadTypeId"

// record is the message shape documented in docs/output-format.md.
type record struct {
	Schema                     int            `json:"schema"`
	SequenceNumber             int64          `json:"sequence_number,omitempty"`
	MessageID                  string         `json:"message_id,omitempty"`
	EnqueuedAt                 string         `json:"enqueued_at,omitempty"`
	ContentType                string         `json:"content_type,omitempty"`
	CorrelationID              string         `json:"correlation_id,omitempty"`
	Subject                    string         `json:"subject,omitempty"`
	SessionID                  string         `json:"session_id,omitempty"`
	PartitionKey               string         `json:"partition_key,omitempty"`
	To                         string         `json:"to,omitempty"`
	ReplyTo                    string         `json:"reply_to,omitempty"`
	TimeToLiveSeconds          int64          `json:"time_to_live_seconds,omitempty"`
	DeliveryCount              uint32         `json:"delivery_count,omitempty"`
	DeadLetterReason           string         `json:"dead_letter_reason,omitempty"`
	DeadLetterErrorDescription string         `json:"dead_letter_error_description,omitempty"`
	DeadLetterSource           string         `json:"dead_letter_source,omitempty"`
	ApplicationProperties      map[string]any `json:"application_properties,omitempty"`
	Body                       string         `json:"body"`
	BodyText                   *string        `json:"body_text,omitempty"`

	// bodyBytes sizes the BODY table cell of a binary body without decoding base64 again.
	bodyBytes int
}

// typedValue carries an AMQP type that JSON cannot express natively.
type typedValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func recordFromReceived(message *azservicebus.ReceivedMessage) record {
	rec := record{
		Schema:                     Schema,
		MessageID:                  message.MessageID,
		ContentType:                valueOf(message.ContentType),
		CorrelationID:              valueOf(message.CorrelationID),
		Subject:                    valueOf(message.Subject),
		SessionID:                  valueOf(message.SessionID),
		PartitionKey:               valueOf(message.PartitionKey),
		To:                         valueOf(message.To),
		ReplyTo:                    valueOf(message.ReplyTo),
		DeliveryCount:              message.DeliveryCount,
		DeadLetterReason:           valueOf(message.DeadLetterReason),
		DeadLetterErrorDescription: valueOf(message.DeadLetterErrorDescription),
		DeadLetterSource:           valueOf(message.DeadLetterSource),
		ApplicationProperties:      propertiesFromAMQP(message.ApplicationProperties),
		Body:                       base64.StdEncoding.EncodeToString(message.Body),
		bodyBytes:                  len(message.Body),
	}
	if message.SequenceNumber != nil {
		rec.SequenceNumber = *message.SequenceNumber
	}
	if message.EnqueuedTime != nil {
		rec.EnqueuedAt = message.EnqueuedTime.UTC().Format(time.RFC3339Nano)
	}
	if message.TimeToLive != nil {
		rec.TimeToLiveSeconds = int64(message.TimeToLive.Seconds())
	}
	if utf8.Valid(message.Body) {
		text := string(message.Body)
		rec.BodyText = &text
	}
	return rec
}

// payloadTypeID returns the PayloadTypeId property, or an empty string when it is absent or not a string.
func (r record) payloadTypeID() string {
	id, _ := r.ApplicationProperties[PayloadTypeIDProperty].(string)
	return id
}

func propertiesFromAMQP(properties map[string]any) map[string]any {
	if len(properties) == 0 {
		return nil
	}
	converted := make(map[string]any, len(properties))
	for key, value := range properties {
		converted[key] = propertyFromAMQP(value)
	}
	return converted
}

func propertyFromAMQP(value any) any {
	switch v := value.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v
	case float32:
		return floatProperty("float", float64(v), v)
	case float64:
		return floatProperty("double", v, v)
	case time.Time:
		return typedValue{Type: "timestamp", Value: v.UTC().Format(time.RFC3339Nano)}
	case []byte:
		return typedValue{Type: "binary", Value: base64.StdEncoding.EncodeToString(v)}
	case amqp.UUID:
		return typedValue{Type: "uuid", Value: v.String()}
	case amqp.Symbol:
		return typedValue{Type: "symbol", Value: string(v)}
	case time.Duration:
		return typedValue{Type: "duration", Value: v.String()}
	default:
		return typedValue{Type: fmt.Sprintf("%T", v), Value: fmt.Sprint(v)}
	}
}

// floatProperty keeps finite floats as JSON numbers, because NaN and Inf cannot be encoded as JSON.
func floatProperty(typeName string, asDouble float64, original any) any {
	if math.IsNaN(asDouble) || math.IsInf(asDouble, 0) {
		return typedValue{Type: typeName, Value: fmt.Sprint(original)}
	}
	return original
}

func valueOf(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
