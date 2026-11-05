package export

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const bodyColumnRunes = 60

// Format is the value of the --output flag.
type Format string

// The formats every command accepts.
const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
)

// ParseFormat checks an --output value.
func ParseFormat(value string) (Format, error) {
	switch Format(value) {
	case FormatTable, FormatJSON:
		return Format(value), nil
	default:
		return "", fmt.Errorf("unknown output format %q, want table or json", value)
	}
}

// Options configures a message writer.
type Options struct {
	Format Format
	// DeadLetter adds the DLQ_REASON column to the table.
	DeadLetter bool
	// PayloadTypeID, when set, drops messages whose PayloadTypeId property differs.
	PayloadTypeID string
}

// Writer writes each page of messages as it arrives.
type Writer struct {
	out         io.Writer
	options     Options
	wroteHeader bool
}

// NewWriter returns a writer for out.
func NewWriter(out io.Writer, options Options) *Writer {
	return &Writer{out: out, options: options}
}

// WriteMessages converts a page, drops the messages the payload type filter rejects and writes the rest.
// It returns how many messages it wrote.
func (w *Writer) WriteMessages(messages []*azservicebus.ReceivedMessage) (int, error) {
	records := make([]record, 0, len(messages))
	for _, message := range messages {
		rec := recordFromReceived(message)
		if matchesPayloadType(rec, w.options.PayloadTypeID) {
			records = append(records, rec)
		}
	}

	if w.options.Format == FormatJSON {
		return len(records), writeNDJSON(w.out, records)
	}
	return len(records), w.writeTable(records)
}

func matchesPayloadType(rec record, payloadTypeID string) bool {
	return payloadTypeID == "" || rec.payloadTypeID() == payloadTypeID
}

func writeNDJSON(out io.Writer, records []record) error {
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	for _, rec := range records {
		if err := encoder.Encode(rec); err != nil {
			return fmt.Errorf("write json record: %w", err)
		}
	}
	return nil
}

// writeTable aligns each page on its own, so that a page reaches the terminal before the next one is peeked.
func (w *Writer) writeTable(records []record) error {
	if len(records) == 0 {
		return nil
	}

	table := tabwriter.NewWriter(w.out, 0, 8, 2, ' ', 0)
	if !w.wroteHeader {
		fmt.Fprintln(table, strings.Join(tableHeader(w.options.DeadLetter), "\t"))
		w.wroteHeader = true
	}
	for _, rec := range records {
		fmt.Fprintln(table, strings.Join(tableRow(rec, w.options.DeadLetter), "\t"))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write table: %w", err)
	}
	return nil
}

func tableHeader(isDeadLetter bool) []string {
	columns := []string{"SEQ", "ENQUEUED", "MESSAGE_ID", "PAYLOAD_TYPE", "SUBJECT"}
	if isDeadLetter {
		columns = append(columns, "DLQ_REASON")
	}
	return append(columns, "BODY")
}

func tableRow(rec record, isDeadLetter bool) []string {
	columns := []string{
		fmt.Sprint(rec.SequenceNumber),
		rec.EnqueuedAt,
		cell(rec.MessageID),
		cell(rec.payloadTypeID()),
		cell(rec.Subject),
	}
	if isDeadLetter {
		columns = append(columns, cell(rec.DeadLetterReason))
	}
	return append(columns, bodyCell(rec))
}

func bodyCell(rec record) string {
	if rec.BodyText == nil {
		return fmt.Sprintf("<%d bytes binary>", rec.bodyBytes)
	}
	return truncateRunes(cell(*rec.BodyText), bodyColumnRunes)
}

// cell keeps a value on one table line and inside its own column.
func cell(value string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(value)
}
