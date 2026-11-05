package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// MessagesOptions limits an export. PayloadTypeID, when set, drops other payload types from the output only.
type MessagesOptions struct {
	Max           int
	PayloadTypeID string
}

// MessagesResult says what an export wrote. Partial is true when the peek reached Max, so the entity may hold more.
type MessagesResult struct {
	Peeked   int
	Exported int
	Partial  bool
}

// Messages peeks up to options.Max messages and writes each as a record on its own NDJSON line to w. It is
// the building block of export, and of the snapshots that resend and purge write. onProgress is called after
// every page.
func Messages(
	ctx context.Context,
	peeker bus.Peeker,
	options MessagesOptions,
	w io.Writer,
	onProgress func(peeked, exported int),
) (MessagesResult, error) {
	writer := NewWriter(w, Options{Format: FormatJSON, PayloadTypeID: options.PayloadTypeID})

	exported := 0
	peeked := 0
	_, err := bus.Peek(ctx, peeker, bus.PeekOptions{Max: options.Max}, func(page []*azservicebus.ReceivedMessage) error {
		written, err := writer.WriteMessages(page)
		peeked += len(page)
		exported += written
		onProgress(peeked, exported)
		return err
	})
	return MessagesResult{Peeked: peeked, Exported: exported, Partial: peeked >= options.Max}, err
}

// ExportResult is what export prints on stdout when the records went to a file.
type ExportResult struct {
	Schema   int    `json:"schema"`
	Exported int    `json:"exported"`
	File     string `json:"file"`
	Partial  bool   `json:"partial"`
}

// WriteExportResult writes the result as one JSON object, or as one sentence.
func WriteExportResult(out io.Writer, format Format, result ExportResult) error {
	if format == FormatJSON {
		return encodeLine(out, result, "export result")
	}
	fmt.Fprintf(out, "Exported %d messages to %s.", result.Exported, result.File)
	if result.Partial {
		fmt.Fprint(out, " The entity may hold more than --max.")
	}
	fmt.Fprintln(out)
	return nil
}

func encodeLine(out io.Writer, value any, what string) error {
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write %s: %w", what, err)
	}
	return nil
}
