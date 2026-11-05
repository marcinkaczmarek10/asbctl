package export

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// Send result statuses.
const (
	StatusSent   = "sent"
	StatusFailed = "failed"
)

// PayloadTypeCount is one row of a plan.
type PayloadTypeCount struct {
	PayloadTypeID string `json:"payload_type_id"`
	Count         int    `json:"count"`
}

// SendTarget names the queue or topic that a send goes to.
type SendTarget struct {
	Queue string `json:"queue,omitempty"`
	Topic string `json:"topic,omitempty"`
}

// SendPlan is what send would do. It is printed instead of sending when --yes is missing or with --dry-run.
type SendPlan struct {
	Schema       int                `json:"schema"`
	Target       SendTarget         `json:"target"`
	Total        int                `json:"total"`
	PayloadTypes []PayloadTypeCount `json:"payload_types"`
	DryRun       bool               `json:"dry_run"`
}

// NewSendPlan builds a plan from a count per payload type. Rows are sorted by count, largest first.
func NewSendPlan(target SendTarget, countsByPayloadType map[string]int, dryRun bool) SendPlan {
	plan := SendPlan{Schema: Schema, Target: target, PayloadTypes: make([]PayloadTypeCount, 0, len(countsByPayloadType)), DryRun: dryRun}
	for payloadTypeID, count := range countsByPayloadType {
		plan.Total += count
		plan.PayloadTypes = append(plan.PayloadTypes, PayloadTypeCount{PayloadTypeID: payloadTypeID, Count: count})
	}
	slices.SortFunc(plan.PayloadTypes, func(a, b PayloadTypeCount) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.PayloadTypeID, b.PayloadTypeID))
	})
	return plan
}

// WriteSendPlan writes the plan as one JSON object, or as a short block and a table.
func WriteSendPlan(out io.Writer, format Format, plan SendPlan) error {
	if format == FormatJSON {
		return encodeLine(out, plan, "send plan")
	}

	target := "queue " + plan.Target.Queue
	if plan.Target.Queue == "" {
		target = "topic " + plan.Target.Topic
	}
	fmt.Fprintf(out, "target:    %s\nmessages:  %d\n", target, plan.Total)
	if len(plan.PayloadTypes) > 0 {
		fmt.Fprintln(out)
		table := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(table, "COUNT\tPAYLOAD_TYPE")
		for _, row := range plan.PayloadTypes {
			fmt.Fprintf(table, "%d\t%s\n", row.Count, noneIfEmpty(cell(row.PayloadTypeID)))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write send plan: %w", err)
		}
	}

	if plan.DryRun {
		fmt.Fprintln(out, "\nDry run: nothing was sent.")
	} else {
		fmt.Fprintln(out, "\nNothing was sent. Pass --yes to send.")
	}
	return nil
}

type sendResultLine struct {
	Schema    int    `json:"schema"`
	Line      int    `json:"line"`
	MessageID string `json:"message_id,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// WriteSendResult writes the outcome of one message: an NDJSON line in json mode. The table shows only failures.
func WriteSendResult(out io.Writer, format Format, result bus.SendResult) error {
	if format != FormatJSON {
		if result.Err != nil {
			fmt.Fprintf(out, "line %d %s: failed: %v\n", result.Line, cell(result.MessageID), result.Err)
		}
		return nil
	}

	line := sendResultLine{Schema: Schema, Line: result.Line, MessageID: result.MessageID, Status: StatusSent}
	if result.Err != nil {
		line.Status = StatusFailed
		line.Error = result.Err.Error()
	}
	return encodeLine(out, line, "send result")
}

// WriteSendSummary writes the closing line of a send in table mode. Json mode has none: its exit code says it.
func WriteSendSummary(out io.Writer, format Format, counts bus.SendCounts, total int) {
	if format == FormatJSON {
		return
	}
	fmt.Fprintf(out, "Sent %d of %d messages, %d failed.\n", counts.Sent, total, counts.Failed)
}
