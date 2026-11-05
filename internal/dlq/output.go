package dlq

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// Location names a queue or a subscription.
type Location struct {
	Queue        string `json:"queue,omitempty"`
	Topic        string `json:"topic,omitempty"`
	Subscription string `json:"subscription,omitempty"`
}

// LocationFromTarget describes the source of a command.
func LocationFromTarget(target bus.Target) Location {
	return Location{Queue: target.Queue, Topic: target.Topic, Subscription: target.Subscription}
}

func (l Location) String() string {
	switch {
	case l.Queue != "":
		return "queue " + l.Queue
	case l.Subscription != "":
		return fmt.Sprintf("topic %s, subscription %s", l.Topic, l.Subscription)
	default:
		return "topic " + l.Topic
	}
}

// DedupePlan says how the duplicate detection guard will treat the selection.
type DedupePlan struct {
	Enabled             bool  `json:"enabled"`
	WindowSeconds       int64 `json:"window_seconds,omitempty"`
	Assumed             bool  `json:"assumed"`
	RepeatedInSelection int   `json:"repeated_in_selection"`
	InLedger            int   `json:"in_ledger"`
}

// Plan is what a command would do. It is printed instead of acting when --yes is missing, or with --dry-run.
type Plan struct {
	Schema   int         `json:"schema"`
	Type     string      `json:"type"`
	Command  string      `json:"command"`
	Source   Location    `json:"source"`
	Target   *Location   `json:"target,omitempty"`
	Selected int         `json:"selected"`
	Groups   []Group     `json:"groups"`
	Snapshot string      `json:"snapshot,omitempty"`
	Dedupe   *DedupePlan `json:"dedupe,omitempty"`
	Warnings []string    `json:"warnings"`
	DryRun   bool        `json:"dry_run"`
}

// ResendPlanInput is what NewResendPlan needs.
type ResendPlanInput struct {
	Source    bus.Target
	Target    Location
	Selection Selection
	Snapshot  string
	Params    ResendParams
	DryRun    bool
}

// NewResendPlan describes a resend, including the warnings that the target and the settings call for.
func NewResendPlan(in ResendPlanInput) Plan {
	source := LocationFromTarget(in.Source)
	dedupe := dedupePlan(in.Params, in.Selection)
	plan := Plan{
		Schema: export.Schema, Type: "plan", Command: "dlq resend", Source: source, Target: &in.Target,
		Selected: len(in.Selection.Entries), Groups: in.Selection.Groups(), Snapshot: in.Snapshot, Dedupe: &dedupe, DryRun: in.DryRun,
		Warnings: []string{},
	}

	if in.Target.Topic != "" {
		plan.Warnings = append(plan.Warnings, "a topic send reaches every matching subscription, not only the one the messages came from")
	}
	if in.Params.Dedupe.Assumed {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"the duplicate detection settings of the target are not readable, so it is assumed on with a window of %s", in.Params.Dedupe.Window))
	}
	if in.Params.Dedupe.Enabled && in.Params.NewMessageID != nil {
		plan.Warnings = append(plan.Warnings, "with --new-message-id a consumer that deduplicates on MessageId will process each message twice")
	}
	if in.Target != source {
		plan.Warnings = append(plan.Warnings, "bounce detection reads the source dead-letter queue only, so it cannot see a message that is dead-lettered again on the target")
	}
	return plan
}

// NewPurgePlan describes a purge.
func NewPurgePlan(source bus.Target, selection Selection, snapshot string, dryRun bool) Plan {
	return Plan{
		Schema: export.Schema, Type: "plan", Command: "dlq purge", Source: LocationFromTarget(source),
		Selected: len(selection.Entries), Groups: selection.Groups(), Snapshot: snapshot, DryRun: dryRun, Warnings: []string{},
	}
}

func dedupePlan(params ResendParams, selection Selection) DedupePlan {
	plan := DedupePlan{Enabled: params.Dedupe.Enabled, WindowSeconds: int64(params.Dedupe.Window / time.Second), Assumed: params.Dedupe.Assumed}
	if !params.Dedupe.Enabled || params.NewMessageID != nil {
		return plan
	}
	duplicates := selection.DuplicateIDs()
	for _, entry := range selection.Entries {
		if entry.MessageID == "" {
			continue
		}
		if _, repeated := duplicates[entry.MessageID]; repeated {
			plan.RepeatedInSelection++
		} else if _, resent := params.Recent[entry.MessageID]; resent {
			plan.InLedger++
		}
	}
	return plan
}

// WritePlan writes the plan as one JSON object, or as a block, a table and the warnings.
func WritePlan(out io.Writer, format export.Format, plan Plan) error {
	if format == export.FormatJSON {
		return encodeLine(out, plan, "plan")
	}

	fmt.Fprintf(out, "command:   %s\nsource:    %s (dead-letter queue)\n", plan.Command, plan.Source)
	if plan.Target != nil {
		fmt.Fprintf(out, "target:    %s\n", plan.Target)
	}
	fmt.Fprintf(out, "selected:  %d\n", plan.Selected)
	switch {
	case plan.Snapshot != "" && !plan.DryRun:
		fmt.Fprintf(out, "snapshot:  %s (written when the command runs)\n", plan.Snapshot)
	case plan.Snapshot != "":
		fmt.Fprintf(out, "snapshot:  %s (not written in a dry run)\n", plan.Snapshot)
	}
	if plan.Dedupe != nil {
		fmt.Fprintf(out, "duplicate detection:  %s\n", dedupeText(*plan.Dedupe))
	}

	if len(plan.Groups) > 0 {
		fmt.Fprintln(out)
		table := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(table, "COUNT\tPAYLOAD_TYPE\tREASON")
		for _, group := range plan.Groups {
			fmt.Fprintf(table, "%d\t%s\t%s\n", group.Count, orNone(group.PayloadTypeID), orNone(group.DeadLetterReason))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write plan: %w", err)
		}
	}

	for _, warning := range plan.Warnings {
		fmt.Fprintf(out, "\nWarning: %s.", warning)
	}
	if len(plan.Warnings) > 0 {
		fmt.Fprintln(out)
	}
	if plan.DryRun {
		fmt.Fprintln(out, "\nDry run: nothing was changed.")
	} else {
		fmt.Fprintln(out, "\nNothing was changed. Pass --yes to run it.")
	}
	return nil
}

func dedupeText(plan DedupePlan) string {
	if !plan.Enabled {
		return "off"
	}
	text := fmt.Sprintf("on, window %s", time.Duration(plan.WindowSeconds)*time.Second)
	if plan.Assumed {
		text += " (assumed)"
	}
	if plan.RepeatedInSelection+plan.InLedger > 0 {
		text += fmt.Sprintf(": %d repeat a MessageId in the selection and %d were resent inside the window, and will be skipped",
			plan.RepeatedInSelection, plan.InLedger)
	}
	return text
}

func orNone(value string) string {
	if value == "" {
		return "(none)"
	}
	return value
}

type resultLine struct {
	Schema         int    `json:"schema"`
	SequenceNumber int64  `json:"sequence_number"`
	MessageID      string `json:"message_id,omitempty"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

// WriteResult writes the outcome of one message. The table shows only the messages that did not go well.
func WriteResult(out io.Writer, format export.Format, result Result) error {
	if format != export.FormatJSON {
		if result.Status != StatusResent && result.Status != StatusPurged {
			fmt.Fprintf(out, "sequence %d %s: %s", result.SequenceNumber, result.MessageID, result.Status)
			if result.Err != nil {
				fmt.Fprintf(out, ": %v", result.Err)
			}
			fmt.Fprintln(out)
		}
		return nil
	}

	line := resultLine{Schema: export.Schema, SequenceNumber: result.SequenceNumber, MessageID: result.MessageID, Status: result.Status}
	if result.Err != nil {
		line.Error = result.Err.Error()
	}
	return encodeLine(out, line, "result")
}

// WriteResendSummary writes the closing object of a resend, or its closing sentences in table mode.
func WriteResendSummary(out io.Writer, format export.Format, summary ResendSummary) error {
	if format == export.FormatJSON {
		return encodeLine(out, summary, "summary")
	}
	fmt.Fprintf(out, "Resent %d, failed %d, sent but not completed %d, skipped %d, bounced %d.\n",
		summary.Resent, summary.Failed, summary.SentNotCompleted, summary.Skipped, summary.Bounced)
	if summary.StoppedOnBounce {
		fmt.Fprintln(out, "Stopped before the next wave because too many resent messages came back to the dead-letter queue.")
	}
	if summary.NotReached > 0 {
		fmt.Fprintf(out, "%d selected messages were not reached.\n", summary.NotReached)
	}
	if summary.SentNotCompleted > 0 {
		fmt.Fprintln(out, "Messages that were sent but not completed are still in the dead-letter queue and were sent already.")
	}
	fmt.Fprintf(out, "Snapshot: %s\n", summary.Snapshot)
	return nil
}

// WritePurgeSummary writes the closing object of a purge, or its closing sentences in table mode.
func WritePurgeSummary(out io.Writer, format export.Format, summary PurgeSummary) error {
	if format == export.FormatJSON {
		return encodeLine(out, summary, "summary")
	}
	fmt.Fprintf(out, "Purged %d, failed %d.\n", summary.Purged, summary.Failed)
	if summary.NotReached > 0 {
		fmt.Fprintf(out, "%d selected messages were not reached.\n", summary.NotReached)
	}
	fmt.Fprintf(out, "Snapshot: %s\n", summary.Snapshot)
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
