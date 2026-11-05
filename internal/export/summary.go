package export

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

const (
	descriptionColumnRunes = 120
	noneLabel              = "(none)"
)

// Group is one (payload type, reason) row of the dead-letter summary.
type Group struct {
	PayloadTypeID       string `json:"payload_type_id"`
	DeadLetterReason    string `json:"dead_letter_reason"`
	Count               int    `json:"count"`
	FirstEnqueuedAt     string `json:"first_enqueued_at,omitempty"`
	LastEnqueuedAt      string `json:"last_enqueued_at,omitempty"`
	MaxDeliveryCount    uint32 `json:"max_delivery_count"`
	TopErrorDescription string `json:"top_dead_letter_error_description,omitempty"`
	SampleMessageID     string `json:"sample_message_id,omitempty"`
}

// Summary is the result of dlq summary.
type Summary struct {
	Schema  int             `json:"schema"`
	Entity  bus.EntityFacts `json:"entity"`
	Peeked  int             `json:"peeked"`
	Partial bool            `json:"partial"`
	Groups  []Group         `json:"groups"`
}

type groupKey struct {
	payloadTypeID string
	reason        string
}

type groupStats struct {
	count        int
	firstAt      time.Time
	lastAt       time.Time
	maxDelivery  uint32
	descriptions map[string]int
	sampleID     string
}

// Aggregator groups peeked dead-letter messages as they arrive, so a page can be dropped after it is added.
type Aggregator struct {
	groups map[groupKey]*groupStats
	peeked int
}

// NewAggregator returns an empty aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{groups: make(map[groupKey]*groupStats)}
}

// Add groups a page of messages.
func (a *Aggregator) Add(messages []*azservicebus.ReceivedMessage) {
	for _, message := range messages {
		a.addRecord(recordFromReceived(message))
	}
}

func (a *Aggregator) addRecord(rec record) {
	a.peeked++

	key := groupKey{payloadTypeID: rec.payloadTypeID(), reason: rec.DeadLetterReason}
	stats, ok := a.groups[key]
	if !ok {
		stats = &groupStats{descriptions: make(map[string]int), sampleID: rec.MessageID}
		a.groups[key] = stats
	}

	stats.count++
	stats.maxDelivery = max(stats.maxDelivery, rec.DeliveryCount)
	if rec.DeadLetterErrorDescription != "" {
		stats.descriptions[rec.DeadLetterErrorDescription]++
	}
	if enqueuedAt, err := time.Parse(time.RFC3339Nano, rec.EnqueuedAt); err == nil {
		if stats.firstAt.IsZero() || enqueuedAt.Before(stats.firstAt) {
			stats.firstAt = enqueuedAt
		}
		if enqueuedAt.After(stats.lastAt) {
			stats.lastAt = enqueuedAt
		}
	}
}

// Summary returns the groups, largest first, with the partial flag worked out from the entity facts and max.
func (a *Aggregator) Summary(entity bus.EntityFacts, maxMessages int) Summary {
	groups := make([]Group, 0, len(a.groups))
	for key, stats := range a.groups {
		groups = append(groups, Group{
			PayloadTypeID:       key.payloadTypeID,
			DeadLetterReason:    key.reason,
			Count:               stats.count,
			FirstEnqueuedAt:     formatTime(stats.firstAt),
			LastEnqueuedAt:      formatTime(stats.lastAt),
			MaxDeliveryCount:    stats.maxDelivery,
			TopErrorDescription: topDescription(stats.descriptions),
			SampleMessageID:     stats.sampleID,
		})
	}
	slices.SortFunc(groups, compareGroups)

	return Summary{
		Schema:  Schema,
		Entity:  entity,
		Peeked:  a.peeked,
		Partial: isPartial(entity, a.peeked, maxMessages),
		Groups:  groups,
	}
}

// isPartial is true when the DLQ holds more than was peeked. With no count to compare, a peek that filled
// --max may have stopped short.
func isPartial(entity bus.EntityFacts, peeked, maxMessages int) bool {
	if entity.Available && entity.DeadLetterMessages != nil {
		return *entity.DeadLetterMessages > int64(peeked)
	}
	return peeked == maxMessages
}

func compareGroups(a, b Group) int {
	return cmp.Or(
		cmp.Compare(b.Count, a.Count),
		cmp.Compare(a.PayloadTypeID, b.PayloadTypeID),
		cmp.Compare(a.DeadLetterReason, b.DeadLetterReason),
	)
}

// topDescription returns the most frequent description, the smallest one on a tie.
func topDescription(counts map[string]int) string {
	top, topCount := "", 0
	for _, description := range slices.Sorted(maps.Keys(counts)) {
		if counts[description] > topCount {
			top, topCount = description, counts[description]
		}
	}
	return top
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// WriteSummary writes the summary as one JSON object, or as a key/value block and a table.
func WriteSummary(out io.Writer, format Format, summary Summary) error {
	if format == FormatJSON {
		encoder := json.NewEncoder(out)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(summary); err != nil {
			return fmt.Errorf("write json summary: %w", err)
		}
		return nil
	}
	return writeSummaryTable(out, summary)
}

func writeSummaryTable(out io.Writer, summary Summary) error {
	facts := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	for _, line := range entityLines(summary.Entity) {
		fmt.Fprintf(facts, "%s:\t%s\n", line[0], line[1])
	}
	if err := facts.Flush(); err != nil {
		return fmt.Errorf("write summary table: %w", err)
	}

	if note := coverageNote(summary); note != "" {
		fmt.Fprintln(out, note)
	}
	if len(summary.Groups) == 0 {
		fmt.Fprintln(out, "\nNo dead-lettered messages were peeked.")
		return nil
	}

	fmt.Fprintln(out)
	table := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(table, "COUNT\tPAYLOAD_TYPE\tREASON\tMAX_DELIVERY\tFIRST\tLAST\tDESCRIPTION")
	for _, group := range summary.Groups {
		fmt.Fprintf(table, "%d\t%s\t%s\t%d\t%s\t%s\t%s\n",
			group.Count, noneIfEmpty(cell(group.PayloadTypeID)), noneIfEmpty(cell(group.DeadLetterReason)), group.MaxDeliveryCount,
			group.FirstEnqueuedAt, group.LastEnqueuedAt, truncateRunes(cell(group.TopErrorDescription), descriptionColumnRunes))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write summary table: %w", err)
	}
	return nil
}

func entityLines(entity bus.EntityFacts) [][2]string {
	name := "queue " + entity.Queue
	if entity.Queue == "" {
		name = fmt.Sprintf("topic %s, subscription %s", entity.Topic, entity.Subscription)
	}
	lines := [][2]string{{"entity", name}}
	if !entity.Available {
		return append(lines, [2]string{"details", "unavailable (no permission to read them)"})
	}

	lines = append(lines,
		[2]string{"active", countText(entity.ActiveMessages)},
		[2]string{"dead-letter", countText(entity.DeadLetterMessages)},
	)
	if entity.ScheduledMessages != nil {
		lines = append(lines, [2]string{"scheduled", countText(entity.ScheduledMessages)})
	}
	lines = append(lines,
		[2]string{"transfer dead-letter", countText(entity.TransferDeadLetterMessages)},
		[2]string{"max delivery count", fmt.Sprint(valueOrZero(entity.MaxDeliveryCount))},
		[2]string{"lock duration", entity.LockDuration},
		[2]string{"duplicate detection", duplicateDetectionText(entity)},
	)
	return lines
}

func countText(count *int64) string {
	if count == nil {
		return "unknown"
	}
	return fmt.Sprint(*count)
}

func valueOrZero(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func duplicateDetectionText(entity bus.EntityFacts) string {
	if entity.RequiresDuplicateDetection == nil || !*entity.RequiresDuplicateDetection {
		return "off"
	}
	return "on, window " + entity.DuplicateDetectionWindow
}

func coverageNote(summary Summary) string {
	if !summary.Partial {
		return ""
	}
	if summary.Entity.Available && summary.Entity.DeadLetterMessages != nil {
		return fmt.Sprintf("Partial: the summary covers %d of %d messages.", summary.Peeked, *summary.Entity.DeadLetterMessages)
	}
	return fmt.Sprintf("Partial: the summary covers the first %d messages and the dead-letter queue may hold more.", summary.Peeked)
}

func noneIfEmpty(value string) string {
	if strings.TrimSpace(value) == "" {
		return noneLabel
	}
	return value
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
