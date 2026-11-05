package export

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

func TestNewSendPlanSortsAndTotals(t *testing.T) {
	plan := NewSendPlan(SendTarget{Queue: "orders"}, map[string]int{"B": 2, "": 1, "A": 2, "C": 5}, true)

	want := []PayloadTypeCount{{"C", 5}, {"A", 2}, {"B", 2}, {"", 1}}
	if !reflect.DeepEqual(plan.PayloadTypes, want) {
		t.Errorf("PayloadTypes = %v, want %v", plan.PayloadTypes, want)
	}
	if plan.Total != 10 || !plan.DryRun || plan.Schema != 1 {
		t.Errorf("plan = %+v, want total 10, dry run and schema 1", plan)
	}
	if empty := NewSendPlan(SendTarget{}, nil, false); empty.PayloadTypes == nil {
		t.Error("PayloadTypes = nil for an empty plan, want an empty slice so json prints []")
	}
}

func TestWriteSendPlanTable(t *testing.T) {
	tests := []struct {
		name   string
		target SendTarget
		dryRun bool
		want   string
	}{
		{
			name: "queue plan asks for --yes", target: SendTarget{Queue: "orders"},
			want: "target:    queue orders\nmessages:  3\n\nCOUNT  PAYLOAD_TYPE\n2      OrderPlaced\n1      (none)\n\nNothing was sent. Pass --yes to send.\n",
		},
		{
			name: "topic dry run says so", target: SendTarget{Topic: "events"}, dryRun: true,
			want: "target:    topic events\nmessages:  3\n\nCOUNT  PAYLOAD_TYPE\n2      OrderPlaced\n1      (none)\n\nDry run: nothing was sent.\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			err := WriteSendPlan(&out, FormatTable, NewSendPlan(tc.target, map[string]int{"OrderPlaced": 2, "": 1}, tc.dryRun))

			if err != nil || out.String() != tc.want {
				t.Errorf("WriteSendPlan() = %q, %v, want %q", out.String(), err, tc.want)
			}
		})
	}
}

func TestWriteSendPlanOfNothingHasNoTable(t *testing.T) {
	var out bytes.Buffer

	_ = WriteSendPlan(&out, FormatTable, NewSendPlan(SendTarget{Queue: "q"}, nil, false))

	if strings.Contains(out.String(), "COUNT") || !strings.Contains(out.String(), "messages:  0") {
		t.Errorf("plan = %q, want 0 messages and no table", out.String())
	}
}

func TestWriteSendPlanJSON(t *testing.T) {
	var out bytes.Buffer

	_ = WriteSendPlan(&out, FormatJSON, NewSendPlan(SendTarget{Topic: "t"}, map[string]int{"A": 1}, false))

	want := `{"schema":1,"target":{"topic":"t"},"total":1,"payload_types":[{"payload_type_id":"A","count":1}],"dry_run":false}` + "\n"
	if out.String() != want {
		t.Errorf("plan = %s, want %s", out.String(), want)
	}
}

func TestWriteSendResult(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		result bus.SendResult
		want   string
	}{
		{name: "json sent", format: FormatJSON, result: bus.SendResult{Line: 1, MessageID: "m-1"}, want: `{"schema":1,"line":1,"message_id":"m-1","status":"sent"}` + "\n"},
		{name: "json failed", format: FormatJSON, result: bus.SendResult{Line: 2, MessageID: "m-2", Err: errors.New("too large")}, want: `{"schema":1,"line":2,"message_id":"m-2","status":"failed","error":"too large"}` + "\n"},
		{name: "json without a message id", format: FormatJSON, result: bus.SendResult{Line: 3}, want: `{"schema":1,"line":3,"status":"sent"}` + "\n"},
		{name: "table says nothing for a sent message", format: FormatTable, result: bus.SendResult{Line: 1, MessageID: "m-1"}, want: ""},
		{name: "table names a failed message", format: FormatTable, result: bus.SendResult{Line: 2, MessageID: "m-2", Err: errors.New("too large")}, want: "line 2 m-2: failed: too large\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			if err := WriteSendResult(&out, tc.format, tc.result); err != nil || out.String() != tc.want {
				t.Errorf("WriteSendResult() = %q, %v, want %q", out.String(), err, tc.want)
			}
		})
	}
}

func TestWriteSendSummaryIsForTableMode(t *testing.T) {
	var table, js bytes.Buffer

	WriteSendSummary(&table, FormatTable, bus.SendCounts{Sent: 8, Failed: 2}, 10)
	WriteSendSummary(&js, FormatJSON, bus.SendCounts{Sent: 8, Failed: 2}, 10)

	if table.String() != "Sent 8 of 10 messages, 2 failed.\n" || js.Len() != 0 {
		t.Errorf("table = %q, json = %q, want the sentence in table mode and nothing in json", table.String(), js.String())
	}
}

func TestSendWritersReturnTheWriteError(t *testing.T) {
	if err := WriteSendResult(failingWriter{}, FormatJSON, bus.SendResult{Line: 1}); err == nil {
		t.Error("WriteSendResult() error = nil, want the write error")
	}
	if err := WriteSendPlan(failingWriter{}, FormatJSON, SendPlan{}); err == nil {
		t.Error("WriteSendPlan() error = nil, want the write error")
	}
}
