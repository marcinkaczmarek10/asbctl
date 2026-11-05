package dlq

import (
	"context"
	"reflect"
	"testing"
)

func TestPurgeCompletesOnlyTheSelectedMessages(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "Noise", "r"), dlMessage(2, "b", "Order", "r"), dlMessage(3, "c", "Noise", "r"), dlMessage(4, "d", "Order", "r"))
	selection := selectionOf(t, queue, Filter{PayloadTypeID: "Order"})
	var results []Result

	summary, err := Purge(context.Background(), queue, selection, "snap", PurgeOptions{Logger: discardLogger}, func(result Result) error {
		results = append(results, result)
		return nil
	})

	if err != nil {
		t.Fatalf("Purge() error = %v", err)
	}
	if summary.Purged != 2 || summary.Failed != 0 || summary.NotReached != 0 || summary.Snapshot != "snap" || summary.Type != "summary" {
		t.Errorf("summary = %+v, want 2 purged", summary)
	}
	if !reflect.DeepEqual(queue.completed, []int64{2, 4}) || !reflect.DeepEqual(queue.abandoned, []int64{1, 3}) {
		t.Errorf("completed %v, abandoned %v, want 2 and 4 completed and 1 and 3 abandoned", queue.completed, queue.abandoned)
	}
	if len(results) != 2 || results[0].Status != StatusPurged || results[0].MessageID != "b" {
		t.Errorf("results = %+v, want purged b then d", results)
	}
}

func TestPurgeReportsAFailedCompleteAndKeepsTheMessage(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	queue.completeErr[1] = errBoom
	var results []Result

	summary, _ := Purge(context.Background(), queue, selectionOf(t, queue, Filter{}), "snap", PurgeOptions{Logger: discardLogger}, func(result Result) error {
		results = append(results, result)
		return nil
	})

	if summary.Purged != 1 || summary.Failed != 1 {
		t.Errorf("summary = %+v, want 1 purged and 1 failed", summary)
	}
	if results[0].Status != StatusFailed || results[0].Err == nil {
		t.Errorf("first result = %+v, want failed with its error", results[0])
	}
	if !reflect.DeepEqual(queue.abandoned, []int64{1}) {
		t.Errorf("abandoned = %v, want the message that failed to complete released", queue.abandoned)
	}
}

func TestPurgeCountsSelectedMessagesThatNeverShowUp(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	selection := selectionOf(t, queue, Filter{})
	selection.Entries = append(selection.Entries, Entry{SequenceNumber: 9})

	summary, _ := Purge(context.Background(), queue, selection, "snap", PurgeOptions{Logger: discardLogger}, func(Result) error { return nil })

	if summary.Purged != 1 || summary.NotReached != 1 {
		t.Errorf("summary = %+v, want 1 purged and 1 not reached", summary)
	}
}

func TestPurgeStopsWhenTheResultCannotBeWritten(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))

	_, err := Purge(context.Background(), queue, selectionOf(t, queue, Filter{}), "snap", PurgeOptions{Logger: discardLogger}, func(Result) error { return errBoom })

	if err != errBoom || len(queue.completed) != 1 {
		t.Errorf("Purge() error = %v, completed %v, want %v after one message", err, queue.completed, errBoom)
	}
}

func TestPurgeExitCode(t *testing.T) {
	tests := []struct {
		name     string
		summary  PurgeSummary
		selected int
		want     int
	}{
		{name: "nothing selected", summary: PurgeSummary{}, selected: 0, want: 0},
		{name: "all purged", summary: PurgeSummary{Purged: 3}, selected: 3, want: 0},
		{name: "none purged", summary: PurgeSummary{Failed: 3}, selected: 3, want: 1},
		{name: "some purged", summary: PurgeSummary{Purged: 2, Failed: 1}, selected: 3, want: 3},
		{name: "some not reached", summary: PurgeSummary{Purged: 2, NotReached: 1}, selected: 3, want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.summary.ExitCode(tc.selected); got != tc.want {
				t.Errorf("ExitCode(%d) = %d, want %d", tc.selected, got, tc.want)
			}
		})
	}
}
