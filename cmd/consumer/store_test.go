package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// The edge re-sends a gap event if Kafka didn't confirm it, so the same gap can arrive twice.
func TestStoreKeepsEachGapOnceAndReportsTheNewest(t *testing.T) {
	s := openTestStore(t)
	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)
	older := mtconnect.Gap{DetectedAt: t0, InstanceID: 1, From: 100, ResumedAt: 900, Missed: 800, Reason: mtconnect.GapFellBehind}
	newer := mtconnect.Gap{DetectedAt: t0.Add(time.Hour), InstanceID: 1, From: 2000, ResumedAt: 2500, Reason: mtconnect.GapAgentRestarted}

	if got, err := s.LastGap(); err != nil || got != nil {
		t.Fatalf("LastGap on empty store = %+v, %v; want nil", got, err)
	}
	for _, g := range []mtconnect.Gap{older, newer, older} {
		if err := s.InsertGap(g); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.LastGap()
	if err != nil || got == nil || *got != newer {
		t.Fatalf("LastGap = %+v, %v; want %+v", got, err, newer)
	}
	events, err := s.RecentEvents(10)
	if err != nil || len(events) != 2 {
		t.Fatalf("RecentEvents = %+v, %v; want the 2 distinct gaps", events, err)
	}
}

func TestStoreRecordsStatusChangesOnlyWhenTheyChange(t *testing.T) {
	s := openTestStore(t)
	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)
	m := func(name string, st Status, reason string) Machine {
		return Machine{Name: name, Status: st, Reason: reason}
	}

	steps := []struct {
		at       time.Time
		machines []Machine
	}{
		{t0, []Machine{m("Okuma", StatusOK, ""), m("Mazak", StatusWarning, "RAPID FEED OVERRIDE SET AT 0")}}, // Okuma starting OK is not news
		{t0.Add(1 * time.Minute), []Machine{m("Okuma", StatusOK, ""), m("Mazak", StatusWarning, "RAPID FEED OVERRIDE SET AT 0")}},
		{t0.Add(2 * time.Minute), []Machine{m("Okuma", StatusOK, ""), m("Mazak", StatusAlarm, "Program error")}},
		{t0.Add(3 * time.Minute), []Machine{m("Okuma", StatusOK, ""), m("Mazak", StatusOK, "")}},
	}
	for _, st := range steps {
		if err := s.RecordStatuses(st.machines, st.at); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.InsertGap(mtconnect.Gap{DetectedAt: t0.Add(150 * time.Second), InstanceID: 1, From: 5, Reason: mtconnect.GapFellBehind}); err != nil {
		t.Fatal(err)
	}

	events, err := s.RecentEvents(3)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Mazak OK", "gap", "Mazak Alarm Program error"} // newest first, limited to 3
	var got []string
	for _, e := range events {
		if e.Kind == "gap" {
			got = append(got, "gap")
		} else {
			got = append(got, strings.TrimSpace(fmt.Sprintf("%s %s %s", e.Machine, e.Status, e.Reason)))
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("events = %q, want %q", got, want)
	}
}

func TestStorePruneDeletesOnlyObservationsOlderThanTheCutoff(t *testing.T) {
	s := openTestStore(t)
	cutoff := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	o := func(seq uint64, ts time.Time) mtconnect.Observation {
		return mtconnect.Observation{InstanceID: 1, Sequence: seq, DeviceUUID: "m", Device: "M", DataItemID: "x", Timestamp: ts}
	}
	old1, old2 := o(1, cutoff.Add(-48*time.Hour)), o(2, cutoff.Add(-time.Second))
	kept := o(3, cutoff.Add(time.Second))
	if _, err := s.Insert([]mtconnect.Observation{old1, old2, kept}); err != nil {
		t.Fatal(err)
	}

	if n, err := s.Prune(cutoff); err != nil || n != 2 {
		t.Fatalf("Prune = %d, %v; want 2 deleted", n, err)
	}
	if n, err := s.Prune(cutoff); err != nil || n != 0 {
		t.Fatalf("second Prune = %d, %v; want 0", n, err)
	}
	// a pruned observation is gone, so storing it again counts as new; the kept one is still there
	if n, _ := s.Insert([]mtconnect.Observation{old1, kept}); n != 1 {
		t.Errorf("re-insert after prune stored %d new, want 1 (only the pruned one)", n)
	}
}

// Kafka redelivers after a crash between the DB write and the offset commit;
// replaying a batch must not duplicate rows or roll the latest value backwards.
func TestStoreReplayIsHarmless(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)
	obs := func(seq uint64, value string, ts time.Time) mtconnect.Observation {
		return mtconnect.Observation{InstanceID: 1, Sequence: seq, DeviceUUID: "m", Device: "M", DataItemID: "exec",
			Category: "Event", Type: "Execution", Value: value, Timestamp: ts}
	}
	first := []mtconnect.Observation{obs(10, "READY", t0), obs(11, "ACTIVE", t0.Add(time.Second))}

	if n, err := s.Insert(first); err != nil || n != 2 {
		t.Fatalf("first insert = %d, %v; want 2", n, err)
	}
	if n, err := s.Insert(first[:1]); err != nil || n != 0 {
		t.Fatalf("replayed insert = %d, %v; want 0", n, err)
	}

	if count, _ := s.Count(); count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	latest, err := s.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 1 || latest[0].Value != "ACTIVE" || !latest[0].Timestamp.Equal(t0.Add(time.Second)) {
		t.Errorf("latest = %+v, want one ACTIVE row", latest)
	}
}
