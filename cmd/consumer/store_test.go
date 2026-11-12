package main

import (
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
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
	if _, err := s.Insert([]mtconnect.Observation{old1, old2, kept}, cutoff); err != nil {
		t.Fatal(err)
	}

	if n, err := s.Prune(cutoff); err != nil || n != 2 {
		t.Fatalf("Prune = %d, %v; want 2 deleted", n, err)
	}
	if n, err := s.Prune(cutoff); err != nil || n != 0 {
		t.Fatalf("second Prune = %d, %v; want 0", n, err)
	}
	// a pruned observation is gone, so storing it again counts as new; the kept one is still there
	if n, _ := s.Insert([]mtconnect.Observation{old1, kept}, cutoff); n != 1 {
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

	if n, err := s.Insert(first, t0); err != nil || n != 2 {
		t.Fatalf("first insert = %d, %v; want 2", n, err)
	}
	if n, err := s.Insert(first[:1], t0); err != nil || n != 0 {
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

func TestStoreReadsTheLatestStatusChangePerMachine(t *testing.T) {
	s := openTestStore(t)
	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)

	if got, err := s.LastStatusChanges(); err != nil || len(got) != 0 {
		t.Fatalf("LastStatusChanges on empty store = %v, %v; want none", got, err)
	}
	steps := []struct {
		at       time.Time
		machines []Machine
	}{
		{t0, []Machine{{Name: "Okuma", Status: StatusOK}, {Name: "Mazak", Status: StatusAlarm, Reason: "Program error"}}},
		{t0.Add(5 * time.Minute), []Machine{{Name: "Okuma", Status: StatusOK}, {Name: "Mazak", Status: StatusOK}}},
		{t0.Add(9 * time.Minute), []Machine{{Name: "Okuma", Status: StatusOK}, {Name: "Mazak", Status: StatusOK}}}, // no change, nothing recorded
	}
	for _, st := range steps {
		if err := s.RecordStatuses(st.machines, st.at); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.LastStatusChanges()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]StatusChange{"Mazak": {Status: StatusOK, At: time.Date(2026, 10, 21, 6, 5, 0, 0, time.UTC)}} // Okuma never changed
	if len(got) != len(want) || got["Mazak"].Status != want["Mazak"].Status || !got["Mazak"].At.Equal(want["Mazak"].At) {
		t.Errorf("LastStatusChanges = %v, want %v", got, want)
	}
}

// Received-at is the laptop clock, so a wrong agent clock never changes it; a replayed
// observation is nothing new and leaves it alone.
func TestStoreRecordsReceivedAt(t *testing.T) {
	s := openTestStore(t)
	agentClock := time.Date(2026, 10, 21, 3, 0, 0, 0, time.UTC) // 3h behind the laptop
	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)
	o := func(machine string, seq uint64) mtconnect.Observation {
		return mtconnect.Observation{InstanceID: 1, Sequence: seq, DeviceUUID: machine, Device: machine, DataItemID: "exec", Timestamp: agentClock}
	}

	if got, err := s.ReceivedAt(); err != nil || len(got) != 0 {
		t.Fatalf("ReceivedAt on empty store = %v, %v; want none", got, err)
	}
	if _, err := s.Insert([]mtconnect.Observation{o("Okuma", 1), o("Mazak", 2)}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert([]mtconnect.Observation{o("Okuma", 3), o("Mazak", 2)}, t0.Add(time.Minute)); err != nil { // Mazak's is a replay
		t.Fatal(err)
	}

	got, err := s.ReceivedAt()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]time.Time{
		"Okuma": time.Date(2026, 10, 21, 6, 1, 0, 0, time.UTC),
		"Mazak": time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) || !got["Okuma"].Equal(want["Okuma"]) || !got["Mazak"].Equal(want["Mazak"]) {
		t.Errorf("ReceivedAt = %v, want %v", got, want)
	}
}

func TestStoreCyclesAndProgressRoundTrip(t *testing.T) {
	s := openTestStore(t)
	progress := CycleProgress{After: 1730791, Count: 3, Counted: true, Last: t0.Add(10 * time.Minute), Program: "4B",
		Interrupted: true, Faults: map[string]bool{"coolant": true}}
	cycles := []Cycle{
		{Machine: "OKUMA", Program: "1E", Start: t0, End: t0.Add(10*time.Minute + 55*time.Second)},
		{Machine: "OKUMA", Program: "TRAN", Start: t0.Add(11 * time.Minute), End: t0.Add(11*time.Minute + 19*time.Second), Interrupted: true},
	}

	if got, err := s.CycleProgress(); err != nil || len(got) != 0 {
		t.Fatalf("CycleProgress on empty store = %v, %v; want none", got, err)
	}
	if err := s.SaveCycles(cycles, map[string]CycleProgress{"OKUMA": progress}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCycles(cycles[1:], map[string]CycleProgress{"OKUMA": progress}); err != nil { // saved again after a crash
		t.Fatal(err)
	}

	got, err := s.CycleProgress()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]CycleProgress{"OKUMA": progress}) {
		t.Errorf("CycleProgress = %+v, want %+v", got, progress)
	}
	recent, counts, err := s.RecentCycles(10)
	if err != nil {
		t.Fatal(err)
	}
	if show(recent) != "[TRAN 19s! 1E 10m55s]" { // newest first, each kept once
		t.Errorf("RecentCycles = %s, want [TRAN 19s! 1E 10m55s]", show(recent))
	}
	if want := (map[ProgramKey]int{{Machine: "OKUMA", Program: "1E"}: 1}); fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Errorf("non-Interrupted counts = %v, want %v", counts, want)
	}
}

func TestStoreRecentCyclesPerProgram(t *testing.T) {
	s := openTestStore(t)
	var cycles []Cycle
	for i := range 4 {
		start := t0.Add(time.Duration(i) * time.Minute)
		cycles = append(cycles,
			Cycle{Machine: "OKUMA", Program: "TRAN", Start: start, End: start.Add(time.Duration(17+i) * time.Second)},
			Cycle{Machine: "SIM", Program: "TRAN", Start: start, End: start.Add(20 * time.Second), Interrupted: i == 0})
	}
	if err := s.SaveCycles(cycles, nil); err != nil {
		t.Fatal(err)
	}
	recent, counts, err := s.RecentCycles(2)
	if err != nil {
		t.Fatal(err)
	}
	if show(recent) != "[TRAN 20s TRAN 20s TRAN 20s TRAN 19s]" {
		t.Errorf("RecentCycles = %s, want the 2 newest of each machine", show(recent))
	}
	if want := (map[ProgramKey]int{{Machine: "OKUMA", Program: "TRAN"}: 4, {Machine: "SIM", Program: "TRAN"}: 3}); fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Errorf("non-Interrupted counts = %v, want %v (all Cycles, not only the recent)", counts, want)
	}
}

// Cycles come from what housekeeping reads after its progress point, so a replay, which
// stores nothing new, can never be read twice.
func TestStoreReplaysNotReadForCycles(t *testing.T) {
	s := openTestStore(t)
	o := func(seq uint64, typ, value string) mtconnect.Observation {
		return mtconnect.Observation{InstanceID: 1, Sequence: seq, Device: "OKUMA", DeviceUUID: "OKUMA", DataItemID: typ,
			Category: "Event", Type: typ, Value: value, Timestamp: t0.Add(time.Duration(seq) * time.Minute)}
	}
	batch := []mtconnect.Observation{o(1, "PartCount", "0"), o(2, "RotaryVelocity", "300"), o(3, "PartCount", "1")}
	if _, err := s.Insert(batch, t0); err != nil {
		t.Fatal(err)
	}
	first, err := s.CycleObservations(0, 100)
	if err != nil || len(first) != 2 {
		t.Fatalf("CycleObservations = %+v, %v; want the 2 part counts, not the spindle speed", first, err)
	}
	if _, err := s.Insert(append(batch, o(4, "PartCount", "2")), t0); err != nil {
		t.Fatal(err)
	}
	next, err := s.CycleObservations(first[1].Row, 100)
	if err != nil || len(next) != 1 || next[0].Value != "2" {
		t.Errorf("CycleObservations after the replay = %+v, %v; want only the new part count 2", next, err)
	}
}

// Housekeeping works through stored observations a batch at a time, across restarts and replays.
func TestFindNewCyclesNoLossNoRepeats(t *testing.T) {
	s := openTestStore(t)
	stored := func(obs []mtconnect.Observation, from uint64) []mtconnect.Observation {
		var out []mtconnect.Observation
		for i, o := range obs {
			o.InstanceID, o.Sequence = 1, from+uint64(i)
			out = append(out, o)
		}
		return out
	}
	loop := stored(okumaLoop, 1)
	mazak := []mtconnect.Observation{{InstanceID: 2, Sequence: 1, Device: "Mazak", DeviceUUID: "Mazak", DataItemID: "pc",
		Category: "Event", Type: "PartCount", Value: "0", Timestamp: t0}}
	continuous := map[string]bool{"OKUMA": true}

	if _, err := s.Insert(slices.Concat(loop[:12], mazak), t0); err != nil {
		t.Fatal(err)
	}
	for range 3 { // 5 at a time works through the first 13 observations, then find nothing new
		if err := findNewCycles(s, continuous, 5); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Insert(loop, t0); err != nil { // the first 12 again, replayed by Kafka, then the rest
		t.Fatal(err)
	}
	for range 4 {
		if err := findNewCycles(s, continuous, 5); err != nil {
			t.Fatal(err)
		}
	}

	recent, _, err := s.RecentCycles(10)
	if err != nil {
		t.Fatal(err)
	}
	if want := "[TRAN 19s 4B 9m49s 1E 10m56s]"; show(recent) != want {
		t.Errorf("cycles = %s, want %s", show(recent), want)
	}
}

// Housekeeping may already have worked through the observations before a Gap when the Gap is stored.
func TestFindNewCyclesGapStartsANewBatch(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Insert(acrossGap[:3], t0); err != nil {
		t.Fatal(err)
	}
	if err := findNewCycles(s, nil, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertGap(behind[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(acrossGap[3:], t0); err != nil {
		t.Fatal(err)
	}
	if err := findNewCycles(s, nil, 100); err != nil {
		t.Fatal(err)
	}

	recent, _, err := s.RecentCycles(10)
	if err != nil {
		t.Fatal(err)
	}
	if want := "[ 10m56s  10m56s]"; show(recent) != want {
		t.Errorf("cycles = %s, want %s", show(recent), want)
	}
}
