package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

var t0 = time.Date(2026, 11, 10, 4, 0, 0, 0, time.UTC)

// at is an observation of the Okuma made at t0 plus offset.
func at(offset time.Duration, category, typ, value string) mtconnect.Observation {
	return mtconnect.Observation{Device: "OKUMA", DeviceUUID: "OKUMA", DataItemID: typ,
		Category: category, Type: typ, Value: value, Timestamp: t0.Add(offset)}
}

func parts(offset time.Duration, n string) mtconnect.Observation {
	return at(offset, "Event", "PartCount", n)
}
func program(offset time.Duration, p string) mtconnect.Observation {
	return at(offset, "Event", "Program", p)
}
func run(offset time.Duration, state string) mtconnect.Observation {
	return at(offset, "Event", "Execution", state)
}

// show writes Cycles as "program duration" with "!" for Interrupted, so a test reads like its worked example.
func show(cycles []Cycle) string {
	var s []string
	for _, c := range cycles {
		mark := ""
		if c.Interrupted {
			mark = "!"
		}
		s = append(s, fmt.Sprintf("%s %s%s", c.Program, c.End.Sub(c.Start), mark))
	}
	return fmt.Sprint(s)
}

// The demo Okuma's loop, as stored on 2026-11-10: each Program makes one part, the
// counter ticks again about 2 s after the 1E part as 4B starts, and resets at the other changes.
var okumaLoop = []mtconnect.Observation{
	program(0, "HOB"),
	parts(0, "0"),
	run(0, "ACTIVE"),
	run(3*time.Minute+19*time.Second, "PROGRAM_COMPLETED"),
	run(3*time.Minute+20*time.Second, "READY"),
	parts(3*time.Minute+20*time.Second, "1"), // HOB part
	program(3*time.Minute+21*time.Second, "1E"),
	parts(3*time.Minute+21*time.Second, "0"),
	run(3*time.Minute+23*time.Second, "ACTIVE"),
	run(14*time.Minute+15*time.Second, "PROGRAM_COMPLETED"),
	parts(14*time.Minute+16*time.Second, "1"), // 1E part
	run(14*time.Minute+16*time.Second, "READY"),
	program(14*time.Minute+18*time.Second, "4B"),
	parts(14*time.Minute+18*time.Second, "2"), // the fake tick
	run(14*time.Minute+18*time.Second, "ACTIVE"),
	run(24*time.Minute+5*time.Second, "PROGRAM_COMPLETED"),
	parts(24*time.Minute+5*time.Second, "3"), // 4B part
	run(24*time.Minute+5*time.Second, "READY"),
	program(24*time.Minute+5*time.Second, "TRAN"),
	parts(24*time.Minute+5*time.Second, "0"),
	run(24*time.Minute+7*time.Second, "ACTIVE"),
	run(24*time.Minute+23*time.Second, "PROGRAM_COMPLETED"),
	parts(24*time.Minute+24*time.Second, "1"), // TRAN part
	run(24*time.Minute+24*time.Second, "READY"),
}

func TestCyclesOkumaWithContinuousTiming(t *testing.T) {
	got, _ := findCycles(okumaLoop, CycleProgress{}, true)
	want := "[1E 10m56s 4B 9m49s TRAN 19s]" // HOB's part is the first seen, so nothing times it
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func TestCyclesOkumaWithoutContinuousTiming(t *testing.T) {
	got, _ := findCycles(okumaLoop, CycleProgress{}, false)
	want := "[4B 9m49s]" // every other part is the first of a batch
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func TestCyclesResetStartsANewBatch(t *testing.T) {
	got, _ := findCycles([]mtconnect.Observation{
		parts(0, "8"),
		parts(20*time.Second, "9"), // the first increase seen starts timing
		parts(40*time.Second, "10"),
		parts(45*time.Second, "0"), // new batch
		parts(70*time.Second, "1"), // its first part: no Cycle, but timing starts
		parts(90*time.Second, "2"),
		parts(100*time.Second, "0"),
		parts(110*time.Second, "0"), // a repeated value is not a part
		parts(130*time.Second, "1"),
		parts(150*time.Second, "2"),
	}, CycleProgress{}, false)
	want := "[ 20s  20s  20s]"
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func TestCyclesIgnoreAnIncreaseUnderFiveSeconds(t *testing.T) {
	got, _ := findCycles([]mtconnect.Observation{
		parts(0, "1"),
		parts(10*time.Second, "2"),
		parts(14900*time.Millisecond, "3"), // 4.9 s after the previous genuine increase
		parts(15*time.Second, "4"),         // 5 s after it: a Cycle, timed from that increase
		parts(40*time.Second, "5"),
	}, CycleProgress{}, false)
	want := "[ 5s  25s]"
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func cond(offset time.Duration, id, value string) mtconnect.Observation {
	o := at(offset, "Condition", "SYSTEM", value)
	o.DataItemID = id
	return o
}

// stopsAndFaults is a run of 20 s Cycles, each with something different happening during it.
var stopsAndFaults = []mtconnect.Observation{
	parts(-20*time.Second, "0"),
	run(0, "ACTIVE"),
	parts(0, "1"),
	run(20*time.Second, "STOPPED"),
	run(30*time.Second, "ACTIVE"),
	parts(60*time.Second, "2"), // stopped for 10 s: Interrupted
	run(80*time.Second, "PROGRAM_COMPLETED"),
	parts(80*time.Second, "3"), // PROGRAM_COMPLETED and READY between parts are changeover
	run(81*time.Second, "READY"),
	run(83*time.Second, "ACTIVE"),
	parts(100*time.Second, "4"),
	cond(110*time.Second, "hydraulic", "Fault: Low pressure"),
	cond(115*time.Second, "hydraulic", "Normal"),
	parts(120*time.Second, "5"), // a Fault came and went: Interrupted
	cond(125*time.Second, "coolant", "Fault: Coolant low"),
	parts(140*time.Second, "6"), // still faulted at the start of the next Cycle
	parts(160*time.Second, "7"), // so this one is Interrupted too
	cond(161*time.Second, "coolant", "Normal"),
	run(165*time.Second, "FEED_HOLD"),
	run(170*time.Second, "ACTIVE"),
	parts(180*time.Second, "8"),
	at(190*time.Second, "Event", "EmergencyStop", "TRIGGERED"),
	at(191*time.Second, "Event", "EmergencyStop", "ARMED"),
	parts(200*time.Second, "9"),
	parts(220*time.Second, "10"),
}

func TestCyclesInterruptedByAStopOrFault(t *testing.T) {
	got, _ := findCycles(stopsAndFaults, CycleProgress{}, false)
	want := "[ 1m0s!  20s  20s  20s!  20s!  20s!  20s!  20s!  20s]"
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func TestCyclesNoneWithoutIncreases(t *testing.T) {
	mazak := func(o mtconnect.Observation) mtconnect.Observation {
		o.Device, o.DeviceUUID = "Mazak", "Mazak"
		return o
	}
	got, _ := findCycles([]mtconnect.Observation{
		mazak(parts(0, "0")),
		mazak(program(0, "4000-OP10")),
		mazak(run(0, "ACTIVE")),
		mazak(run(10*time.Minute, "READY")),
		mazak(parts(10*time.Minute, "0")),
		mazak(program(11*time.Minute, "4000-OP20")),
		mazak(run(11*time.Minute, "ACTIVE")),
	}, CycleProgress{}, false)
	if len(got) != 0 {
		t.Errorf("cycles = %s, want none", show(got))
	}
}

// Housekeeping finds Cycles a few observations at a time, so a split anywhere must not change them.
func TestCyclesSameWhenFoundInTwoCalls(t *testing.T) {
	for _, continuous := range []bool{true, false} {
		for _, obs := range [][]mtconnect.Observation{okumaLoop, stopsAndFaults} {
			whole, _ := findCycles(obs, CycleProgress{}, continuous)
			for split := range obs {
				first, p := findCycles(obs[:split], CycleProgress{}, continuous)
				second, _ := findCycles(obs[split:], p, continuous)
				if got := show(append(first, second...)); got != show(whole) {
					t.Errorf("continuous %v, split at %d: cycles = %s, want %s", continuous, split, got, show(whole))
				}
			}
		}
	}
}

// A machine reporting both its main and active Program, like the Mazak, makes parts for the active one.
func TestCyclesBelongToTheActiveProgram(t *testing.T) {
	sub := func(o mtconnect.Observation, subType string) mtconnect.Observation {
		o.SubType, o.DataItemID = subType, "Program"+subType
		return o
	}
	got, _ := findCycles([]mtconnect.Observation{
		parts(0, "0"),
		parts(time.Minute, "1"),
		sub(program(time.Minute, "O1000"), "MAIN"),
		sub(program(time.Minute, "O2000"), "ACTIVE"),
		parts(2*time.Minute, "2"),
		sub(program(2*time.Minute, "O1001"), "MAIN"),
		parts(3*time.Minute, "3"),
	}, CycleProgress{}, false)
	want := "[O2000 1m0s O2000 1m0s]"
	if show(got) != want {
		t.Errorf("cycles = %s, want %s", show(got), want)
	}
}

func TestCycleRowsOnePerMachineAndProgram(t *testing.T) {
	cycle := func(machine, program string, end, seconds int, interrupted bool) Cycle {
		e := t0.Add(time.Duration(end) * time.Minute)
		return Cycle{Machine: machine, Program: program, Start: e.Add(-time.Duration(seconds) * time.Second), End: e, Interrupted: interrupted}
	}
	got := cycleRows([]Cycle{ // newest first, as the store gives them
		cycle("OKUMA", "TRAN", 50, 19, false),
		cycle("SIM-MILL", "P1", 40, 20, true),
		cycle("OKUMA", "1E", 30, 655, false),
		cycle("OKUMA", "TRAN", 25, 18, true),
	}, map[ProgramKey]int{{Machine: "OKUMA", Program: "TRAN"}: 1, {Machine: "OKUMA", Program: "1E"}: 14})

	want := []CycleRow{
		{Machine: "OKUMA", Program: "1E", Learning: 10, LearnFrom: 10, Recent: []RecentCycle{
			{End: time.Date(2026, 11, 10, 4, 30, 0, 0, time.UTC), Seconds: 655}}},
		{Machine: "OKUMA", Program: "TRAN", Learning: 1, LearnFrom: 10, Recent: []RecentCycle{
			{End: time.Date(2026, 11, 10, 4, 50, 0, 0, time.UTC), Seconds: 19},
			{End: time.Date(2026, 11, 10, 4, 25, 0, 0, time.UTC), Seconds: 18, Interrupted: true}}},
		{Machine: "SIM-MILL", Program: "P1", Learning: 0, LearnFrom: 10, Recent: []RecentCycle{
			{End: time.Date(2026, 11, 10, 4, 40, 0, 0, time.UTC), Seconds: 20, Interrupted: true}}},
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Errorf("rows = %+v\nwant %+v", got, want)
	}
}
