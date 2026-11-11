package main

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// minCycle is the shortest a Cycle can be: a part count increase sooner than
// this after the previous genuine one is a counter glitch, not a part.
const minCycle = 5 * time.Second

// cyclesToLearn is how many non-Interrupted Cycles a Program's Baseline is learned from.
const cyclesToLearn = 10

// Cycle is the making of one part (see GLOSSARY.md), timed by the agent's clock.
type Cycle struct {
	Machine     string
	Program     string
	Start, End  time.Time
	Interrupted bool
}

// CycleProgress is what finding Cycles carries from one call to the next for one machine.
type CycleProgress struct {
	After   int64     `json:"-"`                 // the arrival order (rowid) of the last observation worked through
	Count   int64     `json:"count"`             // the last part count seen
	Counted bool      `json:"counted,omitempty"` // whether any part count has been seen
	Last    time.Time `json:"last,omitzero"`     // the previous genuine increase; zero at the start of a batch
	Program string    `json:"program,omitempty"` // the Program in effect

	Interrupted bool            `json:"interrupted,omitempty"` // the Cycle in progress has seen a stop or Fault
	Stopped     bool            `json:"stopped,omitempty"`     // Run state is a stop now
	Faults      map[string]bool `json:"faults,omitempty"`      // Condition data items at Fault now
	EStop       bool            `json:"estop,omitempty"`       // emergency stop triggered now
}

// halted reports whether the machine is stopped or faulted right now.
func (p CycleProgress) halted() bool { return p.Stopped || len(p.Faults) > 0 || p.EStop }

// findCycles finds the Cycles in one machine's observations, in arrival order,
// continuing from p. With continuous timing a part count reset doesn't start a
// new batch, so the first part after it still has a Cycle.
func findCycles(obs []mtconnect.Observation, p CycleProgress, continuous bool) ([]Cycle, CycleProgress) {
	var cycles []Cycle
	p.Faults = maps.Clone(p.Faults) // the caller's progress stays as it was
	for _, o := range obs {
		switch {
		case o.Category == "Condition":
			if level(o.Value) == "Fault" {
				if p.Faults == nil {
					p.Faults = map[string]bool{}
				}
				p.Faults[o.DataItemID] = true
			} else {
				delete(p.Faults, o.DataItemID)
			}
		case o.Type == "Execution":
			// READY and PROGRAM_COMPLETED between parts are changeover, not a stop
			p.Stopped = o.Value == "STOPPED" || o.Value == "INTERRUPTED" || o.Value == "FEED_HOLD"
		case o.Type == "EmergencyStop":
			p.EStop = o.Value == "TRIGGERED"
		case o.Type == "Program" && (o.SubType == "" || o.SubType == "ACTIVE"):
			// the active Program is what runs now; MAIN is only the one that called it
			p.Program = o.Value
		case o.Type == "PartCount":
			n, err := strconv.ParseInt(o.Value, 10, 64)
			if err != nil {
				continue // UNAVAILABLE says nothing about parts made
			}
			prev, counted := p.Count, p.Counted
			p.Count, p.Counted = n, true
			if counted && n < prev && !continuous {
				p.Last = time.Time{}
			}
			if !counted || n <= prev {
				continue
			}
			if !p.Last.IsZero() && o.Timestamp.Sub(p.Last) < minCycle {
				continue
			}
			if !p.Last.IsZero() {
				cycles = append(cycles, Cycle{Machine: o.Device, Program: p.Program, Start: p.Last, End: o.Timestamp, Interrupted: p.Interrupted})
			}
			p.Last = o.Timestamp
			p.Interrupted = p.halted() // a stop or Fault still on carries into the next Cycle
		}
		if p.halted() {
			p.Interrupted = true
		}
	}
	return cycles, p
}

// CycleRow is one machine and Program in the board's Cycle list.
type CycleRow struct {
	Machine   string        `json:"machine"`
	Program   string        `json:"program"`
	Learning  int           `json:"learning"`   // non-Interrupted Cycles so far, up to learn_from
	LearnFrom int           `json:"learn_from"` // how many the Baseline will be learned from
	Recent    []RecentCycle `json:"recent"`     // newest first
}

// RecentCycle is one Cycle as the board shows it.
type RecentCycle struct {
	End         time.Time `json:"end"`
	Seconds     float64   `json:"seconds"`
	Interrupted bool      `json:"interrupted"`
}

// cycleRows groups recent Cycles (newest first) into one row per machine and
// Program, sorted by name. uninterrupted is each one's count of non-Interrupted Cycles.
func cycleRows(recent []Cycle, uninterrupted map[ProgramKey]int) []CycleRow {
	byProgram := map[ProgramKey]*CycleRow{}
	for _, c := range recent {
		k := ProgramKey{Machine: c.Machine, Program: c.Program}
		r := byProgram[k]
		if r == nil {
			r = &CycleRow{Machine: c.Machine, Program: c.Program, Learning: min(uninterrupted[k], cyclesToLearn), LearnFrom: cyclesToLearn}
			byProgram[k] = r
		}
		r.Recent = append(r.Recent, RecentCycle{End: c.End, Seconds: c.End.Sub(c.Start).Seconds(), Interrupted: c.Interrupted})
	}
	rows := make([]CycleRow, 0, len(byProgram))
	for _, r := range byProgram {
		rows = append(rows, *r)
	}
	slices.SortFunc(rows, func(a, b CycleRow) int {
		return cmp.Or(cmp.Compare(a.Machine, b.Machine), cmp.Compare(a.Program, b.Program))
	})
	return rows
}
